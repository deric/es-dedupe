#!/usr/bin/env python3

# -*- coding: utf-8 -*-

import copy
import datetime
import inspect
import io
import os.path
import pprint as pp
import re
import requests
import sys
import time
import ujson
## Use StringIO.String() for python 2, io.StringIO() for python 3.
## No idea how to differentiate, but with python 2 and io.StringIO()   buf.write fails immensely: TypeError: unicode argument expected, got 'str'
## So we try/except instead.
try:
  from StringIO import StringIO         # python 2
except:
  from io import StringIO               # python 3
from collections import defaultdict
from datetime import timedelta
from time import sleep



## save our original index settings, so we can restore them after deleting duplicates
idx2settings = {}
indices = {}
re_indexexclude = re.compile('^$')

# out current scriptname (minus the path)
ourname = os.path.basename(__file__)
# At least Elasticsearch 6.2.2 does not support application/x-ndjson, but wants to enforce setting an explicit Content-Type.  As to why Elastic wouldn't support this, I have no idea.
es_headers = { 'Content-Type': 'application/json' }





def logme (msg = ""):
  global ourname
  callers = inspect.stack()
  caller = "{}::main".format(ourname)
  for i in range(1, len(callers)):
    if (callers[i][3] == "<module>"):
      break
    else:
      caller = "{0}::{1}".format(caller, callers[i][3])
  now = time.time()
  nowfloat = "{0:f}".format(now - int(now))[1:]
  ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now))
  print("{0}{1} {2}: {3}".format(ts, nowfloat, caller, msg))


def run(args):
    global pp, idx2settings, indices, re_indexexclude

    start = time.time()
    total = 0

    if (args.index != ""):      args.all = False            # if indexname specifically was set, do not do --all mode

    foundanyindex = False
    workisdone = False

    # fetch a list of all indices matching prefix_*suffix
    idx2settings = fetch_allsettings(args)
    idxlist = fetch_indexlist(args)
    indices = {}
    if (idxlist.__class__.__name__ != "dict"):
        logme("ERROR - Could not fetch_indexlist from http://{}:{} (returned class-name is {})\n{}\n".format(args.host, args.port, idxlist.__class__.__name__, pp.pformat(idxlist, 4, -1)))
        sys.exit(-1)
    if ('indices' in idxlist):
        for idxname in idxlist['indices']:
            if (re_indexexclude.match(idxname)):
                if args.verbose:
                    logme("idxname {} is excluded by {}".format(idxname, args.indexexclude))
                continue
            if (idxname in idx2settings):
                if (idxname not in indices):
                    storesize = 0
                    try:
                        storesize = idxlist['indices'][idxname]['total']['store']['size_in_bytes']
                    except:
                        logme("WARNING - Could not find total-store-size_in_bytes for index {0}".format(idxname))
                    if (storesize.__class__.__name__ == "None") or (storesize < 0): storesize = 0
                    indices[idxname] = storesize
            else:
                if args.verbose:
                    logme("# WARNING - Couldn't find settings for index '{0}'".format(idxname))
    logme("The following indices matched your name pattern {0} :\n{1}\n\n".format(idxlist_uri(args), pp.pformat(indices, 4, -1)))
    for idxname in indices:
        if (workisdone == False):
            logme('Current index index {}'.format(idxname))
        else:
            break

        erroroccurred = False

        qs = time.time()
        resp = fetch(idxname, args)
        qe = time.time()
        if (resp == "-1"):
            logme("ERROR - fetch couldn't be successfully executed for idxname {}".format(idxname))
            erroroccurred = True

        docs = -1
        removed = 0
        if (erroroccurred == False):
            if (isinstance(resp, dict)) and ("aggregations" in resp):
                docs = len(resp["aggregations"]["duplicateCount"]["buckets"])
            else:
                logme("ERROR - Unexpected response {}".format(resp))
                workisdone = True
            logme("ES query took {}, retrieved {} unique docs that have dupes".format(timedelta(seconds=(qe - qs)), docs))

        if (docs >= 0):
            bs = time.time()
            # now update write to false if it is not, and return it to original after we are done.
            if (args.noop == False):
                skipremoval = False
                if (idxname not in idx2settings):
                    logme("WARNING - Couldn't find settings for index '{0}'".format(idxname))
                else:
                    if (('write' in idx2settings[idxname]) and (idx2settings[idxname]['write'] != "false")):
                        if args.verbose:
                            logme("# Index '{0}' is not writable in settings, updating blocks-write to false".format(idxname))
                        if (set_index_writable(args, idxname, "false") == False):
                            logme("WARNING - Index '{0}' could not be made writable. Skipping deleting.".format(idxname))
                            skipremoval = True
                        else:
                            idx2settings[idxname]['_esdedup_changed_writeflag'] = True
                if (skipremoval == False):
                    cnt_removed = remove_duplicates(resp, idxname, args)
                    if (cnt_removed == -1):
                        logme("ERROR - remove_duplicates couldn't be successfully executed for idxname {}, resp {}".format(idxname, resp))
                        erroroccurred = True
                        cnt_removed = 0
                        break
                    removed = cnt_removed
                    be = time.time()
                    total += removed
                    logme("Deleted {} duplicates, in total {:,}. Batch-searched in {}, Batch-removed in {}, overall running time {}".format(removed, total, timedelta(seconds=(be - qe)), timedelta(seconds=(be - start)), timedelta(seconds=(be - bs))))
                    sleep(args.sleep)  # avoid flooding ES
                    if os.path.isfile(args.log_agg):
                        if args.no_check:
                            logme("   Skipping ES consistency check.")
                        else:
                            cnt_removed = check_docs(args.log_agg, args)
                            if (cnt_removed == -1):
                                logme("WARNING - check_docs couldn't be successfully executed for log_agg {}".format(args.log_agg))
                                erroroccurred = True
                                removed = 0
                                break
                            removed = cnt_removed
                        total += removed
                        logme("     2ndChck removed {}, in total {:,}".format(removed, total))
                        os.remove(args.log_agg)
                if (('_esdedup_changed_writeflag' in idx2settings[idxname]) and (idx2settings[idxname]['_esdedup_changed_writeflag'] == True)):
                    if (set_index_writable(args, idxname, idx2settings[idxname]['write']) == False):
                        logme("WARNING - Index '{0}' writable setting could not be reset to {1}.".format(idxname, idx2settings[idxname]['write']))
                    else:
                        idx2settings[idxname]['_esdedup_changed_writeflag'] = False
        if (removed == 0):
            if (erroroccurred == False):
                continue  # continue with next index
            else:
                logme("ERROR - An error occurred with idxname {}".format(idxname))

    end = time.time()
    logme("== successfully completed dupe deletion. Took: {0}".format(timedelta(seconds=(end - start))))



def es_uri(args):
    return 'http://{0}:{1}'.format(args.host, args.port)


def bulk_uri(args):
    return '{0}/_bulk?refresh=wait_for'.format(es_uri(args))


def msearch_uri(args):
    return '{0}/_msearch/template'.format(es_uri(args))


def search_uri(idxname, args):
    return '{}/{}/_search'.format(es_uri(args), idxname)


def idxlist_uri(args):          # AKA http://localhost:9200/prefix_*/_stats
    if (args.all == True):
        return '{}/{}{}*/_stats'.format(es_uri(args), args.prefix, args.prefixseparator)
    else:
        return '{}/{}/_stats'.format(es_uri(args), args.index)


def settings_uri(idxname, args):
    return "{}/{}/_settings".format(es_uri(args), idxname)


def allsettings_uri(args):
    return "{}/_all/_settings".format(es_uri(args))


def fetch_indexlist(args):
    global es_headers
    uri = idxlist_uri(args)
    payload = {}
    try:
        json = ujson.dumps(payload)
        if args.verbose:
            logme("## GET {0}".format(uri))
            logme("##\tdata. {0}".format(json))
        resp = requests.get(uri, data=json, headers=es_headers)
        if args.debug:
            logme("## resp: {0}".format(resp.text))
        if (resp.status_code == 200):
            r = ujson.loads(resp.text)
            return r
        else:
            logme("ERROR - failed to fetch indexlist for uri {0}: {1}".format(uri, resp.text))
            sys.exit(-1)
    except requests.exceptions.ConnectionError as e:
        logme("ERROR - connection failed, check --host argument and port. Is ES running on {0} ?".format(es_uri(args)))
        logme(e)
    return 0


def fetch(idxname, args):
    global es_headers
    uri = search_uri(idxname, args)
    payload = {"size": 0,
                "aggs": {
                    "duplicateCount": {"terms":
                            {"field": args.field, "min_doc_count": 2, "size": args.batch},
                                "aggs": {
                                    "duplicateDocuments":
                                    # TODO: _source can contain custom fields, when empty whole document is trasferred
                                    # which causes unnecessary traffic
                                    {"top_hits": {"size": args.dupes, "_source": [args.field]}}
                                }
                            }
                    }
               }
    try:
        json = ujson.dumps(payload)
        if args.verbose:
            logme("# idxname {0}: POST {1}".format(idxname, uri))
            logme("#\tquery: {0}".format(json))
        resp = requests.post(uri, data=json, headers=es_headers)
        if args.debug:
            logme("## idxname {0}, resp: {1}".format(idxname, resp.text))
        if (resp.status_code == 200):
            r = ujson.loads(resp.text)
            return r
        else:
            logme("ERROR - failed to fetch duplicates: {0}".format(resp.text))
    except requests.exceptions.ConnectionError as e:
        logme("ERROR - connection failed, check --host argument and port. Is ES running on {0}?".format(es_uri(args)))
        logme(e)
    return -1


def remove_duplicates(json, idxname, args):
    docs = []
    ids = []
    if (args.debug == True):
        logme("## idxname {}: using json:\n{}\n\n".format(idxname, pp.pformat(json, 4, -1)))
#{u'_shards': {u'failed': 0, u'skipped': 0, u'successful': 4, u'total': 4},
# u'aggregations': {u'duplicateCount': {u'buckets': [{u'doc_count': 1170,
#                                                     u'duplicateDocuments': {u'hits': {u'hits': [{u'_id': u'some-uuid-real-ly',
#                                                                                                  u'_index': u'someindexname',
#                                                                                                  u'_score': 1.0,
#                                                                                                  u'_source': {u'fingerprint': 1416643593},
#                                                                                                  u'_type': u'message'},
#                                                                                                 {u'_id': u'some-other-uuid-ly',
#                                                                                                  u'_index': u'someindexname',
#                                                                                                  u'_score': 1.0,
#                                                                                                  u'_source': {u'fingerprint': 1416643593},
#                                                                                                  u'_type': u'message'},
    for bucket in json["aggregations"]["duplicateCount"]["buckets"]:
        docs.append("{}:{}/{}/{}".format(bucket['key'], idxname, args.doc_type, bucket["duplicateDocuments"]["hits"]["hits"][0]["_id"]))
        i = 0
        for dupl in bucket["duplicateDocuments"]["hits"]["hits"]:
            if (i > 0):
                ids.append(dupl["_id"])
            else:
                if args.debug:
                    logme("## idxname {}: skipping doc {0}".format(idxname, dupl["_id"]))
            i += 1

    buf = StringIO()
    for i in ids:
        add_to_delete_query(buf, idxname, args.doc_type, i)
    if args.debug:
        logme("## idxname {}, bulk_delete-query-buffer:\n{}\n\n".format(idxname, buf.getvalue()))

    erroroccurred = False
    cnt_removed = 0
    if (buf.getvalue() != ""):
        cnt_removed = bulk_remove(buf, args)
        if (cnt_removed == -1):
            logme("WARNING - couldn't be successfully executed for buf {}".format(buf.getvalue()))
            erroroccurred = True
            cnt_removed = 0
    removed = cnt_removed

    buf.close()
    if (removed > 0):
        # log document IDs with their indexes
        with io.open(args.log_agg, mode='a', encoding='utf-8') as f:
            f.write(u'\n'.join(docs))
            f.write(u'\n')
    if (erroroccurred == True):
        return -1
    else:
        return removed


# write query into string buffer
def add_to_delete_query(buf, idxname, doc_type, i):
    buf.write('{"delete":{"_index":"')
    buf.write(idxname)
    buf.write('","_type":"')
    buf.write(doc_type)
    buf.write('","_id":"')
    buf.write(i)
    buf.write('"}}\n')


def log_done(buf, doc, idxname, type, id):
    buf.write(doc)
    buf.write(':')
    buf.write(idxname)
    buf.write('/')
    buf.write(type)
    buf.write('/')
    buf.write(id)
    buf.write('\n')


# returns number of deleted items
def bulk_remove(buf, args):
    global es_headers
    try:
        uri = bulk_uri(args)
        if args.verbose:
            logme("# POST {}".format(uri))
        if args.noop:
            logme("NOT using delete query: {}".format(buf.getvalue()))
            return 0

        resp = requests.post(uri, data=buf.getvalue(), headers=es_headers)
        if args.debug:
            logme("## resp: {0}".format(resp.text))
        if (resp.status_code == 200):
            r = ujson.loads(resp.text)
            if r['errors']:
                logme("got errors in r:\n{}\n\n".format(r))
            cnt = 0
            for item in r['items']:
                if ('found' in item['delete']) and item['delete']['found']:
                    cnt += 1
                else:
                    logme("    {}".format(item))
            return cnt
        else:
            logme("ERROR - failed to fetch duplicates: #{0}".format(resp.text))
    except requests.exceptions.ConnectionError as e:
        logme("ERROR - connection failed, check --host argument and port. Is ES running on {0}?".format(es_uri(args)))
        logme(e)

    # an error occurred!
    return -1




def fetch_allsettings(args):
    global es_headers
    tmpidx2settings = {}
    try:
        uri = allsettings_uri(args)
        if args.verbose:
            logme("# GET {}".format(uri))
        resp = requests.get(uri, data={}, headers=es_headers)
        # {"indexname_109":{"settings":{"index":{"number_of_shards":"4","blocks":{"write":"false","metadata":"false","read":"false"},"provided_name":"indexname_109","creation_date":"1520121603118","analysis":{"analyzer":{"analyzer_keyword":{"filter":"lowercase","tokenizer":"keyword"}}},"number_of_replicas":"0","uuid":"some-uuid-really-now","version":{"created":"5060499"}}}}, ....}
        r = {}
        if args.debug:
            logme("## resp: {0}".format(resp.text))
        r = ujson.loads(resp.text)
        if ('errors' in r):
            logme(r)
        for idxname in r:
            tmpblocks = {}
            if (('settings' in r[idxname]) and ('index' in r[idxname]['settings'])):
                if ('blocks' in r[idxname]['settings']['index']):
                    tmpblocks = r[idxname]['settings']['index']['blocks']
                    tmpblocks['_esdedup_changed_writeflag'] = False         # sic, we are using a python Boolean here, instead of json text "bool"
                elif ('uuid' in r[idxname]['settings']['index']):
                    tmpblocks = {}
                else:
                    tmpblocks = None
            if (tmpblocks != None):
                if (idxname not in tmpidx2settings):
                    tmpidx2settings[idxname] = copy.copy(tmpblocks)

    except requests.exceptions.ConnectionError as e:
        logme("ERROR - connection failed, check --host argument and port. Is ES running on {0}?".format(es_uri(args)))
        logme(e)
    if args.debug:
        global pp
        logme("Got idx2settings as\n{}\n\n".format(pp.pformat(tmpidx2settings, 4, -1)))
    return tmpidx2settings


def set_index_writable(args, idxname, flag):
    global es_headers
    rc = False
    try:
        if (flag == "true"): flag = "true"
        else: flag = "false"
        uri = settings_uri(idxname, args)
        payload = {"index": { "blocks": { "write": flag } } }
        json = ujson.dumps(payload)
        if args.verbose:
            logme("# idxname {0}: PUT {1}".format(idxname, uri))
            logme("#\tquery: {0}".format(json))
        resp = requests.put(uri, data=json, headers=es_headers)
        r = {}
        if args.debug:
            logme("## idxname {0}, resp: {1}".format(idxname, resp.text))
        r = ujson.loads(resp.text)
        if ('errors' in r):
            logme("\terrors occurred:\n{}\n\n".format(r))
        else:
            rc = True
    except requests.exceptions.ConnectionError as e:
        logme("ERROR - connection failed, check --host argument and port. Is ES running on {0}?".format(es_uri(args)))
        logme(e)
    return rc


def check_docs(file, args):
    deleted = 0
    if os.path.isfile(file):
        items = 0
        total = 0
        buf = StringIO()
        stats = defaultdict(int)
        checkdocserrorsoccurred = False
        with open(file) as f:
            for line in f:
                if (':' in line):
                    parts = line.split(":")
                    uri = parts[1].split("/")
                    buf.write('{"index":"')
                    buf.write(uri[0])
                    buf.write('"}\n{"inline":{"query":{"match":{"')
                    buf.write(args.field)
                    buf.write('":"')
                    buf.write(parts[0])
                    buf.write('"}},"_source":["')
                    buf.write(args.field)
                    buf.write('"]}}}\n')
                else:
                    logme("invalid line {}: {}".format(items, line))
                items += 1
                if (items >= args.flush):
                    cnt_deleted = msearch(buf.getvalue(), args, stats, items)
                    if (cnt_deleted == -1):
                        logme("WARNING - msearch could not be successfully executed for buf {}".format(buf.getvalue()))
                        checkdocserrorsoccurred = True
                    else:
                        deleted += cnt_deleted
                        total += items
                    buf = StringIO()
                    items = 0
        if (items > 0):
            cnt_deleted = msearch(buf.getvalue(), args, stats, items)
            if (cnt_deleted == -1):
                logme("WARNING - msearch could not be successfully executed for buf {}".format(buf.getvalue()))
                checkdocserrorsoccurred = True
            else:
                deleted += cnt_deleted
                total += items
        print_stats("== Consistency check", stats, args)
        sum = 0
        for k, v in stats.items():
            sum += v
        if (sum < total):
            logme("Queried for {} documents, retrieved status of {} ({:.2f}%).".format(total, sum, sum/total*100))
            logme("WARNING - Check your ES status and configuration!")
            # rather exit, or we'd be querying an incomplete cluster
            sys.exit(3)
        if (deleted == 0) and (checkdocserrorsoccurred == True):
            deleted = -1
        return deleted
    else:
        logme("ERROR - {} is not a file".format(file))
        sys.exit(1)


def msearch(query, args, stats, docs):
    global es_headers
    cnt_deleted = 0
    try:
        uri = msearch_uri(args)
        if args.verbose:
            logme("# querying for {} documents. GET {}".format(docs, uri))
        if args.debug:
            logme("## query: {}".format(query))
        attempt = 0
        to_del = StringIO()
        to_log = StringIO()
        while True:
            resp = requests.get(uri, data=query, headers=es_headers)
            if args.debug:
                logme("## resp: {0}".format(resp.text))
            if (resp.status_code == 200):
                r = ujson.loads(resp.text)
                if ('error' in r and attempt < 5):
                    attempt += 1
                    logme("query failed with: {}".format(r['error']))
                    logme('retrying in {}s...'.format(args.sleep))
                    sleep(args.sleep)
                    continue

                if ('responses' in r):
                    curr = defaultdict(int)
                    for doc in r['responses']:
                        if ('hits' in doc) and ('total' in doc['hits']):
                            num = doc['hits']['total']
                            curr[num] += 1
                            # a doc to remain in ES
                            if ('hits' in doc['hits']) and (len(doc['hits']['hits']) > 0):
                                remain = doc['hits']['hits'][0]
                                log_done(to_log, str(remain['_source'][args.field]), remain['_index'], remain['_type'], remain['_id'])
                            else:
                                if args.debug:
                                    logme("## missing doc: {}".format(doc['hits']))
                                stats[0] += 1
                            if (num > 1):
                                j = 0
                                for dupl in doc['hits']['hits']:
                                    if (j > 0):
                                        add_to_delete_query(to_del, dupl['_index'], dupl['_type'], dupl['_id'])
                                    j += 1

                        else:
                            logme("incomplete response: {}".format(doc))
                            attempt += 1
                            if (attempt < 5):
                                sleep(args.sleep)
                                continue
                            else:
                                logme("ERROR - ES failed to respond correctly !")
                                break
                    # if all queries succeeded update global stats
                    for k, v in curr.items():
                        stats[k] += v
                    if args.debug:
                        print_stats("Batch", curr, args)
                else:
                    logme("unexpected response: {}".format(resp.text))
                    sys.exit(5)
                if args.verbose:
                    print_stats("Current state", stats, args)
            if (to_del.tell() > 0):
                if args.noop:
                    logme("NOT deleting the following:\n{}\n\n".format(to_del.getvalue()))
                else:
                    if args.verbose:
                        logme("# removing redundant {} documents".format(to_del.tell()))
                    cnt_deleted = bulk_remove(to_del, args)
                    if (cnt_deleted == -1):
                        logme("WARNING - bulk_remove couldn't execute successfully for todel: {}".format(todel.tell()))
                    else:
                        to_del = StringIO()
                    # log docs as done
            with io.open(args.log_done, mode='a', encoding='utf-8') as f:
                f.write(to_log.getvalue())
            to_log.close()
            to_log = StringIO()
            break
        else:
            logme("ERROR - failed to execute search query, got resp: #{0}".format(resp.text))
        to_del.close()
    except requests.exceptions.ConnectionError as e:
        logme("ERROR - connection failed, check --host argument and port. Is ES running on {0}?".format(es_uri(args)))
        logme(e)
    return cnt_deleted


def print_stats(msg, stats, args):
    sum = 0
    for key, value in stats.items():
        sum += value
    ok = 0
    if (1 in stats):
        ok = stats[1]
    missing = 0
    if (0 in stats):
        missing = stats[0]
    logme("{}. OK: {} ({:.2f}%) out of {}. Fixable: {}. Missing: {}".format(msg, ok, (ok/sum*100.0), sum, (sum-ok-missing), missing))
    if args.verbose:
        logme("# stats: {}", stats)


if (__name__ == "__main__"):
    import argparse

    parser = argparse.ArgumentParser(description="Elastic duplicates deleter",add_help=True)
    parser.add_argument("-a", "--all",
                        action="store_true", dest="all", default=True,
                        help="All indexes from given date till today")
    parser.add_argument("-b", "--batch",
                        dest="batch", default=10, type=int,
                        help="Batch size - how many documents are retrieved using one request")
    parser.add_argument("-m", "--max_dupes",
                        dest="dupes", default=10, type=int,
                        help="Dupes size - how many duplicates per document are retrieved")
    parser.add_argument("-H", "--host", dest="host",
                        default="localhost",
                        help="Elasticsearch hostname", metavar="host")
    parser.add_argument("-f", "--field", dest="field",
                        default="Uuid",
                        help="Field in ES that is supposed to be unique", metavar="field")
    parser.add_argument("--flush",
                        dest="flush", default=500, type=int,
                        help="Number records send in one bulk request")
    parser.add_argument("-i", "--index", dest="index",
                        default="",
                        help="Elasticsearch full index name, implies NOT --all", metavar="index")
    parser.add_argument("-I", "--indexexclude", dest="indexexclude",
                        default="",
                        help="Elasticsearch regular expression of index name that is to be excluded, only useful with --all", metavar="indexexclude-regexp")
    parser.add_argument("-p", "--prefix", dest="prefix",
                        default="*",
                        help="Elasticsearch index prefix", metavar="prefix")
    parser.add_argument("-S", "--prefixseparator", dest="prefixseparator",
                        default="-",
                        help="Elasticsearch index prefix separator to use between prefix, idxname and *", metavar="prefixsep")
    parser.add_argument("-P", "--port", dest="port",
                        default=9200, type=int,
                        help="Elasticsearch port", metavar="port")
    parser.add_argument("-t", "--doc_type", dest="doc_type",
                        default="_doc",
                        help="ES document type")
    parser.add_argument("-v", "--verbose",
                        action="store_true", dest="verbose",
                        default=False,
                        help="enable verbose logging")
    parser.add_argument("-d", "--debug",
                        action="store_true", dest="debug",
                        default=False,
                        help="enable debugging")
    parser.add_argument("--no-check",
                        action="store_true", dest="no_check",
                        default=False,
                        help="Disable check & remove if duplicities found after with standard search query")
    parser.add_argument("--log_agg", dest="log_agg",
                        default="es_dedupe.log",
                        help="Logfile for partially deleted documents (documents found by aggregate queries)")
    parser.add_argument("--log_done", dest="log_done",
                        default="es_dedupe.done",
                        help="Logfile containing all document IDs that remained in ES")
    parser.add_argument("--check_log", dest="check",
                        help="Verify that documents has been deleted")
    parser.add_argument("--sleep",
                        dest="sleep", default=60, type=int,
                        help="Sleep in seconds after each ES query (in order to avoid cluster overloading)")
    parser.add_argument("-n", "--noop",
                        action="store_true", dest="noop",
                        default=False,
                        help="Do not take any destructive action (only print delete queries)")

    args = parser.parse_args()
    logme("== Starting ES dupe deleter....")
    if args.verbose:
        logme("# Called with args: {}".format(args))
    try:
        if (args.indexexclude != ""):
            re_indexexclude = re.compile(args.indexexclude)
        if args.check:
            check_docs(args.check, args)
        else:
            run(args)
    except KeyboardInterrupt:
        logme('Interrupted by Keyboard')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
