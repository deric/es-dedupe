#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import os.path
import requests
import sys
import time
import ujson
from io import StringIO
from collections import defaultdict
from datetime import timedelta
from time import sleep

def msg_using(prefix, index):
    if prefix:
        print('Using index {}-{}'.format(prefix, index))
    else:
        print('Using index {}'.format(index))

def idx_name(args, index):
    # when index prefix is defined
    if args.prefix:
        return "{}-{}".format(args.prefix, index)
    return index

def run(args):
    start = time.time()
    total = 0
    msg_using(args.prefix, args.index)
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y.%m.%d")
    index = args.index
    while (index != tomorrow):
        while True:
            qs = time.time()
            idx = idx_name(args, index)
            resp = fetch(idx, args)
            qe = time.time()
            docs = 0
            removed = 0
            if isinstance(resp, dict) and ("aggregations" in resp):
                docs = len(resp["aggregations"]["duplicateCount"]["buckets"])
            else:
                print("ERROR: Unexpected response {}".format(resp))
                sys.exit()
            print("ES query took {}, retrieved {} unique docs".format(timedelta(seconds=(qe - qs)),docs))
            if docs > 0:
                removed = remove_duplicates(resp, idx, args)
                be = time.time()
                total += removed
                print("Deleted {0} duplicates, in total {1}. Batch processed in {2}, running time {3}".format(removed, total, timedelta(seconds=(be - qe)),timedelta(seconds=(be - start))))
                sleep(args.sleep) # avoid flooding ES
            if os.path.isfile(args.log_agg):
                if args.no_chck:
                    print("Skipping ES consistency check.")
                else:
                    removed = check_docs(args.log_agg, args)
                    total += removed
                    print("Secondary check removed {}, in total {}".format(removed, total))
                    os.remove(args.log_agg)

            if removed == 0:
                break # continue with next index
        if (not args.all):
            break # process only one index
        if not args.prefix:
            break
        index = inc_day(index)
        msg_using(args.prefix, index)
    end = time.time()
    print("== de-duplication process completed successfully. Took: {0}".format(timedelta(seconds=(end - start))))

def inc_day(str_date):
    return (datetime.datetime.strptime(str_date, "%Y.%m.%d").date() + datetime.timedelta(days=1)).strftime("%Y.%m.%d")

def es_uri(args):
    return 'http://{0}:{1}'.format(args.host, args.port)

# we have to wait for updating index, otherwise we might be deleting documents that are no longer
# duplicates
def bulk_uri(args):
    return '{0}/_bulk?refresh=wait_for'.format(es_uri(args))

def msearch_uri(args):
    return '{0}/_msearch/template'.format(es_uri(args))

def search_uri(index, args):
    return '{}/{}/_search'.format(es_uri(args), index)

def fetch(index, args):
    uri = search_uri(index, args)
    payload = {"size": 0,
                "aggs":{
                    "duplicateCount":{"terms":
                        {"field": args.field,"min_doc_count": 2,"size":args.batch},
                        "aggs":{
                            "duplicateDocuments":
                                # TODO: _source can contain custom fields, when empty whole document is trasferred
                                # which causes unnecessary traffic
                                {"top_hits":{"size": args.dupes, "_source":[args.field]}}
                          }
                        }
                }
            }
    try:
        json = ujson.dumps(payload)
        if args.verbose:
            print("POST {0}".format(uri))
            print("\tdata: {0}".format(json))
        resp = requests.post(uri, data=json)
        if args.debug:
            print("resp: {0}".format(resp.text))
        if resp.status_code == 200:
            r = ujson.loads(resp.text)
            return r
        else:
            print("failed to fetch duplicates: #{0}".format(resp.text))
    except requests.exceptions.ConnectionError as e:
        print("ERROR: connection failed, check --host argument and port. Is ES running on {0}?".format(es_uri(args)))
        print(e)
    return 0

def remove_duplicates(json, index, args):
    docs = []
    ids = []
    idx = index
    for bucket in json["aggregations"]["duplicateCount"]["buckets"]:
        docs.append("{}:{}/{}/{}".format(bucket['key'], idx, args.doc_type, bucket["duplicateDocuments"]["hits"]["hits"][0]["_id"]))
        #print("bucket: {0}".format(bucket))
        i = 0
        for dupl in bucket["duplicateDocuments"]["hits"]["hits"]:
            if i > 0:
                ids.append(dupl["_id"])
            else:
                if args.verbose:
                    print("skipping doc {0}".format(dupl["_id"]))
            i += 1
    buf = StringIO()
    for i in ids:
        delete_query(buf, idx, args.doc_type, i)

    removed = bulk_remove(buf, args)
    buf.close()
    if removed > 0:
        # log document IDs with their indexes
        with open(args.log_agg, mode='a', encoding='utf-8') as f:
            f.write('\n'.join(docs))
            f.write('\n')
    return removed

# write query into string buffer
def delete_query(buf, index, doc_type, i):
    buf.write('{"delete":{"_index":"')
    buf.write(index)
    buf.write('","_type":"')
    buf.write(doc_type)
    buf.write('","_id":"')
    buf.write(i)
    buf.write('"}}\n')

def log_done(buf, doc, index, type, id):
    buf.write(doc)
    buf.write(':')
    buf.write(index)
    buf.write('/')
    buf.write(type)
    buf.write('/')
    buf.write(id)
    buf.write('\n')

# returns number of deleted items
def bulk_remove(buf, args):
    try:
        uri = bulk_uri(args)
        if args.verbose:
            print("POST {}".format(uri))
        if args.noop:
            print("== only simulation")
            print("Delete query: {}".format(buf.getvalue()))
            return 0

        resp = requests.post(uri, data=buf.getvalue())
        if args.debug:
            print("resp: {0}".format(resp.text))
        if resp.status_code == 200:
            r = ujson.loads(resp.text)
            if r['errors']:
                print(r)
            cnt = 0
            for item in r['items']:
                if ('found' in item['delete']) and item['delete']['found'] == True:
                    cnt += 1
                else:
                    print(item)
            return cnt
        else:
            print("failed to fetch duplicates: #{0}".format(resp.text))
    except requests.exceptions.ConnectionError as e:
        print("ERROR: connection failed, check --host argument and port. Is ES running on {0}?".format(es_uri(args)))
        print(e)

def check_docs(file, args):
    deleted = 0
    if os.path.isfile(file):
        i = 0
        total = 0
        buf = StringIO()
        stats = defaultdict(int)
        with open(file) as f:
            for line in f:
                if ':' in line:
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
                    print("invalid line {}: {}".format(i, line))
                i += 1
                if i >= args.flush:
                    total += i
                    deleted += msearch(buf.getvalue(), args, stats, i)
                    buf = StringIO()
                    i = 0
        if i > 0:
            total += i
            deleted += msearch(buf.getvalue(), args, stats, i)
        print_stats("== Total", stats, args)
        sum = 0
        for k, v in stats.items():
            sum += v
        if sum < total:
            print("Queried for {} documents, retrieved status of {} ({:.2f}%).".format(total, sum, sum/total*100))
            print("WARNING: Check your ES status and configuration!")
            # rather exit, we'd quering incomplete cluster
            sys.exit(3)
        return deleted
    else:
        print("{} is not a file".format(file))
        sys.exit(1)

def msearch(query, args, stats, docs):
    cnt_deleted = 0
    try:
        uri = msearch_uri(args)
        if args.verbose:
            print("Quering for {} documents. GET {}".format(docs, uri))
        if args.debug:
            print("msearch: {}".format(query))
        attempt = 0
        to_del = StringIO()
        to_log = StringIO()
        while True:
            resp = requests.get(uri, data=query)
            if args.debug:
                print("resp: {0}".format(resp.text))
            if resp.status_code == 200:
                r = ujson.loads(resp.text)
                if 'error' in r and attempt < 5:
                    attempt += 1
                    print("Query failed: {}".format(r['error']))
                    print('Retrying in {}s...'.format(args.sleep))
                    sleep(args.sleep)
                    continue

                if 'responses' in r:
                    curr = defaultdict(int)
                    for doc in r['responses']:
                        if 'hits' in doc and 'total' in doc['hits']:
                            num = doc['hits']['total']
                            curr[num] += 1
                            # a doc to remain in ES
                            if 'hits' in doc['hits'] and len(doc['hits']['hits']) > 0:
                                remain = doc['hits']['hits'][0]
                                log_done(to_log, remain['_source'][args.field], remain['_index'], remain['_type'], remain['_id'])
                            else:
                                if args.debug:
                                    print("Missing doc: {}".format(doc['hits']))
                                stats[0] += 1
                            if num > 1:
                                j = 0
                                for dupl in doc['hits']['hits']:
                                    if j > 0:
                                        delete_query(to_del, dupl['_index'], dupl['_type'], dupl['_id'])
                                    j += 1

                        else:
                            print("Incomplete response: {}".format(doc))
                            attempt += 1
                            if attempt < 5:
                                sleep(args.sleep)
                                continue
                            else:
                                print("ES failed to respond")
                                break
                    # if all queries succeeded update global stats
                    for k, v in curr.items():
                        stats[k] += v
                    if args.debug:
                        print_stats("Batch", curr, args)
                else:
                    print("Unexpected response: {}".format(resp.text))
                    sys.exit(5)
                if args.verbose:
                    print_stats("Current state", stats, args)
            if to_del.tell() > 0:
                if args.noop:
                    print("PRETENDING to delete:\n{}".format(to_del.getvalue()))
                else:
                    if args.verbose:
                        print("Removing redundant {} documents".format(to_del.tell()))
                    cnt_deleted = bulk_remove(to_del, args)
                    to_del = StringIO()
                    # log docs as done
            with open(args.log_done, mode='a', encoding='utf-8') as f:
                f.write(to_log.getvalue())
            to_log.close()
            to_log = StringIO()
            #sleep(args.sleep)
            break
        else:
            print("failed to execute search query: #{0}".format(resp.text))
        to_del.close()
    except requests.exceptions.ConnectionError as e:
        print("ERROR: connection failed, check --host argument and port. Is ES running on {0}?".format(es_uri(args)))
        print(e)
    return cnt_deleted

def print_stats(msg, stats, args):
    sum = 0
    for key, value in stats.items():
        sum += value
    ok = 0
    if 1 in stats:
        ok = stats[1]
    missing = 0
    if 0 in stats:
        missing = stats[0]
    print("{}. OK: {} ({:.2f}%) out of {}. Fixable: {}. Missing: {}".format(msg, ok, (ok/sum*100.0), sum, (sum-ok-missing), missing))
    if args.verbose:
        print("stats: {}", stats)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Elasticsearch deduplicator")
    parser.add_argument("-a", "--all",
                        action="store_true", dest="all",
                        default=False,
                        help="All indexes from given date till today")
    parser.add_argument("-b","--batch",
                        dest="batch", default=10, type=int,
                        help="Batch size - how many documents are retrieved using one request")
    parser.add_argument("-m","--max_dupes",
                        dest="dupes", default=10, type=int,
                        help="Dupes size - how many duplicates per document are retrieved")
    parser.add_argument("-H", "--host", dest="host",
                        default="localhost",
                        help="Elasticsearch hostname", metavar="host")
    parser.add_argument("-f", "--field", dest="field",
                        default="Uuid",
                        help="Field in ES that suppose to be unique", metavar="field")
    parser.add_argument("--flush",
                        dest="flush", default=500, type=int,
                        help="Number records send in one bulk request")
    parser.add_argument("-i", "--index", dest="index",
                        default=datetime.date.today().strftime("%Y.%m.%d"),
                        help="Elasticsearch index suffix", metavar="index")
    parser.add_argument("-p", "--prefix", dest="prefix",
                        default="",
                        help="Elasticsearch index prefix", metavar="prefix")
    parser.add_argument("-P", "--port", dest="port",
                        default=9200, type=int,
                        help="Elasticsearch pord", metavar="port")
    parser.add_argument("-t", "--doc_type", dest="doc_type",
                        default="nginx.access",
                        help="ES doctype")
    parser.add_argument("-v", "--verbose",
                        action="store_true", dest="verbose",
                        default=False,
                        help="enable verbose logging")
    parser.add_argument("-d", "--debug",
                        action="store_true", dest="debug",
                        default=False,
                        help="enable debugging")
    parser.add_argument("--no-chck",
                        action="store_true", dest="no_chck",
                        default=False,
                        help="Disable check & remove if duplicities found after with standard search query")
    parser.add_argument("--log_agg", dest="log_agg",
                        default="/tmp/es_dedupe.log",
                        help="Logfile for partially deleted documents (documents found by aggregate queries)")
    parser.add_argument("--log_done", dest="log_done",
                        default="/tmp/es_dedupe.done",
                        help="Logfile containing all document IDs that remained in ES")
    parser.add_argument("--check_log", dest="check",
                        help="Verify that documents has been deleted")
    parser.add_argument("--sleep",
                        dest="sleep", default=1, type=int,
                        help="Sleep in seconds after each ES query (in order to avoid cluster overloading)")
    parser.add_argument("-n", "--noop",
                        action="store_true", dest="noop",
                        default=False,
                        help="Do not take any destructive action (only print delete queries)")


    args = parser.parse_args()
    print("== Starting ES deduplicator....")
    if args.verbose:
        print(args)
    try:
        if args.check:
            check_docs(args.check, args)
        else:
            run(args)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
