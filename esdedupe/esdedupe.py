#!/usr/bin/env python

# -*- coding: utf-8 -*-

import hashlib
import time
import tqdm
import ujson
import requests
import sys

from benedict import benedict
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import parallel_bulk
from elasticsearch.helpers import streaming_bulk
from logging import getLogger
from datetime import timedelta

from . import __VERSION__
from .utils import memusage, time_to_sec, to_es_date


class Esdedupe:

    def __init__(self):
        self.log = getLogger('esdedupe')
        self.total = 0

    # Process documents returned by the current search/scroll
    def build_index(self, docs_hash, unique_fields, hit):
        hashval = None
        hit_benedict = benedict(hit)
        _id = hit_benedict["_id"]
        # there's no need to hash, if we have just single unique key
        if len(unique_fields) > 1:
            combined_key = ""
            for field in unique_fields:
                combined_key += str(hit_benedict['_source'][field])
            hashval = hashlib.md5(combined_key.encode('utf-8')).digest()
        else:
            hashval = str(hit_benedict['_source'][unique_fields[0]])

        docs_hash.setdefault(hashval, []).append(_id)

    def elastic_uri(self, args):
        if args.host.startswith('http'):
            return '{0}:{1}'.format(args.host, args.port)
        else:
            if args.ssl:
                return 'https://{0}:{1}'.format(args.host, args.port)
            else:
                return 'http://{0}:{1}'.format(args.host, args.port)

    def ping(self, args):
        uri = self.elastic_uri(args)
        if not args.cert_verify:
            requests.urllib3.disable_warnings()
        try:
            self.log.debug("GET {0}".format(uri))
            if args.user:
                from requests.auth import HTTPBasicAuth
                resp = requests.get(uri,
                                    auth=HTTPBasicAuth(args.user, args.password),
                                    verify=args.cert_verify)
            else:
                resp = requests.get(uri,
                                    verify=args.cert_verify)
            self.log.debug("Response: {0}".format(resp.text))
            if (resp.status_code == 200):
                return
            else:
                self.log.error("{0}: {1}".format(uri, resp.text))
                sys.exit(1)
        except requests.exceptions.SSLError as e:
            self.log.error("Certificate verification failed on {0} , use -k  to skip checking the certificate".format(uri))
            self.log.error(e)
            sys.exit(1)
        except requests.exceptions.ConnectionError as e:
            self.log.error(
                "Connection failed. Is ES running on {0} ?".format(uri))
            self.log.error("Check --host argument and --port")
            self.log.error(e)
            # do not show this terrible traceback
            # self.log.error(e)
            sys.exit(1)
        return

    def run(self, args):
        start = time.time()
        uri = self.elastic_uri(args)
        self.log.info(
            "Starting esdedupe: {} - duplicate document removal tool".format(__VERSION__))
        if args.noop:
            self.log.info("Running in NOOP mode, no document will be deleted.")
        try:
            # test connection to Elasticsearch cluster first
            self.ping(args)
            if args.user:
                es = Elasticsearch([uri],
                                   basic_auth=(args.user, args.password),
                                   verify_certs=args.cert_verify,
                                   ssl_show_warn=args.cert_verify)
            else:
                es = Elasticsearch(hosts=[uri],
                                   verify_certs=args.cert_verify,
                                   ssl_show_warn=args.cert_verify)

            resp = es.info()
            self.log.info("elastic: {}, host: {}, version: {}".format(
                resp['cluster_name'], args.host, resp['version']['number']))

            docs = {}
            dupl = 0

            # one or more fields to form a unique key (primary key)
            pk = args.field.split(',')
            self.log.info("Unique fields: {}".format(pk))

            if args.index != "":
                index = args.index
                # if indexname specifically was set, do not do --all mode
                args.all = False
                self.process_index(es, docs, pk, dupl, index, args)

            end = time.time()
            if args.noop:
                self.log.info("Simulation finished. Took: {0}".format(
                    timedelta(seconds=(end - start))))
            else:
                if dupl > 0:
                    self.log.info("""Successfully completed duplicates removal.
                                  Took: {0}""".format(timedelta(seconds=(end - start)))
                                  )
                else:
                    self.log.info("Total time: {0}".format(
                        timedelta(seconds=(end - start))))

        except ConnectionError as e:
            self.log.error(e)


    def process_index(self, es, docs, pk, dupl, index, args):
        if args.window:
            if not args.timestamp:
                self.log.error("Please specify --timestamp field")
                sys.exit(1)
            if not args.since:
                self.log.error("Please specify --since %Y-%m-%d\"'T'\"%H:%M:%S timepoint")
                sys.exit(1)
            if not args.until:
                self.log.error("Please specify --until %Y-%m-%d\"'T'\"%H:%M:%S timepoint")
                sys.exit(1)

            win = time_to_sec(args.window)
            self.log.info("Timestamp based search, with window {} from {} until {}".format(
                args.window, args.since, args.until))

            end = args.until

            currStart = args.since
            currEnd = args.since + timedelta(seconds=win)
            self.total = 0
            # scan & remove using sliding window
            while currEnd < end:
                docs = {} # avoid deleting same documents again and again
                self.log.info("Using window {}, from: {} until: {}".format(
                    args.window, to_es_date(currStart), to_es_date(currEnd)))
                args.since = currStart
                args.until = currEnd
                self.total += self.scan_and_remove(es, docs, pk, dupl, index, args)
                currStart += timedelta(seconds=win)
                currEnd += timedelta(seconds=win)

            if currEnd != end:
                self.log.info("Last check, from: {} until: {}".format(
                        to_es_date(currStart), to_es_date(end)))
                args.since = currStart
                args.until = end
                self.total += self.scan_and_remove(es, docs, pk, dupl, index, args)
        else:
            # "normal" index without timestamps
            self.total += self.scan_and_remove(es, docs, pk, dupl, index, args)
        self.log.info("Altogether {} documents were removed (including doc replicas)".format(self.total))

    def scan(self, es, docs_hash, unique_fields, index, args):
        i = 0
        self.log.info("Building documents mapping on index: {}, batch size: {}".format(
            index, args.batch))
        for hit in helpers.scan(es, index=index, size=args.batch,
                                query=self.es_query(args), scroll=args.scroll, request_timeout=args.request_timeout):
            self.build_index(docs_hash, unique_fields, hit)
            i += 1
            if (i % args.mem_report == 0):
                self.log.debug(
                    "Scanned {:0,} unique documents, memory usage: {}".format(
                        len(docs_hash), memusage()))
        return self.count_duplicates(docs_hash)

    def scan_and_remove(self, es, docs_hash, unique_fields, dupl, index, args):
        # find duplicate documents
        dupl = self.scan(es, docs_hash, unique_fields, index, args)
        if dupl == 0:
            self.log.info("No duplicates found")
        else:
            self.total = len(docs_hash)
            self.log.info(
                "Found {:0,} duplicates out of {:0,} docs, unique documents: {:0,} ({:.1f}% duplicates)".format(
                    dupl, dupl+self.total, self.total, dupl/(dupl+self.total)*100)
                )

            if args.log_dupl:
                self.save_documents_mapping(docs_hash, args)
            if args.noop:
                self.log.info("""In order to print matching IDs to stdout run with
                              --debug flag or save results to JSON file using --log_dupl docs.json""")
                if args.debug:
                    self.print_duplicates(docs_hash, index, es, args)
            else:
                if args.threads > 1:
                    return self.parallel_delete(docs_hash, index, es, args, dupl)
                else:
                    # safer option, should avoid overloading elastic
                    return self.sequential_delete(docs_hash, index, es, args, dupl)
        return 0

    def es_query(self, args):
        if args.timestamp:
            filter = {"format": "strict_date_optional_time"}
            if args.since:
                # Greater than or equal to
                filter['gte'] = to_es_date(args.since)
            if args.until:
                # Less than
                filter['lt'] = to_es_date(args.until)
            query = {
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "range": {
                                    args.timestamp: filter
                                }
                            }
                        ]
                    }
                }
            }
            return query
        else:
            {}

    def print_duplicates(self, docs_hash, index, es, args):
        for hashval, ids in docs_hash.items():
            if len(ids) > 1:
                # Get the documents that have mapped to the current hasval
                matching_docs = es.mget(index=index, body={"ids": ids})
                for doc in matching_docs['docs']:
                    print("doc=%s" % doc)

    # For catching Elasticsearch exceptions
    def wrapper(self, gen):
        while True:
            try:
                yield next(gen)
            except StopIteration:
                break
            except Exception as e:
                # TODO: after catching exception we're unable to continue
                # which is good, we don't overload ES cluster
                self.log.error(e)

    def sequential_delete(self, docs_hash, index, es, args, duplicates):
        if not args.no_progress:
            progress = tqdm.tqdm(unit="docs", total=duplicates)
        successes = 0

        for success, info in self.wrapper(streaming_bulk(es, self.delete_iterator(docs_hash, index, args),
                                                         max_retries=args.max_retries, initial_backoff=args.initial_backoff,
                                                         request_timeout=args.request_timeout, chunk_size=args.flush,
                                                         raise_on_exception=args.fail_fast)):
            if success:
                successes += info['delete']['_shards']['successful']
            else:
                print('Doc failed', info)
            if not args.no_progress:
                progress.update(1)

        self.log.info(
            "Deleted {:0,} documents (including shard replicas)".format(successes))
        return successes

    def parallel_delete(self, docs_hash, index, es, args, duplicates):
        if not args.no_progress:
            progress = tqdm.tqdm(unit="docs", total=duplicates)
        successes = 0

        for success, info in self.wrapper(parallel_bulk(es, self.delete_iterator(docs_hash, index, args),
                                                        thread_count=args.threads, request_timeout=args.request_timeout,
                                                        chunk_size=args.flush, raise_on_exception=args.fail_fast)):
            if success:
                successes += info['delete']['_shards']['successful']
            else:
                print('Doc failed', info)
            if not args.no_progress:
                progress.update(1)

        self.log.info(
            "Deleted {:0,} documents (including shard replicas)".format(successes))
        return successes

    def delete_iterator(self, docs_hash, index, args):
        for hashval, ids in docs_hash.items():
            if len(ids) > 1:
                i = 0
                for doc_id in ids:
                    if i > 0:  # skip first document
                        doc = {
                            '_op_type': 'delete',
                            '_index': index,
                            '_id': doc_id
                        }
                        if args.doc_type:
                            doc['_type'] = args.doc_type
                        yield doc
                    i += 1

    def count_duplicates(self, docs_hash):
        duplicates = 0
        for hashval, ids in docs_hash.items():
            size = len(ids)
            if size > 1:
                duplicates += size - 1
        return duplicates

    def save_documents_mapping(self, docs_hash, args):
        self.log.info(
            "Storing documents mapping into: {}".format(args.log_dupl))
        with open(args.log_dupl, "w", encoding="utf8") as ujson_file:
            ujson.dump(docs_hash, ujson_file)
