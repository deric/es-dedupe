#!/usr/bin/env python

# -*- coding: utf-8 -*-

import hashlib
import os.path
import psutil
import time
import tqdm
import ujson
import requests
import sys

from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import parallel_bulk
from elasticsearch.helpers import streaming_bulk
from logging import getLogger
from datetime import timedelta


class Esdedupe:

    def __init__(self):
        self.log = getLogger('esdedupe')

    # Process documents returned by the current search/scroll
    def build_index(self, docs_hash, unique_fields, hit):
        hashval = None
        _id = hit["_id"]
        if len(unique_fields) > 1:
            combined_key = ""
            for field in unique_fields:
                combined_key += str(hit['_source'][field])
            hashval = hashlib.md5(combined_key.encode('utf-8')).digest()
        else:
            hashval = str(hit['_source'][unique_fields[0]])

        docs_hash.setdefault(hashval, []).append(_id)

    def bytes_fmt(self, num, suffix='B'):
        for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Y', suffix)

    def report_memusage(self):
        process = psutil.Process(os.getpid())
        rss = process.memory_info().rss
        self.log.info("Memory usage: {}".format(self.bytes_fmt(rss)))

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
        try:
            self.log.debug("GET {0}".format(uri))
            resp = requests.get(uri)
            self.log.debug("Response: {0}".format(resp.text))
            if (resp.status_code == 200):
                return
            else:
                self.log.error("{0}: {1}".format(uri, resp.text))
                sys.exit(1)
        except requests.exceptions.ConnectionError as e:
            self.log.error(
                "Connection failed. Is ES running on {0} ?".format(uri))
            self.log.error("Check --host argument and --port")
            # do not show this terrible traceback
            # self.log.error(e)
            sys.exit(1)
        return

    def run(self, args):
        start = time.time()
        total = 0

        if args.noop:
            self.log.info("Running in NOOP mode, no document will be deleted.")
        try:
            # test connection to Elasticsearch cluster first
            self.ping(args)
            if args.user:
                es = Elasticsearch([args.host],
                                   port=args.port,
                                   http_auth=(args.user, args.password),
                                   use_ssl=args.ssl
                                   )
            else:
                es = Elasticsearch([args.host],
                                   port=args.port,
                                   use_ssl=args.ssl
                                   )

            resp = es.info()
            self.log.info("elastic: {}, host: {}, version: {}".format(
                resp['cluster_name'], args.host, resp['version']['number']))

            docs_hash = {}
            dupl = 0

            # one or more fields to form a unique key
            unique_fields = args.field.split(',')
            self.log.info("Unique fields: {}".format(unique_fields))

            if args.index != "":
                index = args.index
                # if indexname specifically was set, do not do --all mode
                args.all = False
                self.scan_and_remove(
                    es, docs_hash, unique_fields, dupl, index, args)

            end = time.time()
            if args.noop:
                self.log.info("Simulation finished. Took: {0}".format(
                    timedelta(seconds=(end - start))))
            else:
                if dupl > 0:
                    self.log.info("Successfully completed duplicates removal. Took: {0}".format(
                        timedelta(seconds=(end - start))))
                else:
                    self.log.info("Total time: {0}".format(
                        timedelta(seconds=(end - start))))

        except Exception as e:
            self.log.error(e)

    def scan_and_remove(self, es, docs_hash, unique_fields, dupl, index, args):
        i = 0
        self.log.info("Building documents mapping on index: {}, batch size: {}".format(
            index, args.batch))
        for hit in helpers.scan(es, index=index, size=args.batch, query=self.es_query(args), scroll=args.scroll):
            self.build_index(docs_hash, unique_fields, hit)
            i += 1
            if args.verbose:
                if (i % 1000000 == 0):
                    self.log.info(
                        "Scanned {:0,} unique documents".format(len(docs_hash)))
                    self.report_memusage()
        dupl = self.count_duplicates(docs_hash)
        if dupl == 0:
            self.log.info("No duplicates found")
        else:
            total = len(docs_hash)
            self.log.info("Found {:0,} duplicates out of {:0,} docs, unique documents: {:0,} ({:.1f}% duplicates)".format(
                dupl, dupl+total, total, dupl/(dupl+total)*100))

            if args.log_dupl:
                self.save_documents_mapping(docs_hash, args)
            if args.noop:
                if args.verbose:
                    self.print_duplicates(docs_hash, index, es, args)
            else:
                if args.threads > 1:
                    self.parallel_delete(docs_hash, index, es, args, dupl)
                else:
                    # safer option, should avoid overloading elastic
                    self.sequential_delete(
                        docs_hash, index, es, args, dupl)

    def es_query(self, args):
        if args.timestamp:
            filter = {"format": "strict_date_optional_time"}
            if args.since:
                filter['gte'] = args.since
            if args.until:
                filter['lte'] = args.until
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
            # print(info)
            progress.update(1)

        self.log.info(
            "Deleted {:0,} documents (including shard replicas)".format(successes))

    def parallel_delete(self, docs_hash, index, es, args, duplicates):
        progress = tqdm.tqdm(unit="docs", total=duplicates)
        successes = 0

        for success, info in self.wrapper(parallel_bulk(es, self.delete_iterator(docs_hash, index, args),
                                                        thread_count=args.threads, request_timeout=args.request_timeout,
                                                        chunk_size=args.flush, raise_on_exception=args.fail_fast)):
            if success:
                successes += info['delete']['_shards']['successful']
            else:
                print('Doc failed', info)
            # print(info)
            progress.update(1)

        self.log.info(
            "Deleted {:0,} documents (including shard replicas)".format(successes))

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
