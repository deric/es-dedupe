#!/usr/bin/env python3

# -*- coding: utf-8 -*-

import hashlib
import inspect
import os.path
import psutil
import re
import time
import tqdm
import ujson
from collections import deque

from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import parallel_bulk
from logging import getLogger


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
        for unit in ['','K','M','G','T','P','E','Z']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Y', suffix)

    def report_memusage(self):
        process = psutil.Process(os.getpid())
        rss = process.memory_info().rss / 1024 * 2
        self.log.info("Memory usage: {}".format(self.bytes_fmt(rss)))

    def run(self, args):
        global pp, idx2settings, indices, re_indexexclude

        start = time.time()
        total = 0

        if (args.index != ""):      args.all = False            # if indexname specifically was set, do not do --all mode


        es = Elasticsearch([args.host],
            port=args.port
            )

        resp = es.info()
        self.log.info("elastic: {}, version: {}".format(resp['cluster_name'],resp['version']['number']))

        docs_hash = {}

        # one or more fields to form a unique key
        unique_fields = args.field.split(',')
        self.log.info("Unique fields: {}".format(unique_fields))

        if args.index != "":
            index = args.index
            i = 0
            self.log.info("Building documents mapping on index: {}, batch size: {}".format(index, args.batch))
            for hit in helpers.scan(es, index=index, size=args.batch):
                self.build_index(docs_hash, unique_fields, hit)
                i += 1
                if args.verbose:
                    if (i % 1000000 == 0):
                        self.log.info("Scanned {} unique documents".format(len(docs_hash)))
                        self.report_memusage()
            dupl = self.count_duplicates(docs_hash)
            if dupl == 0:
                self.log.info("No duplicates found")
            else:
                total = len(docs_hash)
                self.log.info("Found {} duplicates out of {} docs, unique documents: {} ({}% duplicates)".format(dupl, dupl+total, total, dupl/(dupl+total)*100))

                if args.log_dupl:
                    self.save_documents_mapping(docs_hash, args)
                if args.noop:
                    if args.verbose:
                        self.print_duplicates(docs_hash, index, es, args)
                else:
                    self.delete_duplicates(docs_hash, index, es, args, dupl)


    def print_duplicates(self, docs_hash, index, es, args):
        for hashval, ids in docs_hash.items():
          if len(ids) > 1:
            # Get the documents that have mapped to the current hasval
            matching_docs = es.mget(index=index, body={"ids": ids})
            for doc in matching_docs['docs']:
                print("doc=%s\n" % doc)

    def delete_duplicates(self, docs_hash, index, es, args, duplicates):
        progress = tqdm.tqdm(unit="docs", total=duplicates)
        successes = 0

        for success, info in parallel_bulk(es, self.delete_iterator(docs_hash, index, args)):
            if success:
                successes += info['delete']['_shards']['successful']
            else:
                print('Doc failed', info)
            #print(info)
            progress.update(1)


        print("Deleted %d/%d documents" % (successes, duplicates))

    def delete_iterator(self, docs_hash, index, args):
        for hashval, ids in docs_hash.items():
          if len(ids) > 1:
            i = 0
            for doc_id in ids:
                if i > 0: # skip first document
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
        self.log.info("Found: {} dulpicates".format(duplicates))
        return duplicates

    def save_documents_mapping(self, docs_hash, args):
        self.log.info("Storing documents mapping into: {}".format(args.log_dupl))
        with open(args.log_dupl, "wb") as ujson_file:
            ujson.dump(docs_hash, ujson_file)