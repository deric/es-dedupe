# ES deduplicator

A tool for removing duplicated documents that are grouped by some unique field (e.g. `--field Uuid`). Removal process consists of two phases:

 1. Aggregate query find documents that have same `field` value and at least 2 occurences. One copy of such document is left in ES all other are deleted via Bulk API (almost all, usually - there's always some catch). We wait for index update after each `DELETE` operatation. Processed documents are logged into `/tmp/es_dedupe.log`.
 2. Unfortunately aggregate queries are not necessarily exact. Based on `/tmp/es_dedupe.log` logfile we query for each `field` value and DELETE document copies on other shards. Depending on number of nodes and shards in cluster there might be still document that aggregate query didn't return. In order to disable 2nd step use `--no-chck` flag.

Usage:
```
python3 -u dedupe.py -H localhost -i 2017.03.17 -b 10000 -m 100 -a -f Uuid --prefix nginx_logs > es_dedupe.log
```
will try to find duplicated documents in index called `nginx_logs-2017.03.17` where documents are grouped by `Uuid` field.

 * `-a` will process all indexes named with patterh `%Y.%m.%d` until today.
 * `-b` batch size - critical for performance ES queries might take several minutes, depending on size of your indexes
 * `-f` name of field that should be unique
 * `-h` displays help
 * `-m` number of duplicated documents with same unique field value
 * `-t` document type in ES
 * `--sleep 60` time between aggregation requests (gives ES time to run GC on heap)

WARNING: Running huge bulk operations on ES cluster might influence performance of your cluster or even crash some nodes if heap
is not large enough. Increment `-b` and `-m` parameters with caution! ES returns at most `b * m` documents, eventually you might hit
maximum POST request size with bulk requests.

A log file containing documents with unique fields is written into `/tmp/es_dedupe.log`.

By design ES aggregate queries are not necessarily precise. Depending on your cluster setup, some documents won't be deleted due to
inaccurate shard statistics.

Running `$ python3 dedupe.py --check_log /tmp/es_dedupe.log --noop` will query for documents found by aggregate and queries check whether were actually
deleted.
```
== Starting ES deduplicator....
PRETENDING to delete:
{"delete":{"_index":"nginx_access_logs-2017.03.17","_type":"nginx.access","_id":"AVrdoYEJy1wo8jcgI7t5"}}

== Total. OK: 4 (80.00%) out of 5. Fixable: 1. Missing: 0
Queried for 5 documents, retrieved status of 5 (100.00%).
```

## Performance

Most time is spent on ES queries, choose `--batch` and `--max_dupes` size wisely! Between each bulk request script sleeps for `--sleep {seconds}`.

Delete queries are send via `_bulk` API. Processing batch with several thousand documents takes several seconds:
```
== Starting ES deduplicator....
Using index nginx_access_logs-2017.03.17
ES query took 0:00:27.552083, retrieved 0 unique docs
Using index nginx_access_logs-2017.03.18
ES query took 0:01:30.705209, retrieved 10000 unique docs
Deleted 333129 duplicates, in total 333129. Batch processed in 0:00:09.999008, running time 0:02:08.259759
ES query took 0:01:58.288188, retrieved 10000 unique docs
Deleted 276673 duplicates, in total 609802. Batch processed in 0:00:08.487847, running time 0:04:16.037037
```

## Requirements
```
apt install python3-dev
pip3 install -r requirements.txt
```

## History

Originaly written in bash which performed terribly due to slow JSON processing with pipes and `jq`. Python with `ujson` seems to be better fitted for this task.
