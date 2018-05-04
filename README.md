# ES-dedupe

A tool for removing duplicated documents that are grouped by some unique field (e.g. `--field Uuid`). Removal process consists of two phases:

 1. Aggregate query find documents that have same `field` value and at least 2 occurences. One copy of such document is left in ES all other are deleted via Bulk API (almost all, usually - there's always some catch). We wait for index update after each `DELETE` operatation. Processed documents are logged into `/tmp/es_dedupe.log`.
 2. Unfortunately aggregate queries are not necessarily exact. Based on `/tmp/es_dedupe.log` logfile we query for each `field` value and DELETE document copies on other shards. Depending on number of nodes and shards in cluster there might be still document that aggregate query didn't return. In order to disable 2nd step use `--no-check` flag.

## Docker

Running from Docker:
```
docker run -it -e ES=locahost -e INDEX=my-index -e FIELD=id deric/es-dedupe
```
You can either override Docker commad or use ENV variable to pass arguments.

## Usage
```
python -u dedupe.py -H localhost -P 9200 -i exact-index-name -f Uuid > es_dedupe.log
```
will try to find duplicated documents in an index called 'exact-index-name' where documents are grouped by `Uuid` field.

```
python -u dedupe.py -H localhost -P 9200 --all --prefix 'esindexprefix' --prefixseparator '-' --indexexclude '^excludedindex.*' -f fingerprint > es_dedupe.log
```
will try to find duplicated documents in all indices known to the ES instance on localhost:9200, that look akin to 'esindexprefix-\*' while excluding all indices starting with 'excludedindex', where documents are grouped by `fingerprint` field.

 * `-a` will process all indexes known to the ES instance that match the prefix and prefixseparator.
 * `-b` batch size - critical for performance ES queries might take several minutes, depending on size of your indexes
 * `-f` name of field that should be unique
 * `-h` displays help
 * `-m` number of duplicated documents with same unique field value
 * `-t` document type in ES
 * `--sleep 60` time between aggregation requests (gives ES time to run GC on heap), 15 seconds seems to be enough to avoid triggering ES flood protection though.

WARNING: Running huge bulk operations on ES cluster might influence performance of your cluster or even crash some nodes if heap
is not large enough. Increment `-b` and `-m` parameters with caution! ES returns at most `b * m` documents, eventually you might hit
maximum POST request size with bulk requests.

A log file containing documents with unique fields is written into `/tmp/es_dedupe.log`.

By design ES aggregate queries are not necessarily precise. Depending on your cluster setup, some documents won't be deleted due to
inaccurate shard statistics.

Running `$ python dedupe.py --check_log /tmp/es_dedupe.log --noop` will query for documents found by aggregate and queries check whether were actually
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
For the installation  use the tools provided by your operating system.

On Linux   this can be one of the following:  yum, dnf, apt, yast, emerge, ..
```
* Install python (2 or 3, both will work)
* Install python*ujson and python*requests for the fitting python version
```

On Windows you are pretty much on your own, but fear not, you can do the following ;-)
```
* Download and install a python version from https://www.python.org/ .
* Open a console terminal and head to the repository copy of es-deduplicator, then run:
pip install -r requirements.txt
```

## History

Originally written in bash which performed terribly due to slow JSON processing with pipes and `jq`. Python with `ujson` seems to be better fitted for this task.
