# ES deduplicator

A tool for removing duplicated documents that are grouped by some unique field (e.g. `--field Uuid`).

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

## Performance

Most time is spent on ES queries, choose `--batch` size wisely!

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
