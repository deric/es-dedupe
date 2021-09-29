# ES-dedupe

[![](https://images.microbadger.com/badges/version/deric/es-dedupe.svg)](https://microbadger.com/images/deric/es-dedupe)
[![](https://images.microbadger.com/badges/image/deric/es-dedupe.svg)](https://microbadger.com/images/deric/es-dedupe)

A tool for removing duplicated documents that are grouped by some unique field (e.g. `--field Uuid`).

## Usage

Use `-h/--help` to see supported options:
```
docker run --rm deric/es-dedupe:latest esdedupe --help
```
Remove duplicates from index `exact-index-name` while searching for unique `Uuid` field:

```
docker run --rm deric/es-dedupe:latest esdedupe -H localhost -P 9200 -i exact-index-name -f Uuid > es_dedupe.log 2>&1
```


More advanced example with documents containing timestamps.

```bash
esdedupe -H localhost -f request_id -i nginx_access_logs-2021.01.29 -b 10000 --timestamp Timestamp --since "2021-01-29T15:30:00.000Z" --until "2021-01-29T16:30:00.000Z" --flush 1500 --request_timeout 180
2021-02-01T19:58:25  [139754520647488] INFO  esdedupe elastic: es01, host: localhost, version: 7.6.0
2021-02-01T19:58:25  [139754520647488] INFO  esdedupe Unique fields: ['request_id']
2021-02-01T19:58:25  [139754520647488] INFO  esdedupe Building documents mapping on index: nginx_access_logs-2021.01.29, batch size: 10000
2021-02-01T19:59:16  [139754520647488] INFO  esdedupe Scanned 987,892 unique documents
2021-02-01T19:59:16  [139754520647488] INFO  esdedupe Memory usage: 414.0MB
2021-02-01T20:00:03  [139754520647488] INFO  esdedupe Scanned 1,950,957 unique documents
2021-02-01T20:00:03  [139754520647488] INFO  esdedupe Memory usage: 695.0MB
2021-02-01T20:00:46  [139754520647488] INFO  esdedupe Scanned 2,861,671 unique documents
2021-02-01T20:00:46  [139754520647488] INFO  esdedupe Memory usage: 1007.3MB
2021-02-01T20:01:37  [139754520647488] INFO  esdedupe Scanned 3,579,286 unique documents
2021-02-01T20:01:37  [139754520647488] INFO  esdedupe Memory usage: 1.2GB
2021-02-01T20:02:16  [139754520647488] INFO  esdedupe Found 810,993 duplicates out of 4,833,500 docs, unique documents: 4,022,507 (16.8% duplicates)
100%█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 810001/810993 [7:39:44<00:26, 37.16docs/s]
2021-02-02T03:42:01  [139754520647488] INFO  esdedupe Deleted 1,621,986/810,993 documents
2021-02-02T03:42:01  [139754520647488] INFO  esdedupe Successfully completed duplicates removal. Took: 7:43:36.313482
```


WARNING: Running huge bulk operations on Elastic cluster might influence performance of your cluster or even crash some nodes if heap is not large enough.

A sliding window `-w / --window` could be used to prevent running out of memory on larger indexes (if you have a timestamp field):

```bash
$ esdedupe -H localhost -f request_id -i nginx_access_logs-2021.02.01 -b 10000 --timestamp Timestamp --since 2021-02-01T00:00:00 --until 2021-02-01T10:30:00 --flush 2500 --request_timeout 180 -w 10m --es-level WARN
2021-02-07T01:27:07  [140045012879168] INFO  esdedupe Found 1,544 duplicates out of 162,805 docs, unique documents: 161,261 (0.9% duplicates)
  0%|          | 1/1544 [00:17<7:25:23, 17.32s/docs]2021-02-07T01:27:25  [140045012879168] INFO  esdedupe Deleted 3,088 documents (including shard replicas)
2021-02-07T01:27:25  [140045012879168] INFO  esdedupe Using window 10m, from: 2021-02-01T09:30:00.000Z until: 2021-02-01T09:40:00.000Z
2021-02-07T01:27:25  [140045012879168] INFO  esdedupe Building documents mapping on index: nginx_access_logs-2021.02.01, batch size: 10000
100%|██████████| 1544/1544 [00:18<00:00, 83.11docs/s]
2021-02-07T01:27:33  [140045012879168] INFO  esdedupe Found 1,338 duplicates out of 162,882 docs, unique documents: 161,544 (0.8% duplicates)
  0%|          | 1/1338 [00:19<7:23:17, 19.89s/docs]2021-02-07T01:27:53  [140045012879168] INFO  esdedupe Deleted 2,676 documents (including shard replicas)
2021-02-07T01:27:53  [140045012879168] INFO  esdedupe Using window 10m, from: 2021-02-01T09:40:00.000Z until: 2021-02-01T09:50:00.000Z
2021-02-07T01:27:53  [140045012879168] INFO  esdedupe Building documents mapping on index: nginx_access_logs-2021.02.01, batch size: 10000
100%|██████████| 1338/1338 [00:20<00:00, 64.36docs/s]
2021-02-07T01:28:02  [140045012879168] INFO  esdedupe Found 1,321 duplicates out of 165,664 docs, unique documents: 164,343 (0.8% duplicates)
  0%|          | 1/1321 [00:13<4:56:58, 13.50s/docs]2021-02-07T01:28:15  [140045012879168] INFO  esdedupe Deleted 2,642 documents (including shard replicas)
2021-02-07T01:28:15  [140045012879168] INFO  esdedupe Using window 10m, from: 2021-02-01T09:50:00.000Z until: 2021-02-01T10:00:00.000Z
2021-02-07T01:28:15  [140045012879168] INFO  esdedupe Building documents mapping on index: nginx_access_logs-2021.02.01, batch size: 10000
100%|██████████| 1321/1321 [00:14<00:00, 88.39docs/s]
2021-02-07T01:28:25  [140045012879168] INFO  esdedupe Found 1,291 duplicates out of 168,842 docs, unique documents: 167,551 (0.8% duplicates)
  0%|          | 1/1291 [00:12<4:20:59, 12.14s/docs]2021-02-07T01:28:37  [140045012879168] INFO  esdedupe Deleted 2,582 documents (including shard replicas)
2021-02-07T01:28:37  [140045012879168] INFO  esdedupe Using window 10m, from: 2021-02-01T10:00:00.000Z until: 2021-02-01T10:10:00.000Z
2021-02-07T01:28:37  [140045012879168] INFO  esdedupe Building documents mapping on index: nginx_access_logs-2021.02.01, batch size: 10000
100%|██████████| 1291/1291 [00:15<00:00, 82.91docs/s]
2021-02-07T01:28:48  [140045012879168] INFO  esdedupe Found 1,371 duplicates out of 173,650 docs, unique documents: 172,279 (0.8% duplicates)
  0%|          | 1/1371 [00:18<7:07:57, 18.74s/docs]2021-02-07T01:29:07  [140045012879168] INFO  esdedupe Deleted 2,742 documents (including shard replicas)
2021-02-07T01:29:07  [140045012879168] INFO  esdedupe Using window 10m, from: 2021-02-01T10:10:00.000Z until: 2021-02-01T10:20:00.000Z
2021-02-07T01:29:07  [140045012879168] INFO  esdedupe Building documents mapping on index: nginx_access_logs-2021.02.01, batch size: 10000
100%|██████████| 1371/1371 [00:19<00:00, 68.59docs/s]
2021-02-07T01:29:16  [140045012879168] INFO  esdedupe Found 1,340 duplicates out of 183,592 docs, unique documents: 182,252 (0.7% duplicates)
  0%|          | 1/1340 [00:21<8:00:21, 21.52s/docs]2021-02-07T01:29:38  [140045012879168] INFO  esdedupe Deleted 2,680 documents (including shard replicas)
2021-02-07T01:29:38  [140045012879168] INFO  esdedupe Altogether 14115806 documents were removed (including doc replicas)
2021-02-07T01:29:38  [140045012879168] INFO  esdedupe Total time: 1 day, 10:15:43.528495

```

## Requirements
For the installation  use the tools provided by your operating system.

On Linux   this can be one of the following:  yum, dnf, apt, yast, emerge, ..

* Install python (2 or 3, both will work)
* Install python*ujson and python*requests for the fitting python version


On Windows you are pretty much on your own, but fear not, you can do the following ;-)

* Download and install a python version from https://www.python.org/ .
* Open a console terminal and head to the repository copy of es-deduplicator, then run:
pip install -r requirements.txt


## Testing

Test can be run from a Docker container. You can use supplied `docker-compose` file:
```bash
docker-compose up
```

Manually run tests:
```bash
pip3 install -r requirements-dev.txt
python3 -m pytest -v --capture=no tests/
```


## History

Originally written in bash which performed terribly due to slow JSON processing with pipes and `jq`. Python with `ujson` seems to be better fitted for this task.
