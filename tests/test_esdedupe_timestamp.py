import pytest
from elasticsearch import Elasticsearch
import random
import string
import time

import esdedupe
from logging import INFO
from esdedupe.cli import ArgumentParser
from esdedupe.cmd import setup_logging

INDEX = "test-timeseries"


def random_string(length):
    # Random string with the combination of lower and upper case
    letters = string.ascii_letters + string.digits
    return "".join(random.SystemRandom().choice(letters) for i in range(length))


@pytest.fixture()
def dedupe():
    print("setup")
    es = Elasticsearch()

    # ignore 400 cause by IndexAlreadyExistsException when creating an index
    es.indices.create(index=INDEX, ignore=400, wait_for_active_shards=1)
    print("Created index {}".format(INDEX))

    # fill with documents
    for i in range(5):
        es.create(
            index=INDEX,
            id=random_string(8),
            body={"timestamp": "2021-01-01T01:01:00.000Z", "name": "foo"},
        )
    for i in range(5):
        es.create(
            index=INDEX,
            id=random_string(8),
            body={"timestamp": "2021-01-01T01:05:00.000Z", "name": "bar"},
        )
    for i in range(5):
        es.create(
            index=INDEX,
            id=random_string(8),
            body={"timestamp": "2021-01-01T01:12:00.000Z", "name": "baz"},
        )
    for i in range(5):
        es.create(
            index=INDEX,
            id=random_string(8),
            body={"timestamp": "2021-01-01T01:13:00.000Z", "name": "boo"},
        )

    yield "dedupe"

    # cleanup
    es.indices.delete(index=INDEX, ignore=400)


def test_es_ping():
    es = Elasticsearch(["localhost"])
    assert es.ping()


class TestDedupe:
    def test_docs(self, dedupe):
        es = Elasticsearch()
        res = es.count(index=INDEX)
        # make sure elastic indexes inserted documents
        i = 0
        while res["count"] < 20:
            time.sleep(1)
            i += 1
            res = es.count(index=INDEX)
            if i > 3:
                assert False
        print("doc count: {}".format(res["count"]))

        dedupe = esdedupe.Esdedupe()
        parser = ArgumentParser()
        args = parser.parse_args(
            [
                "-i",
                INDEX,
                "-f",
                "name",
                "-T",
                "timestamp",
                "-w 5m",
                "-F 2021-01-01T01:01:00",
                "-U 2021-01-01T01:20:00",
            ]
        )
        setup_logging(args, INFO, INFO)
        dedupe.run(args)

        i = 0
        while res["count"] > 19:
            time.sleep(1)
            i += 1
            print(res["count"])
            res = es.count(index=INDEX)
            if i > 3:
                assert False

        assert es.count(index=INDEX)["count"] == 4
