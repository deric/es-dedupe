import pytest
from pytest import mark
from pytest import raises
from elasticsearch import Elasticsearch, helpers
import random
import string
import time

import esdedupe
from esdedupe.cli import ArgumentParser

INDEX = 'test-parallel'

def random_string(length):
    # Random string with the combination of lower and upper case
    letters = string.ascii_letters + string.digits
    return ''.join(random.SystemRandom().choice(letters) for i in range(length))


@pytest.fixture()
def dedupe():
    print("setup")
    es = Elasticsearch()

    # ignore 400 cause by IndexAlreadyExistsException when creating an index
    es.indices.create(index=INDEX, ignore=400, wait_for_active_shards=1)
    print("Created index {}".format(INDEX))

    # fill with documents
    for i in range(50):
        es.create(index=INDEX, id=random_string(8), body={"name": "foo"})
    for i in range(50):
        es.create(index=INDEX, id=random_string(8), body={"name": "bar"})

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
        while res['count'] < 100:
            time.sleep(1)
            i += 1
            res = es.count(index=INDEX)
            if i > 3:
                assert False

        dedupe = esdedupe.Esdedupe()
        parser = ArgumentParser()
        dedupe.run(parser.parse_args(['-i', INDEX, '--field', 'name', '--log-stream-stdout', '-j 4',
            '--no-progress']))

        i = 0
        while res['count'] == 100:
            time.sleep(1)
            i += 1
            res = es.count(index=INDEX)
            if i > 3:
                assert False

        assert es.count(index=INDEX)['count'] == 2
