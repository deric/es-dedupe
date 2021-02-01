#!/usr/bin/env python3

# -*- coding: utf-8 -*-

from argparse import ArgumentParser as _Base
from sys import stderr, stdout

class ArgumentParser(_Base):

    def __init__(self, *args, **kwargs):
        super(ArgumentParser, self).__init__(*args, **kwargs)

    def parse_args(self, args):
        self.add_argument("-a", "--all",
                            action="store_true", dest="all", default=True,
                            help="All indexes from given date till today")
        self.add_argument("-b", "--batch",
                            dest="batch", default=1000, type=int,
                            help="Batch size - how many documents are retrieved using one request")
        self.add_argument("-m", "--max_dupes",
                            dest="dupes", default=10, type=int,
                            help="Dupes size - how many duplicates per document are retrieved")
        self.add_argument("-H", "--host", dest="host",
                            default="localhost",
                            help="Elasticsearch hostname", metavar="host")
        self.add_argument("-f", "--field", dest="field",
                            default="Uuid",
                            help="Field in ES that is supposed to be unique", metavar="field")
        self.add_argument("--flush",
                            dest="flush", default=500, type=int,
                            help="Number records send in one bulk request")
        self.add_argument("-i", "--index", dest="index",
                            default="",
                            help="Elasticsearch full index name, implies NOT --all", metavar="index")
        self.add_argument("-I", "--indexexclude", dest="indexexclude",
                            default="",
                            help="Elasticsearch regular expression of index name that is to be excluded, only useful with --all", metavar="indexexclude-regexp")
        self.add_argument("-j", "--threads",
                            dest="threads", default=1, type=int,
                            help="Number of threads to execute delete queries, when 1 seqential delete will be used. Note parallel delete can easily overload cluster")
        self.add_argument("-p", "--prefix", dest="prefix",
                            default="*",
                            help="Elasticsearch index prefix", metavar="prefix")
        self.add_argument("-S", "--prefixseparator", dest="prefixseparator",
                            default="-",
                            help="Elasticsearch index prefix separator to use between prefix, idxname and *", metavar="prefixsep")
        self.add_argument("-P", "--port", dest="port",
                            default=9200, type=int,
                            help="Elasticsearch port", metavar="port")
        self.add_argument("-t", "--doc_type", dest="doc_type",
                            default=None,
                            help="ES document type")
        self.add_argument("--timestamp", dest="timestamp",
                            default=None,
                            help="Timestamp field")
        self.add_argument("--since", dest="since",
                            default=None,
                            help="Search from given timestamp")
        self.add_argument("--until", dest="until",
                            default=None,
                            help="Search until given timestamp")
        self.add_argument("-v", "--verbose",
                            action="store_true", dest="verbose",
                            default=False,
                            help="enable verbose logging")
        self.add_argument("-r", "--max_retries",
                            dest="max_retries", default=3, type=int,
                            help="Maximum retries for rejected bulk delete")
        self.add_argument("--initial_backoff",
                            dest="initial_backoff", default=2, type=int,
                            help="Number of seconds we should wait before the first retry. Any subsequent retries will be powers of initial_backoff * 2**retry_number")
        self.add_argument("-d", "--debug",
                            action="store_true", dest="debug",
                            default=False,
                            help="enable debugging")
        self.add_argument("--no-check",
                            action="store_true", dest="no_check",
                            default=False,
                            help="Disable check & remove if duplicities found after with standard search query")
        self.add_argument("--log_dupl", dest="log_dupl",
                            default=None,
                            help="File to store duplicates mapping in JSON format")
        self.add_argument("--log_done", dest="log_done",
                            default="es_dedupe.done",
                            help="Logfile containing all document IDs that remained in ES")
        self.add_argument("--check_log", dest="check",
                            help="Verify that documents has been deleted")
        self.add_argument("-n", "--noop",
                            action="store_true", dest="noop",
                            default=False,
                            help="Do not take any destructive action (only print delete queries)")
        self.add_argument('--log-stream-stdout', action='store_true',
                          default=False,
                          help='Log to stdout instead of stderr')
        _help = 'Send logging data to syslog in addition to stderr'
        self.add_argument('--log-syslog', action='store_true', default=False,
                          help=_help)
        self.add_argument('--syslog-device', default='/dev/log',
                          help='Syslog device')
        self.add_argument('--syslog-facility', default='local0',
                          help='Syslog facility')


        parser = super(ArgumentParser, self).parse_args(args)
        return parser