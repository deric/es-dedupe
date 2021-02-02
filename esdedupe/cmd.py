#!/usr/bin/env python

# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import argparse
import os
import sys

from logging import DEBUG, INFO, WARN, Formatter, StreamHandler, \
    getLogger
from logging.handlers import SysLogHandler
from sys import stderr, stdout

import esdedupe

from . import __VERSION__
from .cli import ArgumentParser


def setup_logging(args):
    fmt = '%(asctime)s [%(thread)d] %(levelname)-5s %(name)s %(message)s'
    formatter = Formatter(fmt=fmt, datefmt='%Y-%m-%dT%H:%M:%S ')
    stream = stdout if args.log_stream_stdout else stderr
    handler = StreamHandler(stream=stream)
    handler.setFormatter(formatter)
    logger = getLogger()
    logger.addHandler(handler)

    if args.log_syslog:
        fmt = 'esdedupe[%(process)-5s:%(thread)d]: %(name)s ' \
            '%(levelname)-5s %(message)s'
        handler = SysLogHandler(address=args.syslog_device,
                                facility=args.syslog_facility)
        handler.setFormatter(Formatter(fmt=fmt))
        logger.addHandler(handler)

    logger.level = DEBUG if args.debug else INFO

    # elasticsearch scroll output is too verbose
    getLogger('elasticsearch').level = WARN


def main():
    parser = ArgumentParser(description="Elastic duplicates deleter",
                            add_help=True, prog='esdedupe')
    args = parser.parse_args(sys.argv[1:])

    if args.version:
        print("esdedupe {}".format(__VERSION__))
        os._exit(0)

    setup_logging(args)
    try:
        dedupe = Esdedupe()
        dedupe.run(args)
    except KeyboardInterrupt:
        print('Interrupted by Keyboard')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


if (__name__ == "__main__"):
    main()
