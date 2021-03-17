#!/usr/bin/env python

# -*- coding: utf-8 -*-

import os.path
import psutil
import datetime


def bytes_fmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


def memusage():
    process = psutil.Process(os.getpid())
    rss = process.memory_info().rss
    return bytes_fmt(rss)


SEC_PER_UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

# convert simple time representation to seconds, e.g. 5m, 1h
def time_to_sec(s):
    return int(s[:-1]) * SEC_PER_UNIT[s[-1]]

    # format datetime into Elastic's strict_date_optional_time
def to_es_date(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")