#!/usr/bin/env python

# -*- coding: utf-8 -*-

import psutil


def bytes_fmt(self, num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)


def memusage(self):
    process = psutil.Process(os.getpid())
    rss = process.memory_info().rss
    return bytes_fmt(rss)
