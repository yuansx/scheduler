#!/usr/bin/env python
# -*- coding: utf-8 -*-

class SchedulerMaster(object):
    def __init__(self, granularity='m'):
        if granularity not in ('m', 'h', 'd'):
            raise Exception('not support granularity {0}'.format(granularity))
        self._granularity = granularity

    def process(self):
        self._check_time()
