#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys


class Configure(dict):
    def merge(self, dct):
        self.update(dct)


configure = Configure()
sys.modules['config'] = configure
sys.modules['src.config'] = configure


