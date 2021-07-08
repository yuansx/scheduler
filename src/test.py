#!/usr/bin/env python
# -*- coding: utf-8 -*-
import threading
import os
import time
from src.util.log import log_info


def test_process(*args, **kwargs):
    log_info('test in {0} process, {1}, {2}'.format(os.getenv('base_path'), os.getpid(), threading.currentThread().ident))
    time.sleep(10)


def test_thread(*args, **kwargs):
    log_info('test in {0} thread, {1}, {2}'.format(os.getenv('base_path'), os.getpid(), threading.currentThread().ident))
    time.sleep(10)


if '__main__' == __name__:
    pass
