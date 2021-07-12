#!/usr/bin/env python
# -*- coding: utf-8 -*-
import threading
import traceback
import os
import time
from src.util.log import log_info
from src.mysql_master_worker import MysqlMaster


def test_process(*args, **kwargs):
    log_info('test in {0} process, {1}, {2}'.format(os.getenv('base_path'), os.getpid(), threading.currentThread().ident))


def test_thread(*args, **kwargs):
    try:
        log_info('test in {0} croroutine, {1}, {2}'.format(os.getenv('base_path'), os.getpid(), threading.currentThread().ident))
        MysqlMaster('test', 'm')#.process()
    except:
        log_info(traceback.format_exc())


if '__main__' == __name__:
    pass
