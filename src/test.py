#!/usr/bin/env python
# -*- coding: utf-8 -*-
import threading
import traceback
import os
import time
from src.util.log import log_info
from src.mysql_master_worker import MysqlMaster, MysqlWorker


def test_process(*args, **kwargs):
    log_info('test in {0} process, {1}, {2}'.format(os.getenv('base_path'), os.getpid(), threading.currentThread().ident))


a = False

def test_thread(*args, **kwargs):
    try:
        global a
        if a:
            return
        a = True
        manager = kwargs['manager']
        log_info('test in {0} croroutine, manager: {1}'.format(os.getpid(), id(manager)))
        MysqlMaster(manager, kwargs['job_id'], 'm').process()
    except:
        log_info(traceback.format_exc())


class MyTest(MysqlWorker):
    def real_run(self, data):
        log_info('recv {0} in {1}'.format(data, os.getpid()))
        time.sleep(3)
        return True


if '__main__' == __name__:
    pass
