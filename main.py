#!/usr/bin/env python
# -*- coding: utf-8 -*-

import gevent.monkey
gevent.monkey.patch_all()


from apscheduler.schedulers.gevent import GeventScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.executors.gevent import GeventExecutor
from apscheduler.events import *
from src.util.log import log_init, log_info, add_logger
import logging
from pytz import utc
import toml
import os
import copy
try:
    from conf.config import job_list
except:
    job_list = None


class CMain(object):
    def __init__(self):
        self._scheduler = GeventScheduler()
        self._jobs = {}

    @staticmethod
    def _parse_config():
        cur_path = os.path.split(os.path.realpath(os.sys.argv[0]))[0]
        os.environ['base_path'] = cur_path
        base_config_file = os.path.join(cur_path, 'conf/config.toml')
        return toml.loads(open(base_config_file).read())

    @staticmethod
    def _init_log(log_config):
        log_level = log_config.get('level', 3)
        log_path = log_config.get('path', './log')
        prefix = log_config.get('prefix', 'scheduler')
        os.environ.update({
            'log_level': str(log_level),
            'log_path': log_path,
            'log_prefix': prefix,
            'main_pid': str(os.getpid()),
        })
        log_init(prefix, log_path, prefix, log_level)
        add_logger('apscheduler', logging.WARNING)

    def _handler_event(self, event):
        pass
        # self._scheduler.print_jobs()

    def _init_scheduler(self, scheduler):
        executors = {
            'default': GeventExecutor(),
            'processpool': ProcessPoolExecutor(scheduler.get('process_num', 5)),
        }
        job_defaults = {
            'coalesce': scheduler.get('coalesce', False),
            'max_instances': scheduler.get('max_instances', 1),
        }
        self._scheduler.configure(executors=executors, job_defaults=job_defaults, timezone=utc)
        self._scheduler.add_listener(self._handler_event)

    def _add_job(self, job):
        one_job = copy.copy(job)
        func = one_job.pop('func')
        self._jobs[job['id']] = self._scheduler.add_job(func, **one_job)

    def _load_jobs(self, jobs):
        for job in jobs:
            if {'id', 'func', 'trigger'} - set(job.keys()):
                continue
            if job_list is not None and job['id'] not in job_list:
                continue
            self._add_job(job)

    def _init(self):
        base_config = self._parse_config()
        self._init_log(base_config.get('log', {}))
        self._init_scheduler(base_config.get('scheduler', {}))
        self._load_jobs(base_config.get('jobs', []))

    def main(self):
        self._init()
        try:
            log_info('scheduler init success')
            self._scheduler.start().join()
        except:
            self._scheduler.shutdown()


if __name__ == '__main__':
    CMain().main()

