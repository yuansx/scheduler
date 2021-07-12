#!/usr/bin/env python
# -*- coding: utf-8 -*-

import gevent.monkey
gevent.monkey.patch_all()


import os
cur_path = os.path.split(os.path.realpath(os.sys.argv[0]))[0]
os.environ['base_path'] = cur_path


from apscheduler.schedulers.gevent import GeventScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.executors.gevent import GeventExecutor
from apscheduler.events import *
from src.util.log import log_init, log_info, add_logger
from src.util.util import get_ip_address
from src import config
import logging
from pytz import utc
import copy
import signal


def handler_exit(sig_id, frame):
    os.environ['terminate'] = 'true'


class CMain(object):
    def __init__(self):
        self._scheduler = GeventScheduler()
        self._jobs = {}
        self._workers = {}

    @staticmethod
    def _init_log():
        log_config = config["common"].get("log", {})
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

    @staticmethod
    def _init_master_config():
        if_name = config['common'].get('master', {}).get('interface')
        if if_name:
            os.environ['address'] = get_ip_address(if_name)
        else:
            address = None
            for if_name in ('eth0', 'en0', 'eth1', ):
                try:
                    address = get_ip_address(if_name)
                except:
                    continue
                if address:
                    break
            if address:
                os.environ['address'] = address
            else:
                raise Exception('Not found address')
        port = config['common'].get('master', {}).get('port', 7070)
        os.environ['port'] = str(port)
        os.environ['auth_key'] = config['common'].get('auth_key', 'derek')

    def _init_scheduler(self):
        scheduler = config["common"].get("scheduler", {})
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

    def _load_jobs(self):
        jobs = config["common"].get("jobs", [])
        job_list = config["base"].get("job_list", [])
        for job in jobs:
            if {'id', 'func', 'trigger'} - set(job.keys()):
                continue
            if job_list and job['id'] not in job_list:
                continue
            self._add_job(job)

    def _import_class(func):
        parts = module.split(":", 1)
        if len(parts) == 1:
            module, obj = module, "run"
        else:
            module, obj = parts[0], parts[1]

        try:
            __import__(module)
        except ImportError:
            if module.endswith(".py") and os.path.exists(module):
                raise ImportError("Failed to find application, did "
                    "you mean '%s:%s'?" % (module.rsplit(".", 1)[0], obj))
            else:
                raise

        mod = sys.modules[module]

        try:
            app = eval(obj, mod.__dict__)
        except NameError:
            raise AppImportError("Failed to find application: %r" % module)

        if app is None:
            raise AppImportError("Failed to find application object: %r" % obj)

        if not callable(app):
            raise AppImportError("Application object must be callable.")
        return app


    def _init_worker(self):
        job_list = config["base"].get("job_list", [])
        for worker in config["common"].get("manager", {}).get("worker", [])
            if {'id', 'func'} - set(worker.keys()):
                continue
            if job_list and worker['id'] not in job_list:
                continue
            self._workers[name] = []

    def _init_signal():
        signal.signal(signal.SIGTERM, )

    def _init(self):
        self._init_worker()
        self._init_master_config()
        self._init_log()
        self._init_scheduler()
        self._load_jobs()

    def main(self):
        self._init()
        try:
            self._scheduler.start().join()
        except:
            self._scheduler.shutdown()


if __name__ == '__main__':
    CMain().main()

