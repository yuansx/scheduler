#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
cur_path = os.path.split(os.path.realpath(os.sys.argv[0]))[0]
os.environ['base_path'] = cur_path


#from apscheduler.schedulers.gevent import GeventScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.executors.gevent import GeventExecutor
from apscheduler.events import *
from src.util.log import log_init, log_info, log_warning, log_error, add_logger
from src.util.util import get_ip_address, set_process_name
from src.util.manager import QueueManager
from multiprocessing import Queue
from src import config
import logging
from pytz import utc
import copy
import signal
import sys
import time


class CMain(object):
    def __init__(self):
        #self._scheduler = GeventScheduler()
        self._scheduler = BackgroundScheduler()
        self._jobs = {}
        self._workers = {}
        self._manager = None
        self._queues = {}
        self._terminate = False

    @staticmethod
    def _init_log():
        log_config = config["common"].get("log", {})
        log_level = log_config.get('level', 4)
        log_path = log_config.get('path', './log')
        prefix = log_config.get('prefix', 'scheduler')
        proj = log_config.get('project', 'scheduler')
        log_init(proj, log_path, prefix, log_level)
        add_logger('apscheduler', logging.WARNING)
        if 'project' in config['base']:
            proj = config['base']['project']
        set_process_name(proj)

    def _handler_event(self, event):
        pass
        # self._scheduler.print_jobs()

    def _init_manager(self):
        manager = config['common'].get('manager', {})
        if_name = manager.get('interface')
        if if_name:
            address = get_ip_address(if_name)
        else:
            address = None
            for if_name in ('eth0', 'en0', 'eth1', ):
                try:
                    address = get_ip_address(if_name)
                except:
                    continue
                if address:
                    break
        if not address:
            raise Exception('Not found address')
        os.environ['address'] = address
        port = manager.get('port', 7070)
        auth_key = manager.get('auth_key', 'derek').encode()
        self._manager = QueueManager(address=(address, port), authkey=auth_key)
        self._manager.start()

    def _init_scheduler(self):
        scheduler = config["common"].get("scheduler", {})
        executors = {
            'default': ThreadPoolExecutor(scheduler.get('thread_num', 20)),
            #'default': GeventExecutor(),
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
        default_kwargs = {
            'manager': self._manager,
            'job_id': one_job['id'],
        }
        if 'kwargs' not in one_job:
            one_job['kwargs'] = default_kwargs
        else:
            one_job['kwargs'].update(default_kwargs)
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

    def _import_class(self, worker_class):
        parts = worker_class.split(":", 1)
        if len(parts) == 1:
            raise ValueError('Invalid format of worker class')
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
            raise ImportError("Failed to find application: %r" % module)

        if app is None:
            raise ImportError("Failed to find application object: %r" % obj)

        if not callable(app):
            raise ImportError("Application object must be callable.")
        return app

    def _init_worker(self):
        job_list = config["base"].get("job_list", [])
        for worker in config["common"].get("manager", {}).get("worker", []):
            if job_list and worker['id'] not in job_list:
                continue
            name = worker['id']
            if name in self._workers:
                log_warning('repeated worker {0}'.format(name))
                continue
            num = worker.get('number', 1)
            self._workers[name] = {
                'name': name,
                'len': num,
                'processes': []
            }
            self._workers[name]['len'] += 1
            for n in range(num):
                p = self._import_class(worker['class'])(name, n + 1)
                p.start()
                self._workers[worker['id']]['processes'].append(p)

    def _init_queue(self):
        job_list = config["base"].get("job_list", [])
        for c_queue in config["common"].get("manager", {}).get("queue", []):
            if job_list and c_queue['id'] not in job_list:
                continue
            func = c_queue['func']
            self._queues[func] = Queue()
            QueueManager.register(func, callable=lambda name: self._queues[name])

    def _signal_terminate(self, sig_id, frame):
        log_warning('recv signal {0}'.format(sig_id))
        self._terminate = True

    def _signal_child(self, sig_id, frame):
        pass

    def _init_signal(self):
        signal.signal(signal.SIGTERM, self._signal_terminate)
        signal.signal(signal.SIGINT, self._signal_terminate)
        signal.signal(signal.SIGCHLD, self._signal_child)

    @staticmethod
    def _monkey_patch():
        import gevent.monkey
        gevent.monkey.patch_all()

    def _init(self):
        self._init_queue()
        self._init_manager()
        self._init_worker()
        self._init_log()
        self._init_signal()
        self._init_scheduler()
        self._load_jobs()
        #self._monkey_patch()

    def start(self):
        self._init()
        self._scheduler.start()
        self._main_loop()

    def _wait_worker(self):
        for name, worker in self._workers.items():
            for p in worker['processes']:
                if p.is_alive():
                    p.join()

    def _main_loop(self):
        while not self._terminate:
            time.sleep(1)
        self._scheduler.shutdown()
        self._wait_worker()
        self._manager.shutdown()


if __name__ == '__main__':
    CMain().start()

