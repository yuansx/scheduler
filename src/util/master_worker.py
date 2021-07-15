#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os
from multiprocessing.managers import BaseManager
from multiprocessing import Queue
from multiprocessing import Process
import queue
from src.util.log import log_init, log_error, log_warning, log_debug
from src.util.manager import QueueManager
from src import config
from src.util.util import set_process_name
import traceback
import signal


class DManagerQueue(object):
    def __init__(self):
        self._task_queue = None
        self._result_queue = None

    def init_queue(self, manager, action: str):
        if not manager:
            return
        for c_queue in config['common'].get('manager', {}).get('queue', []):
            if c_queue['id'] != action:
                continue
            feature = c_queue['feature']
            func = c_queue['func']
            if feature == 'task_queue' and hasattr(manager, func):
                self._task_queue = getattr(manager, func)(func)
            if feature == 'result_queue' and hasattr(manager, func):
                self._result_queue = getattr(manager, func)(func)

    @staticmethod
    def register(action):
        for c_queue in config['common'].get('manager', {}).get('queue', []):
            if c_queue['id'] != action:
                continue
            func = c_queue['func']
            QueueManager.register(func)

    def get_task_queue(self):
        return self._task_queue

    def get_result_queue(self):
        return self._result_queue

    @staticmethod
    def get_current_time(time_format: str) -> int:
        return int(time.strftime(time_format))


class DMaster(DManagerQueue):

    def __init__(self, manager, action: str, granularity: str = 'm', timeout: str = 1):
        DManagerQueue.__init__(self)
        DManagerQueue.init_queue(self, manager, action)
        self.action = action
        self._timeout = timeout
        self._time_format = self._get_time_format(granularity)
        self._manager = manager

    def owner(self):
        return '{0}:{1}'.format(self._manager.ip(), self._manager.port())

    def _str(self):
        return '{0}_{1} {2}'.format(self.__class__.__name__, self.action, self.owner())

    def __repr__(self):
        return self._str()

    def __str__(self):
        return self._str()

    def secret_key(self):
        return self._manager.secret_key()

    @staticmethod
    def _get_time_format(granularity: str) -> str:
        if not isinstance(granularity, str):
            raise TypeError('granularity is not a string')
        granularity = granularity.lower()
        if granularity == 'm':
            time_format = '%Y%m%d%H%M'
        elif granularity == 'h':
            time_format = '%Y%m%d%H'
        elif granularity == 'd':
            time_format = '%Y%m%d'
        else:
            raise ValueError('granularity is {0}, only support m, h, d'.format(granularity))
        return time_format

    def action_exists(self, time_stamp: int) -> bool:
        raise NotImplementedError('action_exists must be implemented by SchedulerMaster subclasses')

    def scramble_for_master(self, time_stamp: int) -> bool:
        raise NotImplementedError('scramble_for_master must be implemented by SchedulerMaster subclasses')

    def produce_list(self, time_stamp: int) -> list:
        raise NotImplementedError('produce_list must be implemented by SchedulerMaster subclasses')

    def set_master_state(self, time_stamp: int, data, state: str):
        raise NotImplementedError('set_master_state must be implemented by SchedulerMaster subclasses')

    def operate_error(self, operator, data, msg):
        pass

    def consume_finish(self, time_stamp) -> list:
        return [time_stamp]

    def task_queue_put(self, time_stamp: int):
        task_queue = self.get_task_queue()
        if not task_queue:
            return
        for one in self.produce_list(time_stamp):
            try:
                task_queue.put(one)
                log_debug('put {0} to {1} task queue'.format(one, self.action))
            except queue.Full as e:
                log_error(traceback.format_exc())
                self.operate_error('put to full task_queue', one, e)
            except BaseException as e:
                log_error(traceback.format_exc())
                self.operate_error('put task_queue', one, e)

    def worker_fail(self, data):
        pass

    def do_result(self, time_stamp):
        result_queue = self.get_result_queue()
        if not result_queue:
            return
        while True:
            try:
                ret = result_queue.get(timeout=self._timeout)
                log_debug('get {0} from {1} result queue'.format(ret, self.action))
            except queue.Empty:
                break
            try:
                if ret:
                    if ret['code'] != 0:
                        self.worker_fail(ret['data'])
                        self.operate_error('worker fail', ret['data'], 'worker fail')
            except ValueError as e:
                log_error(traceback.format_exc())
                self.operate_error('{0} is not exists in record list'.format(ret), ret, e)
            except Exception as e:
                pass
        time_list = self.consume_finish(time_stamp)
        if time_list:
            self.set_master_state(time_list, None, 'finish')

    def process(self):
        cur_time = self.get_current_time(self._time_format)
        if not self.action_exists(cur_time):
            data = self.scramble_for_master(cur_time)
            if data:
                self.task_queue_put(cur_time)
                self.set_master_state(cur_time, data, 'success')
        self.do_result(cur_time)


class DWorker(Process, DManagerQueue):
    def __init__(self, action: str, idx: int, wait_time: int = 3, timeout: int = 3):
        DManagerQueue.__init__(self)
        DManagerQueue.register(action)
        self._terminate = False
        self.action = action
        self._idx = idx
        self._timeout = timeout
        self._wait_time = wait_time
        Process.__init__(self, name=self._worker_name())
        self._init_log()
        self._init_signal()
        self._manager = None

    def _worker_name(self):
        return '{0}_worker{1}'.format(self.action, self._idx)

    def _str(self):
        return '{0}: {1}'.format(self.__class__.__name__, self._worker_name())

    def __repr__(self):
        return self._str()

    def __str__(self):
        return self._str()

    def _init_log(self):
        log_config = config["common"].get("log", {})
        log_level = log_config.get('level', 4)
        log_path = log_config.get('path', './log')
        proj = self._worker_name()
        log_init(self._name, log_path, proj, log_level)
        set_process_name(proj)

    def _signal_terminate(self, sig_id, frame):
        log_warning('recv signal {0}'.format(sig_id))
        self._terminate = True

    def _init_signal(self):
        signal.signal(signal.SIGTERM, self._signal_terminate)
        signal.signal(signal.SIGINT, self._signal_terminate)

    def query_master(self):
        raise NotImplementedError('set_master_state must be implemented by SchedulerMaster subclasses')
    
    def real_run(self, data) -> bool:
        return True

    def _run(self):
        ip, port, secret_key = self.query_master()
        if not (ip and port and secret_key):
            return
        self._manager = QueueManager(address=(ip, port, ), authkey=secret_key.encode())
        self._manager.connect()
        self.init_queue(self._manager, self.action)
        try:
            task_queue = self.get_task_queue()
            result_queue = self.get_result_queue()
        except BaseException as e:
            log_warning(e)
        if not task_queue:
            return
        while True:
            try:
                data = task_queue.get(timeout=self._timeout)
                log_debug('get {0} from {1} task queue'.format(data, self.action))
            except queue.Empty:
                break
            ret = self.real_run(data)
            if not result_queue:
                continue
            data = {'code': 0 if ret else -1, 'data': data}
            result_queue.put(data)
            log_debug('put {0} from {1} task queue'.format(data, self.action))

    def terminate(self):
        pass

    def run(self):
        while not self._terminate:
            self._run()
            time.sleep(self._wait_time)
        self.terminate()

