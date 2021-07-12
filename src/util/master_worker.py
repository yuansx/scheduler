#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os
from multiprocessing.managers import BaseManager
from multiprocessing import Queue
import queue
from src.util.log import log_error
import traceback


task_queue = Queue()
result_queue = Queue()


class QueueManager(BaseManager):
    pass


class DMaster(object):

    def __init__(self, action: str, granularity: str = 'm'):
        self.action = action
        self._time_format = self._get_time_format(granularity)
        self.address = os.getenv('address', '127.0.0.1')
        self.port = int(os.getenv('port', '7070'))
        self.auth_key = os.getenv('auth_key', 'derek').encode()
        self.manager = None
        self._init_manager()
        self._task_list = list()

    def _init_manager(self):
        if not self.manager:
            QueueManager.register('get_task_queue', callable=lambda: task_queue)
            QueueManager.register('get_result_queue', callable=lambda: result_queue)
            self.manager = QueueManager(address=(self.address, self.port), authkey=self.auth_key)
            self.manager.start()

    def owner(self):
        return '{0}:{1}'.format(self.address, self.port)

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

    def get_current_time(self) -> int:
        return int(time.strftime(self._time_format))

    def action_exists(self, time_stamp: int) -> bool:
        raise NotImplementedError('action_exists must be implemented by SchedulerMaster subclasses')

    def scramble_for_master(self, time_stamp: int) -> bool:
        raise NotImplementedError('scramble_for_master must be implemented by SchedulerMaster subclasses')

    def produce_list(self, time_stamp: int) -> list:
        raise NotImplementedError('product_list must be implemented by SchedulerMaster subclasses')

    def set_master_state(self, time_stamp: int, data, state: str):
        raise NotImplementedError('set_master_state must be implemented by SchedulerMaster subclasses')

    def operate_error(self, operator, data, msg):
        pass

    def consume_finish(self) -> int:
        return self.get_current_time()

    def _task_queue_put(self, time_stamp):
        titme.sleep(4)
        task_queue = self.manager.get_task_queue()
        for one in self.product_list(time_stamp):
            try:
                task_queue.put(one)
            except queue.Full as e:
                log_error(traceback.format_exc())
                self.operate_error('put to full task_queue', one, e)
            except BaseException as e:
                log_error(traceback.format_exc())
                self.operate_error('put task_queue', one, e)
            self._task_list.append(one)

    def _result_queue_get(self):
        result_queue = self.manager.get_result_queue()
        while True:
            try:
                ret = result_queue.get(timeout=1)
                if ret:
                    self._task_list.remove(ret)
            except queue.Empty:
                break
            except ValueError as e:
                log_error(trace.format_exc())
                self.operate_error('remove member not exists from task list', ret, e)
            except BaseException as e:
                log_error(traceback.format_exc())
                self.operate_error('get result_queue', ret, e)
        time_stamp = self.consume_finish()
        if time_stamp:
            self.set_master_state(time_stamp, None, 'finish')

    def process(self):
        cur_time = self.get_current_time()
        if not self.action_exists(cur_time):
            data = self.scramble_for_master(cur_time)
            if data:
                self._task_queue_put(cur_time)
                self.set_master_state(cur_time, data, 'success')
        self._result_queue_get()

