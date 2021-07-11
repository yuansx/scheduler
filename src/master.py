#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import os
from multiprocessing.managers import BaseManager
from multiprocessing import Queue


class QueueManager(BaseManager):
    pass


class SchedulerMaster(object):
    manager = None
    def __init__(self, action: str, granularity: str = 'm'):
        self.action = action
        self._time_format = self._get_time_format(granularity)
        self.address = os.getenv('address', '127.0.0.1')
        self.port = int(os.getenv('port', '7070'))
        self.auth_key = os.getenv('auth_key', 'derek').encode()
        self._init_manager()

    def _init_manager(self):
        if not self.manager:
            task_queue = Queue()
            result_queue = Queue()
            QueueManager.register('get_task_queue', callable=lambda: task_queue)
            QueueManager.register('get_result_queue', callable=lambda: result_queue)
            self.manager = QueueManager(address=(self.address, self.port), authkey=self.auth_key.encode())

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

    def process(self):
        cur_time = self.get_current_time()
        if self.action_exists(cur_time):
            return
        if not self.scramble_for_master(cur_time):
            return

