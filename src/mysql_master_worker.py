#!/usr/bin/env python
# -*- coding: utf-8 -*-
from src.util.master_worker import DMaster
from src.db.models import DMasterTaskState
from random import randint



class MysqlMaster(DMaster):
    def action_exists(self, time_stamp: int) -> bool:
        where_cond = (DMasterTaskState.action == self.action) & (DMasterTaskState.ftime == time_stamp)
        cnt = DMasterTaskState.select().where(where_cond).count()
        return cnt != 0

    def scramble_for_master(self, time_stamp: int) -> bool:
        data = {
                    'action': self.action,
                    'ftime': time_stamp,
                }
        ret = DMasterTaskState.insert(data).execute()
        return ret

    def produce_list(self, time_stamp: int) -> list:
        return [randint(1, 100) for x in range(10)]

    def set_master_state(self, time_stamp: int, data, state: str):
        where_cond = (DMasterTaskState.action == self.action) & (DMasterTaskState.ftime == time_stamp)
        DMasterTaskState.update({'state': state}).where(where_cond).execute()

