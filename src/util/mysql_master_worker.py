#!/usr/bin/env python
# -*- coding: utf-8 -*-
from src.util.master_worker import DMaster, DWorker
from src.db.models import DTaskMasterState


class MysqlMaster(DMaster):
    def action_exists(self, time_stamp: int) -> bool:
        where_cond = (DTaskMasterState.action == self.action) & (DTaskMasterState.ftime == time_stamp)
        cnt = DTaskMasterState.select().where(where_cond).count()
        return cnt != 0

    def scramble_for_master(self, time_stamp: int) -> bool:
        data = {
                    'action': self.action,
                    'ftime': time_stamp,
                    'owner': self.owner(),
                    'secret_key': self.secret_key(),
                }
        ret = DTaskMasterState.insert(data).execute()
        return ret

    def produce_list(self, time_stamp: int) -> list:
        return list(range(10))

    def set_master_state(self, time_stamp: int, data, state: str):
        where_cond = (DTaskMasterState.action == self.action) & (DTaskMasterState.ftime == time_stamp)
        DTaskMasterState.update({'state': state}).where(where_cond).execute()


class MysqlWorker(DWorker):
    def query_master(self):
        where_cond = (DTaskMasterState.action == self.action) & (DTaskMasterState.state == 'success')
        ret = DTaskMasterState.select().where(where_cond).order_by(DTaskMasterState.ftime.desc()).limit(1)
        ip, port, secret_key = None, None, None
        for one in ret:
            address = one.owner.split(':')
            ip = address[0]
            port = int(address[1])
            secret_key = one.secret_key
            break
        return ip, port, secret_key

