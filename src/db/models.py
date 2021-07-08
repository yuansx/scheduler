#!/usr/bin/env python
# -*- coding: utf-8 -*-

from peewee import *
from src.db.peewee_mysql import MyRetryDB, MyBaseModel

rw_database = MyRetryDB('test', **{'charset': 'utf8', 'sql_mode': 'PIPES_AS_CONCAT', 'use_unicode': True, 'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'password': 'Derek@12345'})


class BaseModel(MyBaseModel):
    @classmethod
    def select(cls, *fields):
        # cls._meta.database = ro_database
        ret = super(BaseModel, cls).select(cls, *fields)
        # cls._meta.database = rw_database
        return ret

    @classmethod
    def raw(cls, sql, *params):
        if sql.lower().startswith('select'):
            # cls._meta.database = ro_database
            pass
        else:
            # cls._meta.database = rw_database
            pass
        return super(BaseModel, cls).raw(cls, sql, *params)

    class Meta:
        database = rw_database


class CLbMaster(BaseModel):
    action = CharField(constraints=[SQL("DEFAULT ''")], index=True)
    add_time_stamp = DateTimeField(column_name='addTimeStamp', constraints=[SQL("DEFAULT CURRENT_TIMESTAMP")])
    ftime = BigIntegerField(constraints=[SQL("DEFAULT 0")])
    id = BigAutoField()
    mod_time_stamp = DateTimeField(column_name='modTimeStamp', constraints=[SQL("DEFAULT CURRENT_TIMESTAMP")])
    owner = CharField(constraints=[SQL("DEFAULT ''")])

    class Meta:
        table_name = 'cLBMaster'
        indexes = (
            (('ftime', 'action'), True),
        )

