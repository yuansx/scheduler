#!/usr/bin/env python
# -*- coding: utf-8 -*-

from peewee import *
from src.db.peewee_mysql import MyRetryDB, MyBaseModel
from src import config



def create_database(key):
    db = config[config["base"]["region"]][key]
    return MyRetryDB(db["db"], **{
        "host": db["host"],
        "port": db["port"],
        "user": db["user"],
        "password": db["password"],
        "charset": "utf8",
        "sql_mode": "PIPES_AS_CONCAT",
        'use_unicode': True,
    })


database = create_database("db")
ro_database = create_database("ro_db")


class BaseModel(MyBaseModel):
    @classmethod
    def select(cls, *fields):
        cls._meta.database = ro_database
        ret = super(BaseModel, cls).select(cls, *fields)
        cls._meta.database = database
        return ret

    @classmethod
    def raw(cls, sql, *params):
        if sql.lower().startswith('select'):
            cls._meta.database = ro_database
        else:
            cls._meta.database = database
        return super(BaseModel, cls).raw(cls, sql, *params)

    class Meta:
        database = database


class DMasterTaskState(BaseModel):
    action = CharField(constraints=[SQL("DEFAULT ''")], index=True)
    add_time_stamp = DateTimeField(column_name='addTimeStamp', constraints=[SQL("DEFAULT CURRENT_TIMESTAMP")])
    ftime = BigIntegerField(constraints=[SQL("DEFAULT 0")])
    id = BigAutoField()
    mod_time_stamp = DateTimeField(column_name='modTimeStamp', constraints=[SQL("DEFAULT CURRENT_TIMESTAMP")])
    owner = CharField(constraints=[SQL("DEFAULT ''")])
    state = CharField(constraints=[SQL("DEFAULT 'init'")], index=True)

    class Meta:
        table_name = 'DMasterTaskState'
        indexes = (
            (('ftime', 'action'), True),
        )

