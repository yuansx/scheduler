#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.util.log import add_logger
import peewee
from peewee import *


add_logger('peewee')


class RetryOperationalError(object):
    def execute_sql(self, sql, params=None, commit=True):
        try:
            cursor = super(RetryOperationalError, self).execute_sql(
                sql, params, commit)
        except OperationalError:
            if not self.is_closed():
                self.close()
            with peewee.__exception_wrapper__:
                cursor = self.cursor()
                cursor.execute(sql, params or ())
                if commit and not self.in_transaction():
                    self.commit()
        return cursor


class MyRetryDB(RetryOperationalError, MySQLDatabase):
    pass


class MyBaseModel(Model):
    def __str__(self):
        return str(self.des_model_to_dict(raw_name=True))

    def __repr__(self):
        return self.__str__()

    @classmethod
    def from_dict(cls, d):
        data = {}
        for field in cls._meta.sorted_fields:
            raw_name = field.db_column
            if raw_name in d:
                data[field.name] = d[raw_name]
        return cls(**data)

    def dict(self, raw_name=False):
        return self.des_model_to_dict(raw_name)

    def not_none_dict(self, raw_name=False):
        return filter(lambda x: x[1], self.dict(raw_name).items())

    def dirty_dict(self):
        dirty = {}
        d = self.dict(raw_name=False)
        for k, v in d.items():
            if k in self._dirty:
                dirty[k] = v
        return dirty

    def des_model_to_dict(self, raw_name=False):
        data = {}
        for field in self._meta.sorted_fields:
            field_data = self.__data__.get(field.name)
            if raw_name:
                data[field.db_column] = field_data
            else:
                data[field.name] = field_data
        return data


