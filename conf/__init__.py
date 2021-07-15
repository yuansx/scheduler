#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os
import toml
from src import config


class DLoadConfig(object):
    def __init__(self):
        base_path = os.getenv('base_path')
        self._path = os.path.join(base_path, 'conf')

    @staticmethod
    def validate_base_config():
        if 'region' not in config['base']:
            msg = 'Invalid format of base config, without region'
            print(msg)
            raise Exception(msg)

    @staticmethod
    def validate_queue(c_queue):
        if {'id', 'func', 'feature'} - set(c_queue.keys()):
            msg = 'Invalid format of queue {0}, without id, func or feature'.\
                    format(c_queue)
            print(msg)
            raise Exception(msg)

    @staticmethod
    def validate_worker(c_worker):
        if {'id', 'class'} - set(c_worker.keys()):
            msg = 'Invalid format of worker {0}, without id or class'.\
                    format(c_worker)
            print(msg)
            raise Exception(msg)

    @staticmethod
    def validate_job(c_job):
        if {'id', 'func', 'trigger'} - set(c_job.keys()):
            msg = 'Invalid format of job {0}, without id, func or trigger'.\
                    format(c_job)
            print(msg)
            raise Exception(msg)

    def validate_common_config(self):
        manager = config['common'].get('manager', {})
        for c_queue in manager.get('queue', []):
            self.validate_queue(c_queue)
        for c_worker in manager.get('worker', []):
            self.validate_worker(c_worker)
        for c_job in config['common'].get('jobs', []):
            self.validate_job(c_job)

    def load_regions(self):
        config['valid_regions'] = []
        for item in config['common'].get('global', {}).get('region', []):
            file_path = os.path.join(self._path, item['file'])
            try:
                config[item['key']] = toml.loads(open(file_path).read())
            except FileNotFoundError:
                print("{0} 地域配置文件 {1} 不存在".format(item["name"], item["file"]))
                continue
            except BaseException as e:
                print("{0} 地域配置文件 {1} 不合法, {2}".format(item["name"], file_path, e))
                continue
            if item['key'] in config['valid_regions']:
                raise Exception('Duplicate region {0}'.format(item['key']))
            config['valid_regions'].append(item['key'])
        if config['base']['region'] not in config['valid_regions']:
            msg = 'Not load region {0} config file'.format(config['base']['region'])
            print(msg)
            raise Exception(msg)

    def main(self):
        for name in ["common", "base"]:
            file_path = os.path.join(self._path, '{0}.toml'.format(name))
            config[name] = toml.loads(open(file_path).read())
        self.validate_base_config()
        self.validate_common_config()
        self.load_regions()
        config['valid_regions'] = [item['key'] for item in config['common']['global']['region']]


DLoadConfig().main()
