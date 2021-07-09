#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os
import toml


def load_config():
    from src import config
    base_path = os.getenv('base_path')
    conf_path = os.path.join(base_path, 'conf')
    conf_names = ["common", "base"]
    for file_name in conf_names:
        file_path = os.path.join(conf_path, "{0}.toml".format(file_name))
        config[file_name] = toml.loads(open(file_path).read())
    config["valid_regions"] = [item["key"] for item in config["common"]["global"]["region"]]

    for region in config["common"]["global"]["region"]:
        try:
            file_path = os.path.join(conf_path, region["file"])
            try:
                config[region["key"]] = toml.loads(open(file_path).read())
            except:
                print("{0} 地域配置文件 {1} 不合法".format(region["name"], file_path))
        except FileNotFoundError:
            print("{0} 地域配置文件 {1} 不存在".format(region["name"], region["file"]))


load_config()
