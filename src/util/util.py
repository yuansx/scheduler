#!/usr/bin/env python
# -*- coding: utf-8 -*-
import psutil
import socket
import setproctitle


def get_ip_address(if_name):
    for addr in psutil.net_if_addrs()[if_name]:
        if addr.family == socket.AF_INET:
            return addr.address


def set_process_name(name):
    setproctitle.setproctitle(name)


if '__main__' == __name__:
    print(get_ip_address('eth0'))
