#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import sys
import time
import threading
import traceback


class CLog(object):
    LEVEL_MAP = {
            0: logging.CRITICAL,
            1: logging.ERROR,
            2: logging.WARNING,
            3: logging.INFO,
            4: logging.DEBUG
            }

    @staticmethod
    def instance():
        global inst
        try:
            inst
        except BaseException as e:
            inst = CLog()
        return inst

    @staticmethod
    def get_real_msg_by_frame(msg, f):
        if isinstance(msg, str) and len(msg) > 2 and msg[0] in ('[',) and msg[-1] in ('>', ):
            return msg
        else:
            return '[%s] [%s] [%s] <%s>' % (os.path.basename(f.f_code.co_filename), f.f_code.co_name, f.f_lineno, msg)

    @staticmethod
    def get_real_msg(msg, len=10240):
        """ 根据调用关系取得日志产生所有文件名、函数名和行号，由此生成日志消息前缀 """
        f = sys._getframe().f_back.f_back.f_back
        if f is None:
            f = sys._getframe().f_back.f_back
        ret = CLog.get_real_msg_by_frame(msg, f)
        if isinstance(len, int):
            return ret[:len]
        else:
            return ret

    def __init__(self):
        self._proj = ''
        self._log_dir = None
        self._log_prefix = None
        self._log_level = 4
        self._last_log_name = None
        self._logger = None
        self._stream_handle = None
        self._b_use_stream = True
        self._last_file_handle = None
        self._lock = threading.Lock()
        self._format_str = '[%(asctime)s] [%(levelname)s] [%(process)d] [%(thread)d] %(message)s'

    def _init(self, proj, log_dir, log_prefix, log_level):
        self._proj = proj
        if log_dir[-1] != '/':
            self._log_dir = log_dir + '/'
        else:
            self._log_dir = log_dir
        self._log_prefix = log_prefix
        if log_level < 0:
            log_level = 0
        elif log_level > 4:
            log_level = 4
        self._log_level = log_level
        # 如果目录不存在创建目录
        if not os.access(self._log_dir, os.F_OK):
            try:
                os.makedirs(self._log_dir)
            except OSError as e:
                return -1

    def init(self, proj, log_dir, log_prefix, log_level):
        self._init(proj, log_dir, log_prefix, log_level)
        return self.init_logger()

    # 检查日志名，是否需要重新载入日志文件,为True说明不需要重新载入，False说明需要重新载入
    def check_log_name(self):
        if self._log_dir is None or self._log_prefix is None:
            return 0

        def _b_need_change():
            return self._last_log_name != log_name or not os.path.exists(log_name)

        def _change_log_name():
            with self._lock:
                if not _b_need_change():
                    return
                log_file_handler = logging.FileHandler(log_name)
                log_file_handler.setFormatter(logging.Formatter(self._format_str))
                log_file_handler.setLevel(CLog.LEVEL_MAP[self._log_level])
                self._logger.addHandler(log_file_handler)
                if self._last_file_handle is not None:
                    self._logger.removeHandler(self._last_file_handle)
                    self._last_file_handle.close()
                self._last_file_handle, self._last_log_name = log_file_handler, log_name

        log_name = os.path.join(self._log_dir, '%s.log.%s' % (self._log_prefix, time.strftime('%Y-%m-%d_%H')))
        if not _b_need_change():
            return 0
        try:
            _change_log_name()
        except Exception as e:
            return -1
        else:
            return 0

    def init_logger(self):
        if not self._proj:
            main_pid = int(os.environ.get('main_pid', '0'))
            pid = os.getpid()
            if main_pid != pid:
                raise Exception('xxxxxxxxxxxxxxxxxxxx {0}  {1}'.format(main_pid, pid))
                prefix = '{0}.{1}'.format(os.environ.get('log_prefix'), pid)
                self._init(prefix, os.environ.get('log_path'), prefix, int(os.environ.get('log_level')))
        if self._logger is None:
            self._logger = logging.getLogger(self._proj)
            self._logger.setLevel(logging.DEBUG)
        if 0 != self.create_stream_handle():
            return -1
        return self.check_log_name()

    def create_stream_handle(self):
        if not self._b_use_stream or self._stream_handle is not None:
            return 0
        try:
            stream_handler = logging.StreamHandler(sys.stderr)
            stream_handler.setFormatter(logging.Formatter(self._format_str))
            stream_handler.setLevel(logging.DEBUG)
            self._logger.addHandler(stream_handler)
            self._stream_handle = stream_handler
        except Exception as e:
            return -1
        else:
            return 0

    def remove_stream_handle(self):
        if self._stream_handle is not None:
            self._logger.removeHandler(self._stream_handle)
            self._stream_handle = None
        self._b_use_stream = False

    def log_debug(self, msg):
        if self._log_level < 4:
            return
        self.init_logger()
        self._logger.debug(CLog.get_real_msg(msg))

    def log_info(self, msg):
        if self._log_level < 3:
            return
        self.init_logger()
        self._logger.info(CLog.get_real_msg(msg))

    def log_warning(self, msg):
        if self._log_level < 2:
            return
        self.init_logger()
        self._logger.warning(CLog.get_real_msg(msg))

    def log_error(self, msg):
        if self._log_level < 1:
            return
        self.init_logger()
        self._logger.error(CLog.get_real_msg(msg))

    def log_critical(self, msg):
        if self._log_level < 0:
            return
        self.init_logger()
        self._logger.critical(CLog.get_real_msg(msg))


# 创建一个全局日志
# 不在屏幕打印信息
def log_no_stderr():
    CLog.instance().remove_stream_handle()


def log_init(proj, log_dir, log_prefix, log_level=4):
    return CLog.instance().init(proj, log_dir, log_prefix, log_level)


def log_debug(msg):
    CLog.instance().log_debug(msg)


def log_info(msg):
    CLog.instance().log_info(msg)


def log_warning(msg):
    if sys.exc_info()[0] is not None:
        CLog.instance().log_warning(traceback.format_exc())
        sys.exc_clear()
    CLog.instance().log_warning(msg)


def log_error(msg):
    if sys.exc_info()[0] is not None:
        CLog.instance().log_error(traceback.format_exc())
        sys.exc_clear()
    CLog.instance().log_error(msg)


def log_critical(msg):
    if sys.exc_info()[0] is not None:
        CLog.instance().log_critical(traceback.format_exc())
        sys.exc_clear()
    CLog.instance().log_critical(msg)


class LogStream(logging.Handler):

    def emit(self, record):
        try:
            log_info(record)
            self.flush()
        except RecursionError:
            raise
        except Exception:
            self.handleError(record)


def add_logger(name, level=None):
    level = level or CLog.LEVEL_MAP[int(os.environ.get('log_level', '4'))]
    log_info(level)
    _logger = logging.getLogger(name)
    _logger.setLevel(level)
    _logger.propagate = False
    _logger.addHandler(LogStream())
    return _logger


if __name__ == '__main__':
    import time
    log_init('test', '.', 'test', 5)
    log_debug('你好')
    a = add_logger('test_a')
    a.debug('aa')
