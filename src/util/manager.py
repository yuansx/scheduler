import os
from multiprocessing.managers import BaseManager


class QueueManager(BaseManager):
    def __init__(self, address=None, authkey=None, serializer='pickle', ctx=None):
        BaseManager.__init__(self, address, authkey, serializer, ctx)
        self._ip, self._port = address
        self._secret_key = authkey
        if not self._ip:
            self._ip = os.getenv('address', '127.0.0.1')

    def ip(self):
        return self._ip

    def port(self):
        return self._port

    def secret_key(self):
        return self._secret_key

