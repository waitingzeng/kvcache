"Memcached cache backend"

import time
from threading import local

from .base import BaseCache, InvalidCacheBackendError

from ..utils.encoding import force_str
import dbm
import os.path as osp


class BDBCache(BaseCache):

    def __init__(self, url, params, library, value_not_found_exception):
        super(BDBCache, self).__init__(params)
        self._path = url.path.lstrip('/')

    @property
    def _cache(self):
        """
        Implements transparent thread-safe access to a memcached client.
        """
        if not getattr(self, '_client', None):
            self._client = dbm.open(self._path, 'c')
        return self._client

    def make_key(self, key, version=None):
        # Python 2 memcache requires the key to be a byte string.
        return force_str(super(BDBCache, self).make_key(key, version))

    def add(self, key, value, timeout=0, version=None):
        key = self.make_key(key, version=version)
        timeout = timeout and now + timeout or 0
        data = self.encode(timeout, value)
        return self._cache.Set(key, self.encode(data))

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        data = self._cache.Get(key)
        exp, val = self.decode(data)
        now = time.time()
        if exp > 0 and now > exp:
            self._cache.Delete(key)
            return default
        return val

    def set(self, key, value, timeout=0, version=None):
        key = self.make_key(key, version=version)
        now = int(time.time())
        timeout = timeout and now + timeout or 0
        data = self.encode(timeout, value)
        self._cache.set(key, data)

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self._cache.delete(key)

    def get_many(self, keys, version=None):
        ret = {}
        for key in keys:
            ret[key] = self.get(key)
        return ret

    def close(self, **kwargs):
        self._cache.Close()

    def set_many(self, data, timeout=0, version=None):
        for key, value in data.items():
            self.set(key, value, timeout, version)

    def delete_many(self, keys, version=None):
        for k in keys:
            self.delete(k)


class CacheClass(BDBCache):
    pass
