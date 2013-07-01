"Thread-safe in-memory cache backend."

import time

from .base import BaseCache, PickleException
from ..utils.synch import RWLock

# Global in-memory store of cache data. Keyed by name, to provide
# multiple named local memory caches.
_caches = {}
_expire_info = {}
_locks = {}

class LocMemCache(BaseCache):
    def __init__(self, name, params):
        BaseCache.__init__(self, params)
        global _caches, _expire_info, _locks
        self._cache = _caches.setdefault(name, {})
        self._expire_info = _expire_info.setdefault(name, {})
        self._lock = _locks.setdefault(name, RWLock())

    def add(self, key, value, timeout=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        with self._lock.writer():
            exp = self._expire_info.get(key)
            if exp is None or exp <= time.time():
                try:
                    pickled = self.encode(value)
                    self._set(key, pickled, timeout)
                    return True
                except PickleException:
                    pass
            return False

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        with self._lock.reader():
            exp = self._expire_info.get(key)
            if exp is None:
                return default
            elif exp > time.time():
                pickled = self._cache[key]
                return self.decode(pickled, default)
        with self._lock.writer():
            try:
                del self._cache[key]
                del self._expire_info[key]
            except KeyError:
                pass
            return default

    def _set(self, key, value, timeout=None):
        if self._max_entries and len(self._cache) >= self._max_entries:
            self._cull()
        if timeout is None:
            timeout = self.default_timeout
        self._cache[key] = value
        self._expire_info[key] = time.time() + timeout

    def set(self, key, value, timeout=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        with self._lock.writer():
            try:
                pickled = self.encode(value)
                self._set(key, pickled, timeout)
            except PickleException:
                pass

    def incr(self, key, delta=1, version=None):
        value = self.get(key, version=version)
        if value is None:
            raise ValueError("Key '%s' not found" % key)
        new_value = value + delta
        key = self.make_key(key, version=version)
        with self._lock.writer():
            try:
                pickled = self.encode(new_value)
                self._cache[key] = pickled
            except PickleException:
                pass
        return new_value

    def has_key(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        with self._lock.reader():
            exp = self._expire_info.get(key)
            if exp is None:
                return False
            elif exp > time.time():
                return True

        with self._lock.writer():
            try:
                del self._cache[key]
                del self._expire_info[key]
            except KeyError:
                pass
            return False

    def _cull(self):
        if self._cull_frequency == 0:
            self.clear()
        else:
            doomed = [k for (i, k) in enumerate(self._cache) if i % self._cull_frequency == 0]
            for k in doomed:
                self._delete(k)

    def _delete(self, key):
        try:
            del self._cache[key]
        except KeyError:
            pass
        try:
            del self._expire_info[key]
        except KeyError:
            pass

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        with self._lock.writer():
            self._delete(key)

    def clear(self):
        self._cache.clear()
        self._expire_info.clear()

# For backwards compatibility
class CacheClass(LocMemCache):
    pass
