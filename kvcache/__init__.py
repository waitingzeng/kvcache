try:
    from urllib.parse import parse_qsl
except ImportError:     # Python 2
    from urlparse import parse_qsl

from .backends.base import (
    InvalidCacheBackendError, CacheKeyWarning, BaseCache)
from .utils import importlib

__all__ = [
    'get_cache', 'cache', 'DEFAULT_CACHE_ALIAS'
]

# Name for use in settings file --> name of module in "backends" directory.
# Any backend scheme that is not in this dictionary is treated as a Python
# import path to a custom backend.
BACKENDS = {
    'memcached': 'memcached',
    'locmem': 'locmem',
    'file': 'filebased',
    'db': 'db',
    'dummy': 'dummy',
}

DEFAULT_CACHE_ALIAS = 'default'

def parse_backend_uri(backend_uri):
    """
    Converts the "backend_uri" into a cache scheme ('db', 'memcached', etc), a
    host and any extra params that are required for the backend. Returns a
    (scheme, host, params) tuple.
    """
    if backend_uri.find(':') == -1:
        raise InvalidCacheBackendError("Backend URI must start with scheme://")
    scheme, rest = backend_uri.split(':', 1)
    if not rest.startswith('//'):
        raise InvalidCacheBackendError("Backend URI must start with scheme://")

    host = rest[2:]
    qpos = rest.find('?')
    if qpos != -1:
        params = dict(parse_qsl(rest[qpos+1:]))
        host = rest[2:qpos]
    else:
        params = {}
    if host.endswith('/'):
        host = host[:-1]

    if host.find(':') != -1:
        host = host.split(':', 1)
        host[1] = int(host[1])

    return scheme, host, params

def get_cache(backend, **kwargs):
    """
    Function to load a cache backend dynamically. This is flexible by design
    to allow different use cases:

    To load a backend with the old URI-based notation::

        cache = get_cache('locmem://')

    To load a backend that is pre-defined in the settings::

        cache = get_cache('default')

    To load a backend with its dotted import path,
    including arbitrary options::

        cache = get_cache('django.core.cache.backends.memcached.MemcachedCache', **{
            'LOCATION': '127.0.0.1:11211', 'TIMEOUT': 30,
        })

    """
    try:
        # for backwards compatibility
        backend, location, params = parse_backend_uri(backend)
        if backend in BACKENDS:
            backend = 'kvcache.backends.%s' % BACKENDS[backend]
        params.update(kwargs)
        mod = importlib.import_module(backend)
        backend_cls = mod.CacheClass
    
    except (AttributeError, ImportError) as e:
        raise InvalidCacheBackendError(
            "Could not find backend '%s': %s" % (backend, e))
    cache = backend_cls(location, params)
    # Some caches -- python-memcached in particular -- need to do a cleanup at the
    # end of a request cycle. If the cache provides a close() method, wire it up
    # here.
    return cache

