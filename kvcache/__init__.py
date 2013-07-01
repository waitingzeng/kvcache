import urlparse
from .backends.base import (
    InvalidCacheBackendError, CacheKeyWarning, BaseCache)
from .utils import importlib

__all__ = [
    'get_cache'
]

# Name for use in settings file --> name of module in "backends" directory.
# Any backend scheme that is not in this dictionary is treated as a Python
# import path to a custom backend.
BACKENDS = {
    'memcached': 'memcached',
    'locmem': 'locmem',
    'file': 'filebased',
    'mysql': 'mysql',
    'dummy': 'dummy',
    'sqlite': 'sqlite',
    'redis': 'dredis',
    'mongodb': 'mongodb',
    's3': 's3',
    'leveldb': 'ldb',
    'bdm': 'bdb'
}

for scheme in BACKENDS.keys():
    urlparse.uses_netloc.append(scheme)

def parse_backend_uri(backend_uri):
    """
    Converts the "backend_uri" into a cache scheme ('db', 'memcached', etc), a
    host and any extra params that are required for the backend. Returns a
    (scheme, host, params) tuple.
    """
    if backend_uri.find('://') == -1:
        raise InvalidCacheBackendError("Backend URI must start with scheme://")
    
    url = urlparse.urlparse(backend_uri)
    if url.scheme not in BACKENDS:
        raise InvalidCacheBackendError("Backend URI invalid scheme")

    params = dict(urlparse.parse_qsl(url.query))

    return url.scheme, url, params


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

