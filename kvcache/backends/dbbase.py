"Database cache backend."
import base64
import time
import logging

try:
    import cPickle as pickle
except ImportError:
    import pickle

from .base import BaseCache, MEMCACHE_MAX_KEY_LENGTH
from ..utils.encoding import force_bytes


class BaseDatabaseCache(BaseCache):
    def __init__(self, table, params):
        BaseCache.__init__(self, params)
        self._table = table
        self.create()

    def cursor(self):
        self._reconn()
        return self._conn.cursor()

    def create(self):
        raise NotImplementedError

    def _create(self, sql_create_table, name):
        ''' create collection by name '''
        cursor = self.cursor()
        cursor.execute(sql_create_table % name)
        self._conn.commit()

    def _reconn(self, num=28800, stime=3):
        return True

    def conn(self):
        raise NotImplementedError

class DatabaseCache(BaseDatabaseCache):

    # This class uses cursors provided by the database connection. This means
    # it reads expiration values as aware or naive datetimes depending on the
    # value of USE_TZ. They must be compared to aware or naive representations
    # of "now" respectively.

    # But it bypasses the ORM for write operations. As a consequence, aware
    # datetimes aren't made naive for databases that don't support time zones.
    # We work around this problem by always using naive datetimes when writing
    # expiration values, in UTC when USE_TZ = True and in local time otherwise.

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        cursor = self.cursor()

        cursor.execute("SELECT cache_key, value, expires FROM %s "
                       "WHERE cache_key = %%s" % table, [key])
        row = cursor.fetchone()
        if row is None:
            return default
        now = time.time()
        if row[2] < now:
            db = router.db_for_write(self.cache_model_class)
            cursor = connections[db].cursor()
            cursor.execute("DELETE FROM %s "
                           "WHERE cache_key = %%s" % table, [key])
            return default
        value = row[1]
        return pickle.loads(value)

    def set(self, key, value, timeout=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        self._base_set('set', key, value, timeout)

    def add(self, key, value, timeout=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return self._base_set('add', key, value, timeout)

    def _base_set(self, mode, key, value, timeout=None):
        if timeout is None:
            timeout = self.default_timeout
        cursor = self.cursor()

        cursor.execute("SELECT COUNT(*) FROM %s" % table)
        num = cursor.fetchone()[0]
        now = timezone.now()
        now = int(time.time())
        exp = now + timeout
        if num > self._max_entries:
            self._cull(db, cursor, now)
        pickled = pickle.dumps(value, pickle.HIGHEST_PROTOCOL)
        cursor.execute("SELECT cache_key, expires FROM %s "
                       "WHERE cache_key = %%s" % table, [key])
        try:
            result = cursor.fetchone()
            if result and (mode == 'set' or
                    (mode == 'add' and result[1] < now)):
                cursor.execute("UPDATE %s SET value = %%s, expires = %%s "
                               "WHERE cache_key = %%s" % table,
                               [pickled, exp, key])
            else:
                cursor.execute("INSERT INTO %s (cache_key, value, expires) "
                               "VALUES (%%s, %%s, %%s)" % table,
                               [key, pickled, exp])
        except:
            # To be threadsafe, updates/inserts are allowed to fail silently
            return False
        else:
            return True

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        cursor = self.cursor()

        cursor.execute("DELETE FROM %s WHERE cache_key = %%s" % table, [key])

    def has_key(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        cursor = self.cursor()
        now = int(time.time())
        cursor.execute("SELECT cache_key FROM %s "
                       "WHERE cache_key = %%s and expires > %%s" % table,
                       [key, now])
        return cursor.fetchone() is not None

    def _cull(self, db, cursor, now):
        if self._cull_frequency == 0:
            self.clear()
        else:
            # When USE_TZ is True, 'now' will be an aware datetime in UTC.
            cursor = int(time.time())
            now = int(time.time())
            table = connections[db].ops.quote_name(self._table)
            cursor.execute("DELETE FROM %s WHERE expires < %%s" % table,
                           [connections[db].ops.value_to_db_datetime(now)])
            cursor.execute("SELECT COUNT(*) FROM %s" % table)
            num = cursor.fetchone()[0]
            if num > self._max_entries:
                cull_num = num // self._cull_frequency
                cursor.execute(
                    connections[db].ops.cache_key_culling_sql() % table,
                    [cull_num])
                cursor.execute("DELETE FROM %s "
                               "WHERE cache_key < %%s" % table,
                               [cursor.fetchone()[0]])

    def clear(self):
        cursor = self.cursor()
        cursor.execute('DELETE FROM %s' % table)
