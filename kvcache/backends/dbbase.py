"Database cache backend."
import base64
import time
import logging
from .base import BaseCache, MEMCACHE_MAX_KEY_LENGTH


class BaseDatabaseCache(BaseCache):
    place_hold = '?'


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

    @property
    def sql_params(self):
        return {'table': self._table, 'place_hold': self.place_hold}


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

        cursor.execute("SELECT cache_key, value, expires FROM %(table)s "
                       "WHERE cache_key = %(place_hold)s" % self.sql_params, [key])
        row = cursor.fetchone()
        if row is None:
            return default
        now = time.time()
        if row[2] < now:
            cursor.execute("DELETE FROM %(table)s "
                           "WHERE cache_key = %(place_hold)s" % self.sql_params, [key])
            return default
        value = row[1]
        return self.decode(value)

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
        
        cursor.execute("SELECT COUNT(*) FROM %(table)s" % self.sql_params)
        num = cursor.fetchone()[0]
        now = int(time.time())
        exp = now + timeout
        if self._max_entries and num > self._max_entries:
            self._cull(db, cursor, now)
        pickled = self.encode(value)
        sql = "SELECT cache_key, expires FROM %(table)s WHERE cache_key = %(place_hold)s" % self.sql_params
        cursor.execute(sql, [key])
        try:
            result = cursor.fetchone()
            if result and (mode == 'set' or
                    (mode == 'add' and result[1] < now)):
                cursor.execute("UPDATE %(table)s SET value = %(place_hold)s, expires = %(place_hold)s "
                               "WHERE cache_key = %(place_hold)s" % self.sql_params,
                               [pickled, exp, key])
            else:
                sql = "INSERT INTO %(table)s (cache_key, value, expires) VALUES (%(place_hold)s, %(place_hold)s, %(place_hold)s)" % self.sql_params
                cursor.execute(sql, [str(key), pickled, exp])
            self._conn.commit()
        except:
            logging.error('set fail', exc_info=True)
            # To be threadsafe, updates/inserts are allowed to fail silently
            return False
        else:
            return True

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        cursor = self.cursor()

        cursor.execute("DELETE FROM %(table)s WHERE cache_key = %(place_hold)s" % self.sql_params, [key])

    def has_key(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        cursor = self.cursor()
        now = int(time.time())
        cursor.execute("SELECT cache_key FROM %(table)s "
                       "WHERE cache_key = %(place_hold)s and expires > %(place_hold)s" % self.sql_params,
                       [key, now])
        return cursor.fetchone() is not None

    def _cull(self, db, cursor, now):
        if self._cull_frequency == 0:
            self.clear()
        else:
            # When USE_TZ is True, 'now' will be an aware datetime in UTC.
            cursor = int(time.time())
            now = int(time.time())
            cursor.execute("DELETE FROM %(table)s WHERE expires < %(place_hold)s" % self.sql_params,
                           [connections[db].ops.value_to_db_datetime(now)])
            """
            cursor.execute("SELECT COUNT(*) FROM %(table)s" % self.sql_params)
            num = cursor.fetchone()[0]
            if num > self._max_entries:
                cull_num = num // self._cull_frequency
                cursor.execute(self.cache_key_culling_sql() % table,
                    [cull_num])
                cursor.execute("DELETE FROM %(table)s "
                               "WHERE cache_key < %(place_hold)s" % self.sql_params,
                               [cursor.fetchone()[0]])
            """

    def clear(self):
        cursor = self.cursor()
        cursor.execute('DELETE FROM %(table)s' % self.sql_params)
