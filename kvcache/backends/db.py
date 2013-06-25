"Database cache backend."
import base64
import time
import logging
from datetime import datetime

try:
    import cPickle as pickle
except ImportError:
    import pickle

from django.conf import settings
from .base import BaseCache, MEMCACHE_MAX_KEY_LENGTH
from django.db import connections, router, transaction, DatabaseError
from django.utils import timezone, six
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
        ''' create collection '''

        SQL_CREATE_TABLE = '''CREATE TABLE IF NOT EXISTS %%s (
                                cache_key varchar(%s) PRIMARY KEY,
                                value MEDIUMBLOB,
                                expires int NOT NULL
                                ) ENGINE=MYISAM  DEFAULT CHARSET=utf8;;''' % MEMCACHE_MAX_KEY_LENGTH

        self._create(SQL_CREATE_TABLE, self._table)

    def _create(self, sql_create_table, name):
        ''' create collection by name '''
        cursor = self.cursor()
        cursor.execute(sql_create_table % name)
        self._conn.commit()

    def _reconn(self, num=28800, stime=3):
        _number = 0
        _status = True
        while _status and _number <= num:
            try:
                self._conn.ping()  # cping 校验连接是否异常
                _status = False
            except:
                if self.conn() == True:  # 重新连接,成功退出
                    _status = False
                    break
                _number += 1
                logging.error('connection to mysql %s fail', self.uri)
                time.sleep(stime)

    def conn(self):
        params = self.params
        try:
            self._conn = MySQLdb.connect(
                host=params['host'], port=params['port'],
                user=params['username'], passwd=params[
                    'password'],
                db=params['db'])
            return True
        except MySQLdb.OperationalError, err:
            pass
        return False

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
        db = router.db_for_read(self.cache_model_class)
        table = connections[db].ops.quote_name(self._table)
        cursor = connections[db].cursor()

        cursor.execute("SELECT cache_key, value, expires FROM %s "
                       "WHERE cache_key = %%s" % table, [key])
        row = cursor.fetchone()
        if row is None:
            return default
        now = timezone.now()
        if row[2] < now:
            db = router.db_for_write(self.cache_model_class)
            cursor = connections[db].cursor()
            cursor.execute("DELETE FROM %s "
                           "WHERE cache_key = %%s" % table, [key])
            transaction.commit_unless_managed(using=db)
            return default
        value = connections[db].ops.process_clob(row[1])
        return pickle.loads(base64.b64decode(force_bytes(value)))

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
        db = router.db_for_write(self.cache_model_class)
        table = connections[db].ops.quote_name(self._table)
        cursor = connections[db].cursor()

        cursor.execute("SELECT COUNT(*) FROM %s" % table)
        num = cursor.fetchone()[0]
        now = timezone.now()
        now = now.replace(microsecond=0)
        if settings.USE_TZ:
            exp = datetime.utcfromtimestamp(time.time() + timeout)
        else:
            exp = datetime.fromtimestamp(time.time() + timeout)
        exp = exp.replace(microsecond=0)
        if num > self._max_entries:
            self._cull(db, cursor, now)
        pickled = pickle.dumps(value, pickle.HIGHEST_PROTOCOL)
        b64encoded = base64.b64encode(pickled)
        # The DB column is expecting a string, so make sure the value is a
        # string, not bytes. Refs #19274.
        if six.PY3:
            b64encoded = b64encoded.decode('latin1')
        cursor.execute("SELECT cache_key, expires FROM %s "
                       "WHERE cache_key = %%s" % table, [key])
        try:
            result = cursor.fetchone()
            if result and (mode == 'set' or
                    (mode == 'add' and result[1] < now)):
                cursor.execute("UPDATE %s SET value = %%s, expires = %%s "
                               "WHERE cache_key = %%s" % table,
                               [b64encoded, connections[db].ops.value_to_db_datetime(exp), key])
            else:
                cursor.execute("INSERT INTO %s (cache_key, value, expires) "
                               "VALUES (%%s, %%s, %%s)" % table,
                               [key, b64encoded, connections[db].ops.value_to_db_datetime(exp)])
        except DatabaseError:
            # To be threadsafe, updates/inserts are allowed to fail silently
            transaction.rollback_unless_managed(using=db)
            return False
        else:
            transaction.commit_unless_managed(using=db)
            return True

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        db = router.db_for_write(self.cache_model_class)
        table = connections[db].ops.quote_name(self._table)
        cursor = connections[db].cursor()

        cursor.execute("DELETE FROM %s WHERE cache_key = %%s" % table, [key])
        transaction.commit_unless_managed(using=db)

    def has_key(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        db = router.db_for_read(self.cache_model_class)
        table = connections[db].ops.quote_name(self._table)
        cursor = connections[db].cursor()

        if settings.USE_TZ:
            now = datetime.utcnow()
        else:
            now = datetime.now()
        now = now.replace(microsecond=0)
        cursor.execute("SELECT cache_key FROM %s "
                       "WHERE cache_key = %%s and expires > %%s" % table,
                       [key, connections[db].ops.value_to_db_datetime(now)])
        return cursor.fetchone() is not None

    def _cull(self, db, cursor, now):
        if self._cull_frequency == 0:
            self.clear()
        else:
            # When USE_TZ is True, 'now' will be an aware datetime in UTC.
            now = now.replace(tzinfo=None)
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
        db = router.db_for_write(self.cache_model_class)
        table = connections[db].ops.quote_name(self._table)
        cursor = connections[db].cursor()
        cursor.execute('DELETE FROM %s' % table)

# For backwards compatibility
class CacheClass(DatabaseCache):
    pass
