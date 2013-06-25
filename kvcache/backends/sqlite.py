"Database cache backend."
import base64
import time
import logging
from datetime import datetime
from .dbbase import DatabaseCache
import MySQLdb
import sqlite3


class SqliteDatabaseCache(DatabaseCache):
    def create(self):
        self._reconn()
        SQL_CREATE_TABLE = '''CREATE TABLE IF NOT EXISTS %s (
                                cache_key NOT NULL, value, expires, UNIQUE (cache_key) );'''
        self._create(SQL_CREATE_TABLE, self._table)

    def _reconn(self, num=28800, stime=3):
        self.conn()

    def conn(self):
        params = self.params
        try:
            self._conn = sqlite3.connect(self.params['db'])
            self._conn.text_factory = str
            return True
        except:
            logging.error('can not connect db', exc_info=True)
            pass
        return False

# For backwards compatibility
class CacheClass(SqliteDatabaseCache):
    pass
