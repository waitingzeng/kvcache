#!/user/bin/python
#coding=utf8
"Database cache backend."
import base64
import time
import logging
from datetime import datetime
from .dbbase import DatabaseCache, BaseCache
from .base import MEMCACHE_MAX_KEY_LENGTH
import MySQLdb


class MySQLDatabaseCache(DatabaseCache):
    place_hold = '%s'

    def __init__(self, url, params):
        BaseCache.__init__(self, params)
        self._url = url
        self.db = url.path
        if self.db.find('.') != -1:
            self.db, self._table = self.db.rsplit('.', 1)
        else:
            self._table = params.get('table', 'kvcache')
        self.create()

    def create(self):
        ''' create collection '''

        SQL_CREATE_TABLE = '''CREATE TABLE IF NOT EXISTS %%s (
                                cache_key varchar(%s) PRIMARY KEY,
                                value MEDIUMBLOB,
                                expires int NOT NULL
                                ) ENGINE=MYISAM  DEFAULT CHARSET=utf8;;''' % MEMCACHE_MAX_KEY_LENGTH

        self._create(SQL_CREATE_TABLE, self._table)

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
                logging.error('connection to mysql %s fail', self._url)
                time.sleep(stime)

    def conn(self):
        url = self._url
        print url.hostname, url.port, url.username, url.password, self.db
        try:
            self._conn = MySQLdb.connect(
                host=url.hostname, port=url.port or 3306,
                user=url.username, passwd=url.password,
                db=self.db.lstrip('/'))
            return True
        except MySQLdb.OperationalError, err:
            pass
        return False

# For backwards compatibility
class CacheClass(MySQLDatabaseCache):
    pass
