import sys
sys.path.insert(0, '..')
import unittest
import MySQLdb
from kvcache import get_cache
import random


class KVTests(unittest.TestCase):
    URI = 'sqlite:///tmp/test.db.test'

    def test_get_set(self):
        cache = get_cache(self.URI)
        k, v = random.random(), random.random()
        cache.set(k, v)
        self.assertEqual(cache.get(k), v)

if __name__ == '__main__':
    unittest.main()        

