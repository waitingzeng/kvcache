import sys
sys.path.insert(0, '..')
import unittest
from kvcache import get_cache
import random


class KVTests(unittest.TestCase):
    URI = 'memcached://127.0.0.1'

    def test_get_set(self):
        cache = get_cache(self.URI)
        k, v = random.random(), random.random()
        cache.set(k, v)
        self.assertEqual(cache.get(k), v)
        cache.delete(k)
        self.assertEqual(cache.get(k), None)
        

if __name__ == '__main__':
    unittest.main()        

