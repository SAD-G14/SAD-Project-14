import time
import unittest
import requests

from client.client import Client


class TestClient(unittest.TestCase):
    def test_pull_empty(self):
        client = Client('localhost', 5000)
        key, value = client.pull()
        self.assertEqual('200', key)
        self.assertEqual(b'null\n', value)

    def test_subscribe(self):
        c = Client('localhost', 5000)
        c.subscribe(print)
        time.sleep(5)

if __name__ == '__main__':
    unittest.main()
