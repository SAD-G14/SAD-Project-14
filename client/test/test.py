import unittest
import requests

from client.client import Client


class TestClient(unittest.TestCase):
    def test_pull_exception(self):
        c = Client('localhost', 5000)
        key, value = c.pull()
        self.assertEqual(value, b'')

    def test_subscribe(self):
        c = Client('localhost', 5000)
        c.subscribe(print)
        while True:
            pass

if __name__ == '__main__':
    unittest.main()
