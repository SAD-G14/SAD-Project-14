import time
import unittest
import requests

from client.client import Client


def subscribe_function(key, value) -> None:
    with open('test_log.txt', 'a') as file:
        file.write(f'key: {key}, value: {value}')
    return None


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        self.client = Client('localhost', 5000)
        with open('test_log.txt', 'w') as file:
            file.truncate()
        return None

    def tearDown(self) -> None:
        del self.client
        return None

    def test_pull_empty(self):
        key, value = self.client.pull()
        self.assertEqual('exception', key)

    def test_push_pull(self):
        self.client.push('key', b'value')
        key, value = self.client.pull()
        self.assertEqual('key', key)
        self.assertEqual(b'value', value)

    def test_push_push_pull_pull(self):
        self.client.push('key_1', b'value_1')
        self.client.push('key_2', b'value_2')
        key, value = self.client.pull()
        self.assertEqual('key_1', key)
        self.assertEqual(b'value_1', value)
        key, value = self.client.pull()
        self.assertEqual('key_2', key)
        self.assertEqual(b'value_2', value)

    def test_subscribe_push(self):
        self.client.subscribe(subscribe_function)
        time.sleep(5)
        self.client.push('key_1', b'value_1')
        time.sleep(5)
        self.client.__del__()


if __name__ == '__main__':
    unittest.main()
