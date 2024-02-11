import time
import unittest
import requests

from client.client import Client

LOG_FILE = 'test.log'


def subscribe_function(key, value) -> None:
    with open(LOG_FILE, 'a') as file:
        file.write(f'key: {key}, value: {value}\n')
    return None


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        self.client = Client('localhost', 5000)
        with open(LOG_FILE, 'w') as file:
            file.truncate()
        return None

    def tearDown(self) -> None:
        self.client.deconstruct()
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

    def test_push_ack_pull(self):
        self.client.push('key_1', b'value_1')
        self.client.send_ack(self.client.producer_id, self.client.sequence_number)
        key, value = self.client.pull()
        self.assertEqual('exception', key)

    def test_subscribe_push(self):
        self.client.subscribe(subscribe_function)
        time.sleep(5)
        key, value = 'key_1', b'value_1'
        self.client.push(key, value)
        time.sleep(5)
        with open(LOG_FILE, 'r') as file:
            line = file.readline()
        self.assertEqual(f'key: {key}, value: {value}\n', line)


if __name__ == '__main__':
    unittest.main()
