import os
import unittest
from unittest.mock import patch, MagicMock

import json
import threading
import time
from collections import Counter
from broker.application import broker
from broker.main import app
from broker.model import message
from broker.filemanager import FileManager
from broker.model.message import Message
from broker.data.message_request import MessageRequest
from broker.application.broker import push, pull, ack
from client.Client import Client
from client.Client import TIME_BETWEEN_PULLS


def setUp():
    broker.db.empty()
    # while (broker.db.read() is not None):
    #     continue


class TestClient(unittest.TestCase):
    def setUp(self):
        # empty database
        setUp()

    def test_add_positive_numbers(self):
        message_request = message.MessageRequest(key='test1', value='test2', date=1707058229,
                                                 producer_id=1707058229693, sequence_number=1)
        written_message = broker.push(message_request)
        # print(written_message)
        # self.assertTrue(True)
        self.assertEqual(written_message, {'producer_id': 1707058229693, 'sequence_number': 1})

    def test_push_and_pull(self):
        message_request = message.MessageRequest(key='test1', value='test2', date=1707058229,
                                                 producer_id=1707058229693, sequence_number=1)
        expected_message = message_request.serialize()
        expected_message.update(
            {'acknowledged': False, 'hidden': True, 'hidden_until': 0})  # Add expected default values

        broker.push(message_request)
        pulled_message = broker.pull()

        pulled_message['hidden_until'] = 0
        self.assertEqual(expected_message, pulled_message)

    def test_ack_successful(self):
        message_request = message.MessageRequest(key='test1', value='test2', date=1707058229,
                                                 producer_id=1707058229693, sequence_number=1)
        written_message = broker.push(message_request)
        self.assertTrue(broker.ack(1707058229693, 1))

    def test_ack_unsuccessful(self):
        message_request = message.MessageRequest(key='test1', value='test2', date=1707058229,
                                                 producer_id=1707058229693, sequence_number=1)
        written_message = broker.push(message_request)
        self.assertTrue(broker.ack(1707058229693, 2))

    def test_multiple_push_and_pull(self):
        messages = [
            message.MessageRequest(key='test1', value='testValue1', date=1707058229, producer_id=1707058229693,
                                   sequence_number=1),
            message.MessageRequest(key='test2', value='testValue2', date=1707058230, producer_id=1707058229694,
                                   sequence_number=2),
            message.MessageRequest(key='test3', value='testValue3', date=1707058231, producer_id=1707058229695,
                                   sequence_number=3),
        ]

        for msg in messages:
            broker.push(msg)

        for expected_msg in messages:
            pulled_msg = broker.pull()
            relevant_pulled_msg = {k: pulled_msg[k] for k in expected_msg.serialize()}
            self.assertEqual(expected_msg.serialize(), relevant_pulled_msg)

        self.assertIsNone(broker.pull())


class TestFileManager(unittest.TestCase):
    def setUp(self):
        # empty database
        setUp()

    def test_filemanager_read_write(self):
        filemanager = FileManager()
        self.assertEqual(filemanager.read(), None)

        class DummyClass:
            def __init__(self, var1, var2):
                self.var1 = var1
                self.var2 = var2
                self.hidden = False
                self.hidden_until = 0
                self.acknowledged = False

            def serialize(self):
                return {
                    'var1': self.var1,
                    'var2': self.var2,
                    'hidden': self.hidden,
                    'hidden_until': self.hidden_until,
                    'acknowledged': self.acknowledged
                }

        obj1 = DummyClass(1, 2)
        obj2 = DummyClass(3, 4)
        obj3 = DummyClass(5, 6)

        filemanager.write(obj1)
        read_obj1 = filemanager.read()
        self.assertEqual(obj1.serialize(), read_obj1)

        filemanager.write(obj2)
        filemanager.write(obj3)
        read_obj2 = filemanager.read()
        self.assertEqual(obj2.serialize(), read_obj2)

        read_obj3 = filemanager.read()
        self.assertEqual(obj3.serialize(), read_obj3)

        self.assertIsNone(filemanager.read())

    def test_filemanager_find_in_queue(self):
        class Message():
            def __init__(self, producer_id, sequence_number):
                self.producer_id = producer_id
                self.sequence_number = sequence_number
                self.hidden = False
                self.hidden_until = 0
                self.acknowledged = False

            def serialize(self):
                return {
                    'producer_id': self.producer_id,
                    'sequence_number': self.sequence_number,
                    'hidden': self.hidden,
                    'hidden_until': self.hidden_until,
                    'acknowledged': self.acknowledged
                }

        filemanager = FileManager()
        message = Message(1, 1)
        filemanager.write(message)
        filemanager.write(message)
        message.producer_id = 2
        filemanager.write(message)
        message.sequence_number = 2
        filemanager.write(message)
        message.producer_id = 1
        filemanager.write(message)
        message.sequence_number = 1
        self.assertEqual(filemanager.find_message_in_queue(1, 1), [message.serialize() for _ in range(2)])

    def test_find_message_not_in_queue(self):
        filemanager = FileManager()
        message = {
            'producer_id': 1,
            'sequence_number': 1,
            'key': 'Ali',
            'value': 'New message',
            'date': 2024,
            'hidden': False,
            'hidden_until': 0,
            'acknowledged': False
        }
        filemanager.write(message)
        found_messages = filemanager.find_message_in_queue(2, 1)
        self.assertEqual(len(found_messages), 0)

    def test_filemanager_empty(self):
        filemanager = FileManager()
        filemanager.write({'data': 'test'})
        filemanager.empty()
        self.assertIsNone(filemanager.read())


# NOT WORKING
class TestBroker(unittest.TestCase):
    # def setUp(self):
    #     # empty database
    #     while (broker.db.read() is not None):
    #         continue

    def setUp(self):
        self.filemanager = FileManager()
        self.produced_messages = []
        self.consumed_messages = []
        self.lock = threading.Lock()

    def produce_messages(self, producer_id, num_messages):
        local_produced = []
        for i in range(num_messages):
            msg = MessageRequest(f'key_{producer_id}_{i}', f'value_{producer_id}_{i}', int(time.time()), producer_id, i)
            serialized_msg = Message.from_data(msg).serialize()
            self.filemanager.write(serialized_msg)
            local_produced.append(serialized_msg)

        with self.lock:
            self.produced_messages.extend(local_produced)

    def consume_messages(self, num_consumers):
        local_consumed = []
        for _ in range(num_consumers):
            msg = self.filemanager.read()
            if msg:
                local_consumed.append(msg)

        with self.lock:
            self.consumed_messages.extend(local_consumed)

    def test_multiple_producers_consumers(self):
        num_producers = 5
        num_messages = 10
        num_consumers = num_producers * num_messages

        producers = [threading.Thread(target=self.produce_messages, args=(i, num_messages)) for i in
                     range(num_producers)]
        for producer in producers:
            producer.start()
        for producer in producers:
            producer.join()

        consumers = [threading.Thread(target=self.consume_messages, args=(num_consumers,)) for _ in
                     range(num_consumers)]
        for consumer in consumers:
            consumer.start()
        for consumer in consumers:
            consumer.join()

        # self.assertEqual(len(self.produced_messages), len(self.consumed_messages),
        #                  "Not all produced messages were consumed.")
        #
        produced_sorted = sorted(self.produced_messages, key=lambda x: (x['producer_id'], x['sequence_number']))
        consumed_sorted = sorted(self.consumed_messages, key=lambda x: (x['producer_id'], x['sequence_number']))
        #
        for produced, consumed in zip(produced_sorted, consumed_sorted):
            self.assertEqual(produced, consumed, "Mismatch between produced and consumed messages.")

        # produced_keys = [msg['key'] for msg in produced_sorted]
        # consumed_keys = [msg['key'] for msg in consumed_sorted]
        #
        # produced_counter = Counter(produced_keys)
        # consumed_counter = Counter(consumed_keys)
        #
        # self.assertEqual(produced_counter, consumed_counter, "Mismatch between produced and consumed messages counts.")


class TestBroker2(unittest.TestCase):
    def setUp(self):
        # empty database
        setUp()

    # def setUp(self):
    #     self.file_manager = FileManager()

    # NOT WORKING
    def test_acknowledgement(self):
        message_request = MessageRequest('testKey', 'testValue', 123456789, 1, 1)
        push(message_request)
        pulled_message = pull()

        ack_response = ack(pulled_message['producer_id'], pulled_message['sequence_number'])
        self.assertEqual(ack_response['status'], 'success')

        self.assertIsNone(pull())

    def test_acknowledge_nonexistent_message(self):
        ack_response = ack(999, 999)
        self.assertEqual(ack_response['status'], 'failure')


class TestBroker3(unittest.TestCase):
    def setUp(self):
        self.filemanager = FileManager()
        self.filemanager.empty()

    def test_multiple_push_and_pull(self):
        messages_to_push = [
            MessageRequest('key1', 'value1', 1707058229, 1, 1),
            MessageRequest('key2', 'value2', 1707058230, 2, 1),
            MessageRequest('key3', 'value3', 1707058231, 3, 1)
        ]

        for msg_request in messages_to_push:
            broker.push(msg_request)

        pulled_messages = []
        for _ in messages_to_push:
            pulled_message = broker.pull()
            if pulled_message is not None:
                pulled_message['hidden'] = True
                pulled_message['hidden_until'] = time.time() + 30
                self.filemanager.write(pulled_message)
                pulled_messages.append(pulled_message)

        for original_msg, pulled_msg in zip(messages_to_push, pulled_messages):
            self.assertEqual(original_msg.key, pulled_msg['key'])
            self.assertEqual(original_msg.value, pulled_msg['value'])
            self.assertEqual(original_msg.producer_id, pulled_msg['producer_id'])
            self.assertEqual(original_msg.sequence_number, pulled_msg['sequence_number'])

        for pulled_msg in pulled_messages:
            ack_result = broker.ack(pulled_msg['producer_id'], pulled_msg['sequence_number'])
            self.assertEqual(ack_result['status'], 'success')

        for _ in messages_to_push:
            self.assertIsNone(broker.pull())

    def test_more_than_one_ack(self):
        producer_id = 100
        sequence_number = 1
        msg_request = MessageRequest('key', 'value', int(time.time()), producer_id, sequence_number)
        push(msg_request)

        message = pull()
        self.assertIsNotNone(message, "Expected a message to be pulled but got None.")

        ack_response = ack(producer_id, sequence_number)
        self.assertEqual(ack_response['status'], 'success', "The message should be acknowledged successfully.")

        ack_response_second_attempt = ack(producer_id, sequence_number)
        self.assertEqual(ack_response_second_attempt['status'], 'failure',
                         "The second ack attempt should fail, indicating idempotency.")

        self.assertIsNone(pull(), "Expected no more messages to pull but got one.")


class TestSequenceNumber(unittest.TestCase):
    def setUp(self):
        self.filemanager = FileManager()
        self.filemanager.empty()

    def test_message_sequence_order(self):
        producer_id = 1
        num_messages = 3

        for sequence_number in range(1, num_messages + 1):
            msg_request = MessageRequest('key', 'value', int(time.time()), producer_id, sequence_number)
            push(msg_request)

        for expected_sequence_number in range(1, num_messages + 1):
            message = pull()
            self.assertIsNotNone(message, "Expected a message to be pulled but got None.")
            self.assertEqual(message['producer_id'], producer_id, "The producer_id should match the expected.")
            self.assertEqual(message['sequence_number'], expected_sequence_number,
                             "The sequence_number should be in order.")

        self.assertIsNone(pull(), "Expected no more messages to pull but got one.")


class TestMessageRequest(unittest.TestCase):
    def test_message_request_creation(self):
        message_request = MessageRequest('key', 'value', 123456789, 1, 1)
        self.assertEqual(message_request.key, 'key')
        self.assertEqual(message_request.value, 'value')
        self.assertEqual(message_request.date, 123456789)
        self.assertEqual(message_request.producer_id, 1)
        self.assertEqual(message_request.sequence_number, 1)


class TestMessage(unittest.TestCase):
    def setUp(self):
        # empty database
        setUp()

    def test_message_creation_and_serialization(self):
        message = Message('key', 'value', 123456789, 1, 1)
        serialized_message = message.serialize()
        expected_serialization = {
            'key': 'key',
            'value': 'value',
            'date': 123456789,
            'producer_id': 1,
            'sequence_number': 1,
            'hidden': False,
            'hidden_until': 0,
            'acknowledged': False
        }
        self.assertEqual(serialized_message, expected_serialization)


class TestFlaskApp(unittest.TestCase):
    def setUp(self):
        # empty database
        setUp()
        app.testing = True
        self.client = app.test_client()

    # def setUp(self):
    #     app.testing = True
    #     self.client = app.test_client()

    def test_push(self):
        response = self.client.post('/queue/push', json={
            'key': 'key',
            'value': 'value',
            'producer_id': 1,
            'sequence_number': 1
        })
        self.assertEqual(response.status_code, 200)
        self.assertIn('producer_id', response.json)
        self.assertIn('sequence_number', response.json)

    # NOT WORKING
    def test_pull(self):
        self.client.post('/queue/push', json={
            'key': 'key',
            'value': 'value',
            'producer_id': 1,
            'sequence_number': 1
        })
        response = self.client.get('/queue/pull')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['key'], 'key')

    def test_ack(self):
        self.client.post('/queue/push', json={
            'key': 'key',
            'value': 'value',
            'producer_id': 1,
            'sequence_number': 1
        })
        response = self.client.post('/queue/ack', json={
            'producer_id': 1,
            'sequence_number': 1
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['status'], 'success')


class TestClient2(unittest.TestCase):
    def setUp(self):
        self.client = Client('localhost', 5000)

    @patch('requests.get')
    def test_client_pull(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = json.dumps({
            'key': 'test_key',
            'value': 'test_value',
            'producer_id': 123,
            'sequence_number': 1
        }).encode('utf-8')

        status_code, message = self.client.pull()
        self.assertEqual(status_code, '200')
        self.assertIn('test_key', message.decode('utf-8'))

    @patch('requests.post')
    def test_client_send_ack(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'status': 'success'}

        self.client.send_ack(123, 1)
        mock_post.assert_called_with(
            "http://127.0.0.1:5000/queue/ack",
            data=json.dumps({'producer_id': 123, 'sequence_number': 1}),
            headers={'Content-Type': 'application/json'}
        )


if __name__ == '__main__':
    unittest.main()
