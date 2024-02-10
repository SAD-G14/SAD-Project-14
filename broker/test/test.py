import os
import unittest

# from flask import json
import json
import threading
import time
from broker.application import broker
from broker.model import message
from broker.filemanager import FileManager
from broker.model.message import Message
from broker.data.message_request import MessageRequest


class TestClient(unittest.TestCase):
    def test_add_positive_numbers(self):
        message_request = message.MessageRequest(key='test1', value='test2', date=1707058229,
                                                 producer_id=1707058229693, sequence_number=1)
        written_message = broker.push(message_request)
        print(written_message)
        self.assertTrue(True)

    def test_push_and_pull(self):
        message_request = message.MessageRequest(key='test1', value='test2', date=1707058229,
                                                 producer_id=1707058229693, sequence_number=1)
        expected_message = message_request.serialize()
        expected_message.update(
            {'acknowledged': False, 'hidden': True, 'hidden_until': 0})  # Add expected default values

        broker.push(message_request)
        pulled_message = broker.pull()

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
    def test_filemanager_read_write(self):
        filemanager = FileManager()
        self.assertEqual(filemanager.read(), None)

        class DummyClass:
            def __init__(self, var1, var2):
                self.var1 = var1
                self.var2 = var2

        obj1 = DummyClass(1, 2)
        obj2 = DummyClass(3, 4)
        obj3 = DummyClass(5, 6)

        filemanager.write(obj1)
        read_obj1 = filemanager.read()
        expected_obj1 = {'var1': 1,
                         'var2': 2}
        self.assertEqual(read_obj1, expected_obj1)

        filemanager.write(obj2)
        filemanager.write(obj3)
        read_obj2 = filemanager.read()
        expected_obj2 = {'var1': 3, 'var2': 4}
        self.assertEqual(read_obj2, expected_obj2)

        read_obj3 = filemanager.read()
        expected_obj3 = {'var1': 5, 'var2': 6}
        self.assertEqual(read_obj3, expected_obj3)

        self.assertIsNone(filemanager.read())

    def test_filemanager_find_in_queue(self):
        class Message():
            def __init__(self, producer_id, sequence_number):
                self.producer_id = producer_id
                self.sequence_number = sequence_number

            def serialize(self):
                return {
                    'producer_id': self.producer_id,
                    'sequence_number': self.sequence_number
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
            'hidden': False
        }
        filemanager.write(message)
        found_messages = filemanager.find_message_in_queue(2, 1)
        self.assertEqual(len(found_messages), 0)


# NOT WORKING
class TestBroker(unittest.TestCase):
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

        produced_sorted = sorted(self.produced_messages, key=lambda x: (x['producer_id'], x['sequence_number']))
        consumed_sorted = sorted(self.consumed_messages, key=lambda x: (x['producer_id'], x['sequence_number']))

        for produced, consumed in zip(produced_sorted, consumed_sorted):
            self.assertEqual(produced, consumed, "Mismatch between produced and consumed messages.")


if __name__ == '__main__':
    unittest.main()
