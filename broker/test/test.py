import os
import unittest

# from flask import json
import json
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
        self.assertEqual(filemanager.read(), json.dumps(obj1, default=vars))
        filemanager.write(obj2)
        filemanager.write(obj3)
        self.assertEqual(filemanager.read(), obj3)
        self.assertEqual(filemanager.read(), obj2)

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


if __name__ == '__main__':
    unittest.main()
