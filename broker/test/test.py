import unittest

from broker.application import broker
from broker.model import message


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
        written_message = broker.push(message_request)
        pulled_message = broker.pull()
        self.assertEqual(message_request.serialize(), pulled_message)

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


if __name__ == '__main__':
    unittest.main()
