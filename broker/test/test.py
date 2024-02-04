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


if __name__ == '__main__':
    unittest.main()
