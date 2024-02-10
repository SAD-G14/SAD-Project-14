from client.client import Client


def send_test_message():
    c = Client('localhost', 5000)
    c.push('test1', 'test2'.encode())


send_test_message()
