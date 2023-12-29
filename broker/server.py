import socket
import queue
from concurrent.futures import ThreadPoolExecutor


class Server:
    def __init__(self, host: str, port: int, n_threads=3) -> None:
        self.socket = socket.socket()
        self.socket.bind((host, port))
        # TODO initialize queues
        self.handle_queue = queue.PriorityQueue()
        self.send_queue = queue.PriorityQueue()
        # TODO initialize threads
        # use thread_pool.submit(function)
        self.thread_pool = ThreadPoolExecutor(max_workers=n_threads)
        self.thread_pool.submit(self.receive)
        self.thread_pool.submit(self.receive)

    def receive(self):
        while True:
            conn, addr = self.socket.accept()
            request = conn.recv(1024)  # TODO fix this number
            # process request
            self.handle_queue.put((conn, addr, request))
        pass

    def send(self):
        while True:
            conn, addr, response = self.send_queue.get()
            # process message
            conn.send(response)
        pass
