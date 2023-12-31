import socket
from typing import Callable


class Client:
    def __init__(self, host: str, port: int) -> None:
        self.socket = socket.socket()
        self.socket.connect((host, port))
        return

    def push(self, key: str, value: bytes) -> None:  # bytes or bytearray?!
        pass

    def pull(self) -> tuple[str, bytes]:
        pass

    def subscribe(self, f: Callable[[str, bytes], None]) -> None:
        pass
