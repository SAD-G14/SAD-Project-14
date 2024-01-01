import socket
from typing import Callable
import requests
import json
from typing import Tuple


class Client:
    def __init__(self, host: str, port: int) -> None:
        self.socket = socket.socket()
        self.socket.connect((host, port))
        return

    def push(self, key: str, value: bytes) -> None:
        # url = f"http://{self.host}:{self.port}/queue/push"
        url = "http://127.0.0.1:5000/queue/push"
        data = {key: value.decode()}  # Convert bytes to string
        headers = {'Content-Type': 'application/json'}
        json_data = json.dumps(data)
        try:
            response = requests.post(url, data=json_data, headers=headers)
            response.raise_for_status()
            print(response.text)
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

    def pull(self) -> Tuple[str, bytes]:
        # url = f"http://{self.host}:{self.port}/health"
        url = "http://127.0.0.1:5000/queue/pull"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return str(response.status_code), response.content
        except requests.exceptions.RequestException as e:
            return str(e), b''

    def subscribe(self, f: Callable[[str, bytes], None]) -> None:
        pass
