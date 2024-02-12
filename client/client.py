import logging
import time
import requests
import json
from threading import Thread, Event
from typing import Tuple, List, Callable
from retry import retry

TIME_BETWEEN_PULLS = 1

PROTOCOL = 'HTTP'
REQUEST = {'push': 'queue/push', 'pull': 'queue/pull', 'ack': 'queue/ack', 'health': 'health'}


class Client:
    def __init__(self, host: str='195.177.255.132', port: int=4000) -> None:
        self.host = host
        self.port = port
        self.sequence_number = 0
        # TODO: server should determine pID
        # maybe not, because Auth is needed and this is MVP
        self.producer_id = int(round(time.time() * 1000))
        self.threads: List[Thread] = []
        self.event: Event = Event()
        return

    def deconstruct(self) -> None:
        self.event.set()
        for thread in self.threads:
            thread.join()
        return None

    def push(self, key: str, value: bytes) -> str:
        self.sequence_number += 1
        url = f'{PROTOCOL}://{self.host}:{self.port}/' + REQUEST['push']
        data = {'key': key, 'value': value.decode(), 'sequence_number': self.sequence_number,
                'producer_id': self.producer_id}
        headers = {'Content-Type': 'application/json'}
        json_data = json.dumps(data)
        try:
            response = requests.post(url, data=json_data, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

    def pull(self) -> Tuple[str, bytes]:
        url = f'{PROTOCOL}://{self.host}:{self.port}/' + REQUEST['pull']
        try:
            response = requests.post(url, data=json.dumps({'producer_id': self.producer_id}), headers={'Content-Type': 'application/json'})
            response.raise_for_status()
            response_json = json.loads(response.content)
            if response_json is None:
                raise Exception("response is empty")
            self.send_ack(int(response_json['producer_id']), int(response_json['sequence_number']))
            return str(response_json['key']), str(response_json['value']).encode()
        except Exception as e:
            logging.error(e)
            return 'exception', str(e).encode()

    def subscribe(self, f: Callable[[str, bytes], None]) -> None:
        self.threads.append(Thread(target=self.consumer_function, args=(f,)))
        self.threads[-1].start()
        return

    def consumer_function(self, f: Callable[[str, bytes], None]) -> None:
        while True:
            time.sleep(TIME_BETWEEN_PULLS)
            key, value = self.pull()
            if key != 'exception':
                f(key, value)
            if self.event.is_set():
                break
        return

    @retry(requests.exceptions.RequestException, tries=5, delay=2, backoff=2)
    def send_ack(self, producer_id: int, sequence_number: int, key:str='key') -> None:
        url = f'{PROTOCOL}://{self.host}:{self.port}/' + REQUEST['ack']
        try:
            data = {'producer_id': producer_id, 'sequence_number': sequence_number, 'key': key}
            headers = {'Content-Type': 'application/json'}
            json_data = json.dumps(data)
            response = requests.post(url, data=json_data, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            return str(e), b''
