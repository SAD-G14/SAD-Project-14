import logging
import random
import time
import requests
import json
from threading import Thread, Event
from typing import Tuple, List, Callable
from retry import retry

TIME_BETWEEN_PULLS = 1
MAX_TIME_BETWEEN_PULLS = 2
MIN_TIME_BETWEEN_PULLS = 0.05

PROTOCOL = 'HTTP'
REQUEST = {'push': 'queue/push', 'pull': 'queue/pull', 'ack': 'queue/ack', 'health': 'health'}


class Client:
    def __init__(self, host:str='195.177.255.132', port: int = 4000, ports=None) -> None:
        self.ports = ports
        if ports is None:
            self.port = port
        else:
            self.port = random.choice(ports)
        self.host = host
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
            logging.error("could not push to server: {}".format(e))
            self.change_server()
        except Exception as e:
            print(f"An error occurred whie pushing to server: {e}")

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
        except requests.exceptions.RequestException as e:
            logging.error("could not pull from server: {}".format(e))
            self.change_server()
            return 'exception', str(e).encode()
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
            try:
                key, value = self.pull()
                if key != 'exception':
                    TIME_BETWEEN_PULLS = max(MIN_TIME_BETWEEN_PULLS, TIME_BETWEEN_PULLS / 2)
                    f(key, value)
                else:
                    TIME_BETWEEN_PULLS = min(MAX_TIME_BETWEEN_PULLS, TIME_BETWEEN_PULLS * 2)
                if self.event.is_set():
                    break
            except Exception as e:
                TIME_BETWEEN_PULLS = min(MAX_TIME_BETWEEN_PULLS, TIME_BETWEEN_PULLS * 2)
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

    def change_server(self):
        logging.warning("server {} unreachable, changing port".format(self.port))
        if self.ports:
            self.port = random.choice(self.ports)
