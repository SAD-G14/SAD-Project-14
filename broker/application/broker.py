import logging
import time

import requests

from broker.data import message_request as MessageData
from broker.filemanager import FileManager
from broker.model.message import Message as MessageModel

db = None
PARTITION = 0
REPLICA = None

def push(message_data: MessageData) -> dict:
    message = MessageModel.from_data(message_data)
    written_message = db.write(message)
    # return written_message
    return {'producer_id': written_message.producer_id, 'sequence_number': written_message.sequence_number}


def pull():
    message = db.read()
    message.hidden = True
    db.write(message)
    return message.serialize()


def ack(producer_id: int, sequence_number: int) -> dict:

    # Find the message in the queue and mark it as acknowledged
    # This depends on how your messages are stored in the queue
    message = db.find_message_in_queue(producer_id, sequence_number)  # TODO find message in queue
    if message:
        message.acknowledged = True
        return {'status': 'success'}
    else:
        return {'status': 'failure'}

def join_server():
    while True:
        try:
            # change for local test
            res = requests.get('http://server:4000/join').json()
            logging.info("Joined server with response: {}".format(res))
            global db
            db = FileManager(res['broker']['partition'], res['broker']['replica'])
            return
        except Exception as e:
            logging.warn("could not join server: {}".format(e))
            time.sleep(1)

def accept_replica(replica):
    global REPLICA
    REPLICA = replica
    logging.info("Accepted replica: {}".format(replica))