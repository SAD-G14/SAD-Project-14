from broker.data import message_request as MessageData
from broker.filemanager import FileManager
from broker.model.message import Message as MessageModel

import time

db = FileManager()


def push(message_data: MessageData) -> dict:
    message = MessageModel.from_data(message_data)
    written_message = db.write(message)
    # return written_message
    return {'producer_id': written_message.producer_id, 'sequence_number': written_message.sequence_number}


def pull():
    message = db.read()
    if message:
        message['hidden'] = True
        message['hidden_until'] = time.time() + 30000 # add 30 seconds
        db.write(message)
        return message
    else:
        return None


def ack(producer_id: int, sequence_number: int) -> dict:
    messages = db.find_message_in_queue(producer_id, sequence_number)
    if messages:
        for message in messages:
            message['acknowledged'] = True
            db.write(message)
        return {'status': 'success'}
    else:
        return {'status': 'failure'}
