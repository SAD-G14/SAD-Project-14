from broker.data import message_request as MessageData
from broker.model.message import Message as MessageModel
from broker.application.interfaces import DB
from broker.filemanager import FileManager

db = FileManager()


def push(message_data: MessageData) -> dict:
    message = MessageModel.from_data(message_data)
    written_message = db.write(message)
    #return written_message
    return {'producer_id': written_message.producer_id, 'sequence_number': written_message.sequence_number}


def pull():
    message = db.read()
    message.hidden = True
    db.write(message)
    return message.serialize()

def ack(producer_id: int, sequence_number: int) -> dict:
   # Find the message in the queue and mark it as acknowledged
   # This depends on how your messages are stored in the queue
   message = find_message_in_queue(producer_id, sequence_number) # TODO find message in queue
   if message:
       message.acknowledged = True
       return {'status': 'success'}
   else:
       return {'status': 'failure'}

