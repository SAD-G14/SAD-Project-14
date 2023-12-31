from broker.data import message_request as MessageData
from broker.model.message import Message as MessageModel
from interfaces import DB

db = DB()


def push(message_data: MessageData) -> MessageModel:
    message = MessageModel.from_data(message_data)
    written_message = db.write(message)
    return written_message
