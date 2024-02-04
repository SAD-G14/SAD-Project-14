from broker.model.message import Message
from broker.filemanager import FileManager


class DB:
    # this should also set the sequence number
    def write(self, message: Message) -> Message:
        FileManager.write(self, message)

    def read(self) -> Message:
        pass
