from broker.model.message import Message


class DB:
    # this should also set the sequence number
    def write(self, message: Message) -> Message:
        pass

    def read(self) -> Message:
        pass