from broker.data.message_request import MessageRequest


class Message:
    def __init__(self, key: str, value: str, date: int, producer_id: int,
                 sequence_number: int, hidden: bool = False, hidden_until: int = 0):
        self.key = key
        self.value = value
        self.date = date
        self.producer_id = producer_id
        self.sequence_number = sequence_number
        self.hidden = hidden
        self.hidden_until = hidden_until

    def from_data(message: MessageRequest):  # static method (ignore pyCharm warning about this)
        return Message(
            message.key, message.value, message.date, message.producer_id, None
        )
