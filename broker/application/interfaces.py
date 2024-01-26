from broker.model.message import Message


class DB:
    def __init__(self):
        self.last_sequence_numbers = {}

    # this should also set the sequence number
    def write(self, message: Message) -> Message:
        if self.is_duplicate(message.producer_id, message.sequence_number):
            return None
        # Write the message to the DB

        self.update_sequence_number(message.producer_id, message.sequence_number)
        return message

    def read(self) -> Message:
        pass

    def update_sequence_number(self, producer_id, sequence_number):
        self.last_sequence_numbers[producer_id] = sequence_number

    def is_duplicate(self, producer_id, sequence_number):
        return self.last_sequence_numbers.get(producer_id, -1) >= sequence_number
