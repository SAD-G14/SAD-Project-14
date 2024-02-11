class MessageRequest:
    def __init__(self, key: str, value: str, date: int, producer_id: int, sequence_number: int):
        self.key = key
        self.value = value
        self.date = date
        self.producer_id = producer_id
        self.sequence_number = sequence_number

    def serialize(self):
        return {
            'key': self.key,
            'value': self.value,
            'date': self.date,
            'producer_id': self.producer_id,
            'sequence_number': self.sequence_number,
            # Include any other attributes that are relevant and should be part of the serialized form
        }

    def str(self):
        return f"key: {self.key}, value: {self.value}, date: {self.date}, producer_id: {self.producer_id}," \
               f" sequence_number: {self.sequence_number}"
