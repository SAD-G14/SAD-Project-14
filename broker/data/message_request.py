class MessageRequest:
    def __init__(self, key: str, value: str, date: int, producer_id: int):
        self.key = key
        self.value = value
        self.date = date
        self.producer_id = producer_id
