import os
import json
import datetime


class FileManager:

    def __init__(self):
        self.pushed_objects_count = 0
        self.log_idx = 0  # index of last log file created
        self.log_dir = 'logs'
        os.makedirs(self.log_dir, exist_ok=True)
        self.last_log = os.path.join(self.log_dir, f'{self.log_idx}.log')  # path of last log file
        with open(self.last_log, 'w') as log_file:
            log_file.write('')

    def read(self):
        try:
            with open(self.last_log, 'r+') as log_file:
                lines = log_file.readlines()
                if not lines:  # Check if there are no lines to read
                    return None
                # oldest_message = lines[0]  # The first line is the oldest
                index = 0
                while (index < len(oldest_message)):
                    data = json.loads(oldest_message[i])
                    if (not data.hidden) or hidden_until < datetime.datetime.now():
                        break
                    index += 1
                if index < len(oldest_message):
                    log_file.seek(0)  # Go back to the start of the file
                    log_file.truncate()  # Truncate the file to remove any leftover content
                    log_file.writelines(lines[:index], lines[index + 1:])  # Write back all but the index line
                    return data
                return None
        except Exception as e:
            print(f'unable to read from file due to exception:\n{e}')
            return None
        return json.loads(oldest_message)

    def write(self, obj):
        object_json = json.dumps(obj, default=vars)
        try:
            with open(self.last_log, 'a') as log_file:
                log_file.write(f'{object_json}\n')
            self.pushed_objects_count += 1
            return obj
        except Exception as e:
            print(f'unable to write due to exception:\n{e}')

    def find_message_in_queue(self, producer_id, sequence_number):
        try:
            with open(self.last_log, 'r') as log_file:
                lines = log_file.readlines()
        except Exception as e:
            print(f'unable to read from file due to exception:\n{e}')
            return None
        logs_json = list(map(lambda x: json.loads(x), lines))
        result = list(filter(lambda x: x['producer_id'] == producer_id and x['sequence_number'] == sequence_number,
         logs_json))
        return result

# todo: move this to the tests


def filemanager_read_write_test_case():
    filemanager = FileManager()
    print(filemanager.read())

    class DummyClass:
        def __init__(self, var1, var2):
            self.var1 = var1
            self.var2 = var2

    filemanager.write(DummyClass(1, 2))
    print(filemanager.read())
    filemanager.write(DummyClass(3, 4))
    filemanager.write(DummyClass(5, 6))
    print(filemanager.read())
    print(filemanager.read())


def filemanager_find_message_in_queue_test_case():
    class Message():
        def __init__(self, producer_id, sequence_number):
            self.producer_id = producer_id
            self.sequence_number = sequence_number

        def serialize(self):
            return {
            'producer_id': self.producer_id,
            'sequence_number': self.sequence_number
            }

    filemanager = FileManager()
    message = Message(1, 1)
    filemanager.write(message)
    filemanager.write(message)
    message.producer_id = 2
    filemanager.write(message)
    message.sequence_number = 2
    filemanager.write(message)
    message.producer_id = 1
    filemanager.write(message)
    print(filemanager.find_message_in_queue(1, 1))

if __name__ == '__main__':
    filemanager_find_message_in_queue_test_case()
