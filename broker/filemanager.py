import os
import json


class FileManager:
    # TODO add sequence number, count of pushed objects

    def __init__(self):
        self.log_idx = 0 # index of last log file created
        self.last_log = f'{logs/self.log_idx}.log' # path of last log file
        with open(self.last_log, 'w') as log_file:
            log_file.write('')

    def read(self):
        try:
            with open('yourfile.txt', 'r+') as log_file:
                lines = log_file.readlines()
                log_file.seek(0)
                log_file.truncate()
                last_line = log_file.writelines(lines[:-1])
        except Exception as e:
            print(f'unable to read from file due to exception:\n{e}')
        if len(lines) == 0:
            return None
        return json.loads(lines[-1])
        

    def write(self, obj):
        object_json = json.dumps(obj)
        try:
            with open(self.last_log, 'a') as log_file:
                log_file.write(f'\n{object_json}')
        except Exception as e:
            print(f'unable to write due to exception:\n{e}')
            
