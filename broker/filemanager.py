import os
import json


class FileManager:

    def __init__(self):
        self.log_idx = 0 # index of last log file created
        self.last_log = f'{logs/self.log_idx}.log' # path of last log file
        with open(self.last_log, 'w') as log_file:
            log_file.write('')

    def read(self):
        if len(self.cache[0]) == 0: # nothing to read
            return None 
        message = self.cache.pop()

    def write(self, obj):
        object_json = json.dumps(obj)
        try:
            with open(self.last_log, 'a') as log_file:
                log_file.write(f'\n{object_json}')
        except Exception as e:
            print(f'unable to write due to exception:\n{e}')
            
