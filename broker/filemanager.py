import os
import json


class FileManager:

    def __init__(self):
        self.pushed_objects_count = 0
        self.log_idx = 0 # index of last log file created
        self.log_dir = 'logs'
        os.makedirs(self.log_dir, exist_ok=True)
        self.last_log = os.path.join(self.log_dir, f'{self.log_idx}.log') # path of last log file
        with open(self.last_log, 'w') as log_file:
            log_file.write('')

    def read(self):
        try:
            with open(self.last_log, 'r+') as log_file:
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
        object_json = json.dumps(obj, default=vars)
        try:
            with open(self.last_log, 'a') as log_file:
                log_file.write(f'\n{object_json}')
            self.pushed_objects_count += 1
            return obj
        except Exception as e:
            print(f'unable to write due to exception:\n{e}')
            

# todo: move this to the tests
def test_case():
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

