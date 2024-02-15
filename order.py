### this test checks if the order garantee holds, i.e. if (k1,v1) is pushed before (k1,v2)
### then it is read before (k1,v2)

import random
import sys
from typing import Dict, List

from client.client import Client

c = Client('localhost', port=4000)

TEST_SIZE = 100000
KEY_SIZE = 8
SUBSCRIER_COUNT = 4
VIOLATION_COUNT = 0

key_seq = [random.choice(range(KEY_SIZE)) for _ in range(TEST_SIZE)]

pulled: Dict[str, List[int]] = {}
for i in range(KEY_SIZE):
    pulled[f"{i}"] = []

def validate_pull(key: str, val: bytes):
    next_val = int(val.decode("utf-8"))
    if len(pulled[key]) != 0:
        prev_val = pulled[key][-1]
        if prev_val >= next_val:
            global VIOLATION_COUNT
            VIOLATION_COUNT += 1
            print(f"order violation, seq: [{prev_val}, {next_val}]\tkey: [{key}]")
            # sys.exit(255)
    pulled[key].append(next_val)


for _ in range(SUBSCRIER_COUNT):
    c.subscribe(validate_pull)

for i in range(TEST_SIZE):
    c.push(f"{key_seq[i]}", f"{i}".encode(encoding="utf-8"))

for i in range(TEST_SIZE):
    print(c.pull())

print("order test passed successfully!")