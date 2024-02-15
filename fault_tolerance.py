### this test checks fault tolerancy of the system. 
### for each component in the system do the following:
### 1. run the test
### 2. when the pop up popes, bring down an instance of the said component. 
### 3. wait for cluster to become healty again
### 4. press enter

### note that API-Gateway, External Database, ... all are components of the system and are prune to downtime
import random
import time
from typing import List
from threading import Lock

from client.client import Client

TEST_SIZE = 100
KEY_SIZE = 8
SUBSCRIER_COUNT = 4
c = Client('localhost', port=4000, ports=[4000, 4203, 4204, 4205])

key_seq = [random.choice(range(KEY_SIZE)) for _ in range(TEST_SIZE)]

pulled: List[int] = []
lock = Lock()
def store(_: str, val: bytes):
    next_val = int(val.decode("utf-8"))
    with lock:
        pulled.append(next_val)


# for _ in range(SUBSCRIER_COUNT):
#     c2 = Client('localhost', port=4000, ports=[4000])
#     c2.subscribe(store)


for i in range(TEST_SIZE//2):
    # time.sleep(0.5)
    c.push(f"{key_seq[i]}", f"{i}".encode(encoding="utf-8"))

print("manually fail one node and wait for cluster to become healthy again")
print("press enter when cluster is healthy")
input()

for i in range(TEST_SIZE//2,TEST_SIZE):
    c.push(f"{key_seq[i]}", f"{i}".encode(encoding="utf-8"))

for i in range(TEST_SIZE):
    (x, y) = c.pull()
    print(x, y)
    store(x, y)

pulled.sort()
print(pulled)
for i in range(TEST_SIZE):
    if pulled[i]!=i:
        print("DATA loss occurred")


print("Fault tolerance test passed successfully!")
