import random

import requests

from client.client import Client
#
# res = requests.get('http://localhost:4000/join')
# print(res.json())

c = Client(host='localhost', port=4000)
for i in range (10):
    c.push('{}'.format(random.random()), 'test2'.encode())
    # print(c.pull())