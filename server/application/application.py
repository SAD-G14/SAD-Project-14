import json
import logging
from time import time
from prometheus_client import Counter, Histogram
import requests
import threading

from server.model.load_balancer import LoadBalancer

COUNTER = Counter(name='method_calls', documentation='Number of method calls', labelnames=['node', 'method'])
HISTOGRAM = Histogram(name='method_latency', documentation='latency of methods', labelnames=['node', 'method'])


def load_file():
    try:
        with open('ip_table', 'r') as file:
            return json.load(file)
    except FileNotFoundError as e:
        logging.error("ip_table file not found: {}".format(e))
        return []

class Application:
    def __init__(self):
        self.nodes = load_file()
        logging.info("nodes: {}".format(self.nodes))
        self.number_of_brokers = len(self.nodes)
        self.lock = threading.Lock()
        self.lb = LoadBalancer(self.nodes)
        pass

    def pull(self, data):
        node_index = self.lb.get_rr_node()*2
        return self.send_request_to_broker(node_index, 'pull', data)

    def push(self, data):
        node_index = self.lb.get_node_from_key(data['key'])*2
        return self.send_request_to_broker(node_index, 'push', data)

    def ack(self, data):
        node_index = self.lb.get_node_from_key(data['key'])*2
        return self.send_request_to_broker(node_index, 'ack', data)

    def join(self, address):
        try:
            self.lock.acquire()
            partition = int(self.number_of_brokers / 2)
            replica = self.number_of_brokers % 2
            if replica == 0:
                self.lb.add_node(partition)
            self.number_of_brokers += 1
            # todo: in case the gateway restarts, it needs to read this list from a file at startup
            broker = {'ip': address, 'partition': partition, 'replica': replica}
            self.nodes.append(broker)
            with open('ip_table', 'w') as file:
                json.dump(self.nodes, file)
            if replica > 0:
                requests.post('http://{}:5000/broker/replica'.format(self.nodes[partition * 2]['ip']), json=broker)
            return broker
        except Exception as e:
            logging.error("error joining the network: {}".format(e))
            return None
        finally:
            self.lock.release()

    def send_request_to_broker(self, node_index: int, method: str, data):
        COUNTER.labels('{}'.format(int(node_index/2)), method).inc()
        start_time = time()
        node = self.nodes[node_index]
        try:
            response = requests.post('http://{}:5000/queue/{}'.format(node['ip'], method), json=data).json()
            HISTOGRAM.labels('{}'.format(int(node_index/2)), method).observe(time() - start_time)
            return response
        except requests.exceptions.RequestException as e:
            # pass
            logging.warning("partition leader is unreachable, promoting replica to leader")
            self.promote_replica(node_index)
            return self.send_request_to_broker(node_index, method, data)

    def promote_replica(self, node_index):
        self.nodes[node_index] = self.nodes[node_index + 1]
        self.nodes[node_index]['replica'] = 0