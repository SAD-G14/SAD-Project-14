import requests

from server.model.load_balancer import LoadBalancer

lb = LoadBalancer()


class Application:
    def __init__(self):
        self.nodes = list()
        self.number_of_brokers = 0
        pass

    def pull(self, data):
        node = self.nodes[lb.get_node_from_key(data['producer_id'])*2]
        res = requests.post('http://{}:5000/queue/pull'.format(node['ip']), json=data)
        return res.json()

    def push(self, data):
        node = self.nodes[lb.get_rr_node()*2]
        res = requests.post('http://{}:5000/queue/push'.format(node['ip']), json=data)
        return res.json()

    def ack(self, data):
        node = self.nodes[lb.get_node_from_key(data['producer_id'])*2]
        res = requests.post('http://{}:5000/queue/ack'.format(node['ip']), json=data)
        return res.json()

    def join(self, address):
        partition = int(self.number_of_brokers / 2)
        replica = self.number_of_brokers % 2
        if replica == 0:
            lb.add_node(partition)
        self.number_of_brokers += 1
        broker = {'ip': address, 'partition': partition, 'replica': replica}
        self.nodes.append(broker)
        if replica > 0:
            requests.post('http://{}:5000/broker/replica'.format(self.nodes[partition * 2]['ip']), json=broker)
        return broker
