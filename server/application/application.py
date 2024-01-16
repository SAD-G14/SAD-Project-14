from server.model.load_balancer import LoadBalancer

lb = LoadBalancer()


class Application:
    def __init__(self):
        self.nodes = dict()
        pass

    def pull(self):
        for i in range(len(self.nodes)):
            # we need the loop for the case that some brokers are empty
            node = self.nodes[lb.get_rr_node()]
            data = node.pull()
            if data:
                return data
        return data

    def push(self, data):
        key = data.key
        node = self.nodes[lb.get_node_from_key(key)]
        return node.push(data)
