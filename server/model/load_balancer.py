from uhashring import HashRing


class LoadBalancer:
    def __init__(self):
        self.node_index = 0
        self.nodes = ['node1']
        self.hr = HashRing(nodes=self.nodes)

    def add_node(self, node):
        self.nodes.append(node)
        self.hr.add_node(node)

    def remove_node(self, node):
        self.hr.remove_node(node)
        self.nodes.remove(node)

    def get_node_from_key(self, key):
        return self.hr.get(key)

    def get_rr_node(self):
        node = self.nodes[self.node_index % len(self.nodes)]
        self.node_index = (self.node_index + 1) % len(self.nodes)
        return node