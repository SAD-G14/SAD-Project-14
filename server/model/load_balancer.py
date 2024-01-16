class LoadBalancer:
    def __init__(self):
        pass

    def add_node(self, node):
        # use this in case of adding a new node
        pass

    def remove_node(self, node):
        # I hope we don't have to use this
        pass

    def get_node_from_key(self, key):
        # note: when pushing from clients, we can use consistent hashing
        pass

    def get_rr_node(self):
        # note: when pulling from brokers, we can use round robin
        pass