from collections import defaultdict

import murmur3
from server_config import NODES


class RendezvousHash(object):

    def __init__(self, nodes):
        self.nodes = []
        self.nodes = nodes
        self.hash_function = lambda x: murmur3.murmur3_32(x)

    def get_node(self, key):
        max_weight = float("-inf")
        max_weight_node = None
        for node in self.nodes:
            weight = self.hash_function("%s:%s" % (str(node), key))
            print(weight)
            if weight > max_weight:
                max_weight = weight
                max_weight_node = node

            elif weight == max_weight:
                max_weight = weight
                max_weight_node = max(str(node), str(winner))
        return max_weight_node

def test():
    ring = Rendezvous(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))

if __name__ == "__main__":  
    test()