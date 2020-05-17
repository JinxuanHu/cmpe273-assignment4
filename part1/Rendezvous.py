from collections import defaultdict

import murmur3
from server_config import NODES
import hashlib
import mmh3


class RendezvousHash(object):

    def __init__(self, nodes):
        self.nodes = []
        self.nodes = nodes
        self.hash_function = lambda x: murmur3.murmur3_32(x)
    
    def murmur(self, key):
        return mmh3.hash(key)
    
    def weight(self,node, key):
        hash_key = self.murmur(key)
        hash_node = self.murmur(str(node))
        return ((hash_node ^ hash_key)%(2^32))


    def get_node(self,key):
        max_weight = float("-inf")
        max_weight_node = None
        for node in self.nodes:
            nweight = self.weight(node, key)
            if nweight > max_weight:
                max_weight = nweight
                max_weight_node = node
        return max_weight_node
