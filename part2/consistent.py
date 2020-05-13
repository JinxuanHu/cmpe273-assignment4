
from server_config import NODES
import hashlib
from pickle_hash import hash_code_hex

class ConsistentHash(object):
    def __init__(self,nodes,replicas = 5):
        self.nodes=nodes
        self.replicas=replicas
        self.ring={}
        self._sorted_keys=[]
        self.add_nodes(nodes)
 
    def add_nodes(self,nodes):
        if nodes:
            for node in nodes:
                for i in range(self.replicas):
                    node_bytes="%s_vnode%s" % (node,i)
                    node_bytes = bytes(node_bytes, encoding="utf8")
                    node_hash = hash_code_hex(node_bytes)
                    # node_hash = hashlib.md5(node_bytes).hexdigest()
                    self.ring[node_hash] = node
                    self._sorted_keys.append(node_hash)
                self._sorted_keys.sort()
 
    def get_node(self,key):
        key_bytes=bytes(key,encoding="utf8")
        key_hash = hash_code_hex(key_bytes)
        # keyhash=hashlib.md5(key_bytes).hexdigest()
        index = 0
        for node_hash in self._sorted_keys:
            index += 1
            if key_hash < node_hash:
                return self.ring[node_hash]
            else:
                continue
        if index == len(self._sorted_keys):
            return  self.ring[self._sorted_keys[0]]
 