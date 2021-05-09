from src.node_with_master import ComputeNode
from src.library import *


def square(x): 
    return x*x


if __name__ == "__main__":
    node = None  
    try:
        node = ComputeNode(function=square, ipaddr="127.0.0.1", port=5503)
        d = node.get_source_and_target_nodes_dict()
        print("Sources:", d["sources"])
        print("Targets:", d["targets"])
        node.start()
    except KeyboardInterrupt:
        print("CTRL-C pressed, exiting...")
        exit(0)
    finally:
        if node:
            node.kill()
