from src.node_with_master import ComputeNode
from src.library import *


def printer(x):
    for i in x:
        print("Output:", i[0], i[1]) 


if __name__ == "__main__":
    node = None
    try:
        node = ComputeNode(function=printer, ipaddr="127.0.0.1", port=5506)
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