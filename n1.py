from src.node_with_master import ComputeNode
from src.library import *


if __name__ == "__main__":
    node = None 
    try:
        node = ComputeNode(function=finite_uniform_prior_sampler, ipaddr="127.0.0.1", port=5502)
        d = node.get_source_and_target_nodes_dict()
        print("Sources:", d["sources"])
        print("Targets:", d["targets"])
        node.start([100])
    except KeyboardInterrupt:
        print("CTRL-C pressed, exiting...")
        exit(0)
    finally:
        if node:
            node.kill()