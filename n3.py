from src.node import ComputeNode
from src.library import *

if __name__ == "__main__":
    try:
        data = load_json()
        node = ComputeNode(id=2, json=data, function=cube)

        d = node.get_source_and_target_nodes_dict()
        print("Sources:", d["sources"])
        print("Targets:", d["targets"])

        node.connect_to_sources()
        node.start()

    except KeyboardInterrupt:
        print("CTRL-C pressed, exiting...")
        exit(0)
    finally:
        node.kill()
