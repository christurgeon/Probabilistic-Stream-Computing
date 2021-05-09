from src.newnode import ComputeNode
from src.library import *

if __name__ == "__main__":
    node = None
    try:
        f = finite_uniform_prior_sampler
        i = "127.0.0.1"
        p = 5502
        node = ComputeNode(function=f, ipaddr=i, port=p)
        node.connect_to_sources()
        node.start([100])
    except KeyboardInterrupt:
        print("CTRL-C pressed, exiting...")
        exit(0)
    finally:
        node.kill()