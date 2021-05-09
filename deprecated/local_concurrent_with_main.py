import json
import os
import sys
import threading
from tqdm import tqdm
from time import sleep
from src.node import ComputeNode


class DistributedEngine:

    def __init__(self, eos_token=None):
        self.__configuration = dict()
        self.__nodes = dict()
        self.__eos_token = eos_token
    

    # add a new node to application graph 
    def submit(self, id, function, ids, ipaddr, port):
        if id in self.__configuration:
            raise Exception("ID {} already specified...".format(id))
        if port < 0 or port > 65535:
            raise Exception("PORT outside of expected range...")
        self.__configuration[id] = ids
        self.__nodes[id] = ComputeNode(id, function, ids, ipaddr, port, self.__eos_token)


    # iterate through all configurations and setup the compute nodes
    def stream(self, input):
        if self.__eos_token is None:
            raise Exception("No EOS token is set!")

        # ensure each node knows who it is communicating with
        stream_source_id = sys.maxsize
        for source_id, target_node_ids in self.__configuration.items():
            print("Node {} sends to {}".format(source_id, target_node_ids))
            if source_id < stream_source_id:
                stream_source_id = source_id
            for target_node_id in target_node_ids:
                ip, port = self.__nodes[target_node_id].get_network_info()
                self.__nodes[source_id].add_target_network_info(target_node_id, ip, port)
                self.__nodes[target_node_id].add_incoming_edge(source_id)

        # validate the constraints
        if len(input) == 0:
            raise Exception("INPUT is empty...")
        if stream_source_id == sys.maxsize:
            raise Exception("No source node could be found...")

        # configure each node to accept new connections
        for node in self.__nodes.values():
            node.connect_to_sources()
        print("\nsleeping to allow nodes to configure\n")
        sleep(5)

        # connect each node to its target nodes
        for node in self.__nodes.values():
            node.start()

        # send all tokens through the stream
        print("Beginning stream with source node <{}>\n".format(stream_source_id))
        for i in tqdm(range(len(input))):
            token = input[i]
            self.__nodes[stream_source_id].process([token])
            sleep(2)
        self.__nodes[stream_source_id].propagate_eos_token()

    # create a copy of the configuration node dictionary
    def get_nodes(self):
        return self.__configuration.copy()


###################################################################################################
###################################################################################################


if __name__ == "__main__":

    # read in the configuration of stream nodes
    config = os.path.join(os.getcwd(), "config.json")
    with open(config) as fin:
        data = json.load(fin)


    d = DistributedEngine(eos_token=-1)

    try:
        LOCALHOST = '127.0.0.1'
        d.submit(1, square,       [2,3], LOCALHOST, 5502)
        d.submit(2, cube,         [4], LOCALHOST, 5503)
        d.submit(3, square,       [4], LOCALHOST, 5504)
        d.submit(4, printer,      [],  LOCALHOST, 5505)
        d.stream([3, 4, 5]) # 9, 16, 25 -> 81, 256, 625

        sleep(2)
        input("\nPress <ENTER> to quit!")
        print("{} threads remain...".format(threading.active_count()))
    except Exception as e:
        print(e)
