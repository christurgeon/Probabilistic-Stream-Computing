import socket 
import sys
import pickle
import queue
import threading
from tqdm import tqdm
from time import sleep

sys.stdout.flush()

MAX_SIZE_BYTES = 4096
MAX_STREAM_BACKLOG = 5

class ComputeNode():
    """
        ComputeNode needs a thread to handle each of the incoming connections
        and an execution thread to compute and propagate tokens when there are
        available tokens from each of the incoming edges.
    """

    def __init__(self, id, f, ids, ipaddr, port, eos_token):
        self.__id = id
        self.__function = f
        self.__connected_targets = 0
        self.__connected_sources = 0
        self.__EOS_TOKEN = eos_token

        self.__ipaddr = ipaddr
        self.__port = port
        self.__target_node_conn_info = dict.fromkeys(ids)
        self.__target_node_sockets = []
        self.__incoming_queues = dict()

    # add a buffer to store data for this node
    def add_incoming_edge(self, id):
        self.__incoming_queues[id] = queue.Queue()
    
    # add the ip address and port number of a destination node
    def add_target_network_info(self, id, ipaddr, port):
        self.__target_node_conn_info[id] = (ipaddr, port)
    
    # return the ip address and port number of this node
    def get_network_info(self):
        return (self.__ipaddr, self.__port)

    # receives data from a source node, compute
    def __source_node_connection(self, connection, address):
        print("<{}> connected by {}".format(self.__id, address))
        with connection:
            while True:
                data = connection.recv(MAX_SIZE_BYTES)
                if not data:
                    break 

                # unserialize data (id, data), add it to the proper queue for that source node
                token = pickle.loads(data) 
                self.__incoming_queues[token[0]].put(token[1])
                is_eos_token = (token[1] == self.__EOS_TOKEN)

                # if all source node queues have data, compute and send the result
                if self.__necessary_tokens():
                    args = [q.get() for q in self.__incoming_queues.values()]
                    print("<{}> necessary tokens met, starting computation on {}".format(self.__id, args))
                    if not is_eos_token:
                        self.process(args)

                # close all socket connections, break loop
                if is_eos_token:
                    print("<{}> receieved EOS token".format(self.__id))
                    self.propagate_eos_token()
                    break

    # propagate the eos tokens and close connections
    def propagate_eos_token(self):
        token = (self.__id, self.__EOS_TOKEN)
        serialized = pickle.dumps(token)
        for socket in self.__target_node_sockets:
            socket.send(serialized)
            socket.close()

    # setup the node to connect to all target nodes
    def __connect_to_targets(self):
        for conn_tuple in self.__target_node_conn_info.values():
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect_ex(conn_tuple)
            self.__target_node_sockets.append(client_socket)
            print("<{}> trying to connect to {}".format(self.__id, conn_tuple))
            self.__connected_targets += 1
   
    # setup the node to accept connections from source nodes
    def connect_to_sources(self):
        def __connect_to_sources():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.bind((self.__ipaddr, self.__port))
                server_socket.listen(MAX_STREAM_BACKLOG)
                print("<{}> is now listening on port #{}...".format(self.__id, self.__port))
                while self.__connected_sources < len(self.__incoming_queues): 
                    node_connection, address = server_socket.accept()
                    thread = threading.Thread(target=self.__source_node_connection, args=(node_connection, address, ))
                    thread.start()
                    self.__connected_sources += 1
        # if source node, no need to wait for sources to connect
        if len(self.__incoming_queues) > 0: 
            threading.Thread(target=__connect_to_sources).start()

    # if all tokens are available, it will compute then pass the result on
    def process(self, args):
        token = (self.__id, self.__function(*args))
        serialized = pickle.dumps(token)
        for socket in self.__target_node_sockets:
            socket.send(serialized)
            
    # each of the incoming edges must supply a token to process
    def __necessary_tokens(self):
        if self.__connected_sources == 0:
            return True 
        for q in self.__incoming_queues.values():
            if q.empty():
                return False
        return True

    # configures all connections 
    def start(self):
        num_targets = len(self.__target_node_conn_info)
        num_sources = len(self.__incoming_queues)
        print("<{}> is expecting #{} target nodes and #{} source nodes...".format(self.__id, num_targets, num_sources))

        # if sink node, no need to connect to targets
        if num_targets > 0: 
            threading.Thread(target=self.__connect_to_targets).start()

        while self.__connected_targets < num_targets or self.__connected_sources < num_sources:
            sleep(1)
        print("<{}> all nodes connected...".format(self.__id))



###################################################################################################
###################################################################################################


class DistributedEngine:

    def __init__(self, eos_token=None):
        self.__configuration = dict()
        self.__nodes = dict()
        self.__eos_token = eos_token
    

    # add a new node to application graph 
    def add_node(self, id, function, ids, ipaddr, port):
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

    def square(x): return x*x
    def printer(x,y): print("RESULT: cubed={}, squared={}".format(x,y))
    def cube(x): return x*x*x
    d = DistributedEngine(eos_token=-1)

    try:
        LOCALHOST = '127.0.0.1'
        d.add_node(1, square,       [2,3], LOCALHOST, 5502)
        d.add_node(2, cube,         [4], LOCALHOST, 5503)
        d.add_node(3, square,       [4], LOCALHOST, 5504)
        d.add_node(4, printer,      [],  LOCALHOST, 5505)
        d.stream([3, 4, 5]) # 9, 16, 25 -> 81, 256, 625

        sleep(2)
        input("\nPress <ENTER> to quit!")
        print("{} threads remain...".format(threading.active_count()))
    except Exception as e:
        print(e)
