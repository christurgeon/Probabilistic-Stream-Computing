import socket 
import pickle
import queue
import inspect
import struct
from time import sleep
from threading import Thread, Lock


class NodeSocketsWrapper:

    def __init__(self, max_recv_bytes, max_stream_backlog, ipaddr, port, master_node):
        self.__max_recv_bytes = max_recv_bytes
        self.__max_stream_backlog = max_stream_backlog
        self.__ipaddr = ipaddr
        self.__port = port
        self.__good_status = True
        self.__mutex = Lock()

        # revise and add later
        self.__master = master_node

        self.__connected_sources = []
        self.__connected_target_sockets = []

    # return the ip address and port number of this node
    def get_network_info(self):
        return (self.__ipaddr, self.__port)

    # change the receive byte size for the sockets
    def change_recv_bytes(self, size):
        self.__max_recv_bytes = size

    # retrieve max receieve bytes size
    def max_recv_bytes(self):
        return self.__max_recv_bytes

    # change the max stream backlog connection size
    def change_backlog_size(self, size):
        self.__max_stream_backlog = size

    # returns the number of connected targets
    def targets_count(self):
        try:
            self.__mutex.acquire()
            size = len(self.__connected_target_sockets)
        finally:
            self.__mutex.release()
            return size 

    # returns the number of connected sources
    def sources_count(self):
        return len(self.__connected_sources)

    # given the id of the node and the number of expected connections, it will wait for that many nodes to connect
    def establish_sources_connection(self, node_id, total_connected):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.__ipaddr, self.__port))
            server_socket.listen(self.__max_stream_backlog)
            print("<{}> is now listening on port #{}...".format(node_id, self.__port))
            num_connected = 0
            while num_connected < total_connected: 
                node_connection, address = server_socket.accept()
                self.__connected_sources.append((node_connection, address))
                num_connected += 1
        return num_connected

    # returns a list of all of the connected sources
    def connected_sources(self):
        return self.__connected_sources.copy()

    # loop that blocks until all connections to the targets are made
    def establish_targets_connection(self, node_id, connection_tuples):
        num_connected = 0
        for c in connection_tuples:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            while self.__good_status:
                try:
                    self.__mutex.acquire()
                    client_socket.connect(c)
                    self.__connected_target_sockets.append(client_socket)
                    print("<{}> connection successful to {}".format(node_id, c))
                    num_connected += 1
                    break
                except:
                    sleep(0.5)
                    continue
                finally:
                    self.__mutex.release()
        return num_connected

    # propagate the token to all targets and closes connections if specified
    def send_all(self, token, close_connections=False):
        serialized = pickle.dumps(token)
        print("SENDING: ", token, "len =", len(serialized))
        for socket in self.__connected_target_sockets:
            socket.sendall(struct.pack(">I", len(serialized)))
            socket.sendall(serialized)
            if close_connections:
                socket.close()
        if close_connections:
            self.__connected_target_sockets = None
    
    # breaks out of loops if still within them, then closes connections
    def close(self, node_id):
        print("<{}> sockets wrapper closing".format(node_id))
        self.__good_status = False
        if self.__connected_target_sockets:
            for socket in self.__connected_target_sockets:
                try:
                    socket.close()
                except Exception as e:
                    print("<{}> excpetion while trying to close socket: {}".format(node_id, e))
            self.__connected_target_sockets = None



class ComputeNode:
    """
        ComputeNode needs a thread to handle each of the incoming connections
        and an execution thread to compute and propagate tokens when there are
        available tokens from each of the incoming edges.
    """

    def __init__(self, id, json, function):
        self.__id = id
        self.__function = function
        self.__EOS_TOKEN = json["eos_token"]
        self.__good_status = True
        self.__mutex = Lock()

        data = None
        for element in json["nodes"]:
            if element.get("id") == id:
                data = element 
                break
        if not data:
            raise Exception("Failed to configure node, check JSON data...")

        self.__connections = NodeSocketsWrapper(4096, 5, data["ipaddr"], data["port"], json["master"])
        self.__target_node_conn_info = dict.fromkeys(data["targets"])
        self.__incoming_queues = dict()
        self.__sink_node_aggregator = []

        self.__add_incoming_edges(json["nodes"])
        self.__add_target_network_info(data, json["nodes"])
        self.__is_sink = len(self.__target_node_conn_info.keys()) == 0

        self.__is_generator = inspect.isgeneratorfunction(self.__function)
        if self.__is_sink and self.__is_generator:
            raise Exception("Sink node cannot invoke generator function")
 

    # returns the source and target nodes
    def get_source_and_target_nodes_dict(self):
        ret = {
            "sources" : list(self.__incoming_queues.keys()),
            "targets" : list(self.__target_node_conn_info.keys())
        }
        return ret


    # break out of loops to kill thread
    def kill(self):
        print("<{}> receieved kill signal".format(self.__id))
        self.__good_status = False
        self.__connections.close(self.__id)


    # setup the node to accept connections from source nodes
    def connect_to_sources(self):
        needed_connections = len(self.__incoming_queues)
        print("<{}> expects #{} target(s) and #{} source(s)...".format(self.__id, len(self.__target_node_conn_info), needed_connections))
        if needed_connections > 0: 
            _ = self.__connections.establish_sources_connection(self.__id, needed_connections) 
            for node_connection, address in self.__connections.connected_sources():
                thread = Thread(target=self.__source_node_connection, args=(node_connection, address, ))
                thread.start()
        return True


    # if all tokens are available, it will compute then pass the result on
    def __process(self, args):
        if not self.__is_sink:
            self.__invoke(args)

        # sink node, add results into aggregator for future computation
        else:
            self.__sink_node_aggregator.append(args)


    # invoke the function on the input, check for generator
    def __invoke(self, args):
        if self.__is_generator:
            for i in self.__function(*args):
                token = (self.__id, i)
                self.__connections.send_all(token)
        else:
            token = (self.__id, self.__function(*args))
            self.__connections.send_all(token)


    # propagate the eos tokens and close connections
    def __propagate_eos_token(self):
        token = (self.__id, self.__EOS_TOKEN)
        self.__connections.send_all(token, close_connections=True)


    # configures all connections 
    def start(self, stream=None):
        num_targets = len(self.__target_node_conn_info)
        num_sources = len(self.__incoming_queues)

        # if sink node, no need to connect to targets
        if num_targets > 0: 
            target_nodes = self.__target_node_conn_info.values()
            Thread(target=self.__connections.establish_targets_connection, args=(self.__id, target_nodes)).start()

        # wait for all sources and targets to be connected
        while self.__connections.targets_count() < num_targets or self.__connections.sources_count() < num_sources:
            sleep(1)
        print("<{}> all nodes connected, starting loop...".format(self.__id))

        # if input and a source node, propogate it
        if stream:
            input("Press <ENTER> to start processing!")
            if num_sources > 0:
                raise Exception("<{}> is not a source node".format(self.__id))
            for i in stream:
                self.__process([i])
            self.__propagate_eos_token()

        # no need to wait after sending all the tokens
        else:
            while self.__good_status:
                sleep(1)
        print("<{}> completed".format(self.__id))


    # receives data from a source node, compute
    def __source_node_connection(self, connection, address):
        print("<{}> connected by {}".format(self.__id, address))
        try:
            with connection:
                while self.__good_status:
                    size = connection.recv(4)
                    if not size:
                        print("breaking...")
                        break
                    data_size = struct.unpack(">I", size)[0]
                    payload = b""
                    remaining_payload_size = data_size
                    while remaining_payload_size != 0:
                        payload += connection.recv(remaining_payload_size)
                        remaining_payload_size = data_size - len(payload)
                    token = pickle.loads(payload) 

                    is_eos_token = token[1] == self.__EOS_TOKEN
                    if not is_eos_token:
                        self.__incoming_queues[token[0]].put(token[1]) # synchronized queue

                    # if all source node queues have data, compute and send the result
                    try:
                        self.__mutex.acquire()
                        if self.__necessary_tokens():
                            args = [q.get() for q in self.__incoming_queues.values()]
                            print("<{}> necessary tokens met, starting computation on {}".format(self.__id, args), flush=True)
                            self.__process(args)

                        # close all socket connections, propagate token or perform sink node computation, then break loop
                        if is_eos_token:
                            print("<{}> receieved EOS token".format(self.__id), flush=True)

                            # checking __good_status will ensure we only care about first EOS token
                            if self.__is_sink and self.__good_status:
                                self.__function(self.__sink_node_aggregator)
                            else:
                                self.__propagate_eos_token()
                            self.__good_status = False
                    finally:
                        self.__mutex.release()
        except Exception as e:
            print("Exception: {}".format(e))


    # add a buffer to store data for this node
    def __add_incoming_edges(self, nodes):
        for node in nodes:
            for id in node["targets"]:
                if id == self.__id:
                    self.__incoming_queues[node["id"]] = queue.Queue()
    

    # add the ip address and port number of a destination node
    def __add_target_network_info(self, thisnode, nodes):
        for id in thisnode["targets"]:
            for node in nodes:
                if id == node["id"]:
                    self.__target_node_conn_info[id] = (node["ipaddr"], node["port"])


    # each of the incoming edges must supply a token to process
    def __necessary_tokens(self):
        if self.__connections.sources_count() == 0:
            return True 
        for q in self.__incoming_queues.values():
            if q.empty():
                return False
        return True