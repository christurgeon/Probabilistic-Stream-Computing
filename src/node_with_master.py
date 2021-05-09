import socket 
import pickle
import queue
import inspect
import struct
from time import sleep
from threading import Thread, Lock



class NodeSocketsWrapper:

    def __init__(self, max_recv_bytes, max_stream_backlog, ipaddr, port):
        self.__max_recv_bytes = max_recv_bytes
        self.__max_stream_backlog = max_stream_backlog
        self.__ipaddr = ipaddr
        self.__port = port
        self.__good_status = True
        self.__mutex = Lock()

        self.__server_socket = None
        self.__master_connection_tuple = None
        self.__master_connection_as_server = None
        self.__master_connection_as_client = None
        self.__connected_sources = []
        self.__connected_target_sockets = []


    # configure with master
    def configure_master(self):
        """
            1. id = _
            2. eos_token = _
            3. sources_list = [id, id, id, ...]
            4. targets_dict = {
                  id : (ipaddr(string), port(int)),
                  id : (ipaddr(string), port(int)),
                  ... 
               }
            5. master_connection = (ipaddr(string), port(int))   
        """
        self.__server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__server_socket.bind((self.__ipaddr, self.__port))
        self.__server_socket.listen(self.__max_stream_backlog)
        self.__master_connection_as_server, _ = self.__server_socket.accept()
        id = self.__receive_from_master()
        eos_token = self.__receive_from_master()
        sources_list = self.__receive_from_master()
        targets_dict = self.__receive_from_master()
        self.__master_connection_tuple = self.__receive_from_master()
        _ = self.__receive_from_master() # okay signal
        print("Successfully configured with master!")
        return id, eos_token, sources_list, targets_dict


    # receiver helper to take in input from master node
    def __receive_from_master(self):
        size = self.__master_connection_as_server.recv(4)
        if not size:
            raise Exception("Connection with master node broken!")
        data_size = struct.unpack(">I", size)[0]
        payload = b""
        remaining_payload_size = data_size
        while remaining_payload_size != 0:
            payload += self.__master_connection_as_server.recv(remaining_payload_size)
            remaining_payload_size = data_size - len(payload)
        print("RECEIEVED {}".format(pickle.loads(payload)))
        return pickle.loads(payload) 


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


    # returns the connected sources
    def connected_sources(self):
        return self.__connected_sources


    # given the id of the node and the number of expected connections, it will wait for that many nodes to connect
    def establish_sources_connection(self, node_id, total_connected):
        print("<{}> is now listening on ('{}', {})".format(node_id, self.__ipaddr, self.__port))
        num_connected = 0
        while num_connected < total_connected: 
            node_connection, address = self.__server_socket.accept()
            self.__connected_sources.append((node_connection, address))
            num_connected += 1
        return num_connected


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
                except socket.error:
                    sleep(0.5)
                    continue
                except Exception as e:
                    print("An unexpected error occurred: {}".format(e))
                    raise e
                finally:
                    self.__mutex.release()
        return num_connected


    # propagate the token to all targets and closes connections if specified
    def send_all(self, token, close_connections=False):
        serialized = pickle.dumps(token)
        print("SENDING: {} of length {}".format(token, len(serialized)))
        for socket in self.__connected_target_sockets:
            socket.sendall(struct.pack(">I", len(serialized)))
            socket.sendall(serialized)
            if close_connections:
                socket.close()
        if close_connections:
            self.__connected_target_sockets = None
    

    # tells the master node that this node is ready, then stalls until OKAY signal is sent back
    def send_ready_to_master(self, node_id): 
        print("<{}> sending ready signal to master".format(node_id))
        if self.__master_connection_tuple is None or self.__master_connection_as_server is None:
            raise Exception("<{}> connection to master has not been established, ensure configure_master() is called first".format(node_id))
        while True:
            self.__master_connection_as_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.__master_connection_as_client.connect(self.__master_connection_tuple)
                serialized = pickle.dumps("BEGIN-STREAM")
                self.__master_connection_as_client.sendall(struct.pack(">I", len(serialized)))
                self.__master_connection_as_client.sendall(serialized)
                break
            except socket.error as msg:
                print("failed: {}".format(msg))
                sleep(0.5)
                continue
            except Exception as e:
                print("An unexpected error occurred: {}".format(e))
        print("<{}> sent ready signal to master".format(node_id))

        # expect to recieve back the OKAY signal to begin, this is meant to block sources from sending too early
        start_signal = self.__receive_from_master()
        if start_signal != "OKAY":
            raise Exception("<{}> unexpected message receieved from master: <{}>".format(node_id, start_signal))
        print("<{}> received begin signal from master node".format(node_id))
        

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
        for client_connection, _ in self.__connected_sources:
            try:
                client_connection.close()
            except:
                continue
        if self.__master_connection_as_server:
            self.__master_connection_as_server.close()
        if self.__master_connection_as_client:
            self.__master_connection_as_client.close()



class ComputeNode:
    """
        ComputeNode needs a thread to handle each of the incoming connections
        and an execution thread to compute and propagate tokens when there are
        available tokens from each of the incoming edges.
    """

    def __init__(self, function, ipaddr, port):
        self.__function = function
        self.__is_generator = inspect.isgeneratorfunction(self.__function)
        self.__good_status = True
        self.__mutex = Lock()

        self.__incoming_queues = dict()
        self.__target_node_conn_info = dict()
        self.__sink_node_aggregator = []
        self.__connections = NodeSocketsWrapper(4096, 5, ipaddr, port)
        self.__handle_config()

 

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
    def __connect_to_sources(self):
        needed_connections = len(self.__incoming_queues)
        print("<{}> expects #{} target(s) and #{} source(s)".format(self.__id, len(self.__target_node_conn_info), needed_connections))
        if needed_connections > 0: 
            _ = self.__connections.establish_sources_connection(self.__id, needed_connections) 
            for node_connection, address in self.__connections.connected_sources():
                thread = Thread(target=self.__source_node_connection, args=(node_connection, address, ))
                thread.start()
        return True


    # handle the configuration determined by the master node
    def __handle_config(self):
        print("NODE STARTING CONNECTION TO MASTER\n\twill be blocked on recv()")
        self.__id, self.__EOS_TOKEN, sources_list, targets_dict = self.__connections.configure_master()
        self.__target_node_conn_info = targets_dict.copy()
        self.__incoming_queues = dict.fromkeys(sources_list, queue.Queue())
        self.__is_sink = len(self.__target_node_conn_info) == 0
        if self.__is_sink and self.__is_generator:
            raise Exception("Sink node cannot invoke generator function")


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

        # A non-source node should not be configured with input data
        if stream and num_sources > 0:
            raise Exception("<{}> is not a source node".format(self.__id))

        # if sink node, no need to connect to targets
        if num_targets > 0: 
            target_nodes = self.__target_node_conn_info.values()
            Thread(target=self.__connections.establish_targets_connection, args=(self.__id, target_nodes)).start()

        # connect to source nodes, if any
        self.__connect_to_sources()

        # wait for all sources and targets to be connected
        while self.__connections.targets_count() < num_targets or self.__connections.sources_count() < num_sources:
            sleep(1)

        # receieve the OKAY signal from the master node 
        self.__connections.send_ready_to_master(self.__id)

        # if input and a source node, begin propagating data
        if stream:
            print("<{}> beginning to stream values...".format(self.__id))
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


    # each of the incoming edges must supply a token to process
    def __necessary_tokens(self):
        if self.__connections.sources_count() == 0:
            return True 
        for q in self.__incoming_queues.values():
            if q.empty():
                return False
        return True