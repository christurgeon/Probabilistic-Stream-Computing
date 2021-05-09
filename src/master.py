import json
import pickle
import socket 
import struct
from time import sleep
import signal 
from threading import Thread, Lock


class MasterNode:


    def __init__(self, json_file_path, max_stream_backlog=10):
        self.__data = self.__load_json(json_file_path) 
        self.__max_stream_backlog = max_stream_backlog
        self.__server_connections = []
        self.__client_connections = []
        self.__ready_client_count = 0
        self.__mutex = Lock()

        # signal.signal(signal.SIGINT, self.close)

    
    # returns a loaded json, validates it to ensure it is well formed
    def __load_json(self, json_file_path):
        try:
            with open(json_file_path) as fin:
                data = json.load(fin)
            conn_info = set()
            if data.get("master") is None:
                raise Exception("<master> field is not preent in JSON")
            else:
                self.__ipaddr = data["master"]["ipaddr"]
                self.__port = data["master"]["port"]
                conn_info.add((self.__ipaddr, self.__port))
            if data.get("eos_token") is None:
                raise Exception("<eos_token> field is not present in JSON")
            if data.get("nodes") is None:
                raise Exception("<nodes> field is not present in JSON")
            if len(data.get("nodes")) == 0:
                raise Exception("No node specified in json")
            ids = set()
            for node in data["nodes"]:
                if node.get("id") is None or node.get("targets") is None or node.get("ipaddr") is None or node.get("port") is None:
                    raise Exception("Field missing from JSON nodes dictionary")
                id = node["id"]
                conn_tuple = (node["ipaddr"], node["port"])
                if id in ids:
                    raise Exception("Duplicate <id> found")
                if conn_tuple in conn_info:
                    raise Exception("Duplicate <ipaddr, port> pair found")
                ids.add(id)
                conn_info.add(conn_tuple)
        except Exception as e:
            print("An exception occurred: {}".format(e))
            raise e
        return data


    # propagate the token to all targets and closes connections if specified
    def __send_all(self, token, socket):
        serialized = pickle.dumps(token)
        print("SENDING: {} of length {}".format(token, len(serialized)))
        socket.sendall(struct.pack(">I", len(serialized)))
        socket.sendall(serialized)


    # provides each worker node with necessary info for computation
    def start(self):
        print("<MASTER> preparing to initialize {} nodes...".format(len(self.__data["nodes"])))
        eos_token = self.__data["eos_token"]
        master_connection_tuple = (self.__ipaddr, self.__port)
        for this_node in self.__data["nodes"]:
            node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sources_list = []
            targets_dict = dict()
            for other_node in self.__data["nodes"]:
                if this_node["id"] == other_node["id"]:
                    continue
                if other_node["id"] in this_node["targets"]:
                    targets_dict[other_node["id"]] = (other_node["ipaddr"], other_node["port"])
                if this_node["id"] in other_node["targets"]:
                    sources_list.append(other_node["id"])
            while True:
                try:
                    connection_tuple = (this_node.get("ipaddr"), this_node.get("port"))
                    node_socket.connect(connection_tuple)
                    self.__server_connections.append(node_socket)
                    print("<MASTER> connection successful to {}".format(connection_tuple))
                    self.__send_all(this_node["id"], node_socket)
                    self.__send_all(eos_token, node_socket)
                    self.__send_all(sources_list, node_socket)
                    self.__send_all(targets_dict, node_socket)
                    self.__send_all(master_connection_tuple, node_socket)
                    print("<MASTER> all serialized data sent for this node")
                    break
                except socket.error as e:
                    sleep(0.5)
                    continue
                except Exception as e:
                    print("<MASTER> unexpected error: {}".format(e))
                    raise e

        # once we know that all nodes have gotten the configuration settings, send an OK
        for node_socket in self.__server_connections:
            # RACE CONDITION, 
            self.__send_all("OK", node_socket)
        
        # expect connections from client nodes sending <BEGIN-STREAM> message
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.__ipaddr, self.__port))
        server_socket.listen(self.__max_stream_backlog)
        print("<MASTER> is now listening on port #{}...".format(self.__port))
        connection_requests = 0
        expected_connections = len(self.__server_connections)
        print("expecting {}".format(expected_connections))
        while connection_requests < expected_connections: 
            node_connection, _ = server_socket.accept()
            print("ACCEPTED ONE\n\n")
            self.__client_connections.append(node_connection)
            Thread(target=self.__wait_for_ready_messages, args=(node_connection, )).start()
            connection_requests += 1
        print("<MASTER> all threads configured to handle ready signal")

        # wait for all nodes to send BEGIN-STREAM signal then reply with OKAY
        print("waiting... {} -> {}".format(self.__ready_client_count, expected_connections))
        while self.__ready_client_count < expected_connections:
            print("waiting... {}".format(self.__ready_client_count))
            sleep(1)
        print(self.__ready_client_count, flush=True) ##################
        for node_socket in self.__server_connections:
             self.__send_all("OKAY", node_socket)
        print("<MASTER> all nodes have been sent start signal")

        # start loop to keep master alive
        while True:
            sleep(1)


    # meant to be run on a thread, listens for ready message from nodes
    def __wait_for_ready_messages(self, connection):
        try:
            size = connection.recv(4)
            if not size:
                raise Exception("<MASTER> connection with client node broken!")
            data_size = struct.unpack(">I", size)[0]
            payload = b""
            remaining_payload_size = data_size
            while remaining_payload_size != 0:
                payload += connection.recv(remaining_payload_size)
                remaining_payload_size = data_size - len(payload)
            message = pickle.loads(payload) 
            if message != "BEGIN-STREAM":
                raise Exception("<MASTER> received an unexpected message <{}> but expected ready message <BEGIN-STREAM>".format(message))
            else:
                self.__mutex.acquire()
                self.__ready_client_count += 1
                self.__mutex.release()
        except Exception as e:
            print("Error occurred within this thread: {}".format(e))
   

    # attempt to close each socket connection
    def close(self):
        print("CLOSING MASTER BRO")
        for connection in self.__client_connections:
            try:
                if connection:
                    connection.close()
            except Exception as e:
                print("<MASTER> couldn't close client connection <{}>".format(e))
        for connection in self.__server_connections:
            try:
                if connection:
                    connection.close()
            except Exception as e:
                print("<MASTER> couldn't close server connection <{}>".format(e))



if __name__ == "__main__": 
    master = MasterNode(json_file_path=r"..\config.json", max_stream_backlog=10)
    try:
        master.start()
    except Exception as e:
        print("Excpetion occurred: {}".format(e))
    finally:    
        master.close()