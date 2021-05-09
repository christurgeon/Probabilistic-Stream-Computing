# Library

This repository is built to demonstrate the speedup attained for probabilistic programming via distributed stream computing. The library includes a file named ```library.py``` which features several rudimentary functions to help with distribution creation and a brute force Bayesian inference method. One can also see a collection of sampling and observe methods which are fundamental to probabilistic programming. 

The bread and butter of the library is the stream computing engine. It contains a master node built within ```master.py```. This node can be spun up and supplied a JSON file of the expected stream configuration. Then, compute nodes are kicked off and communicate with the master node to know which other nodes connect to it and which nodes it connects to. Once all of the connections are made, the master node sends the source nodes the okay signal. Note that all connections and data are sent through TCP using sockets.

The stream computing paradigm is Synchronous Dataflow _-like_ in that each compute node must receieve a data token from each input node before it can compute and pass along the result. Additionally, a sink node will aggregate all results then be allowed to compute on the data once all results are accumulated. There is no on-the-fly computation (yet) for sink nodes. 

## Example

You can find an example which can be run by opening ```n1.py, n2.py, n3.py``` and ```n4.py``` in separate Python runtimes. They must be supplied a port number, IP address and function to compute. You must then kick off the master node and supply it the path to the JSON configuration file. An example JSON file can be observed in ```config.json```. It expects an end-of-stream (EOS) token to gracefully disconnect nodes when all source node data is streamed. It also is configured with the connection information for all of the nodes including the master node, an ID of each node, and the IDs of the nodes a given node sends data to.

In the example, 100 values from a distribution are yielded to nodes 2 and 3, which square and cube the values respectively, then node 4 collects all results and prints the squared and cubed values. 

In practice, the stream can be used to run multiple trials in parallel for a probabilistic program or split up distribution yielding and sampling. Also, the user may utilize probabilistic programming methods from our library or write their own to use with the stream computing engine. 

