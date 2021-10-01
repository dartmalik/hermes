# TODO
### mqtt
* implement PUBSUB
* implement retained messages
* integrate logging
* implement plug-in system (sends and requests)
    * add auth plug-in
    * add LWT message plug-in
    * add presence plug-in
* integrate configuration
* implement Redis-based stores
* add support for clustering (using remoting and redis)

# concepts
### hermes
- a collection of networks (inter-network) that connects senders with receivers

#### subnet
- a network of senders and receivers
- each receiver must be uniquely identified by a (subnet-id, receiver-id) pair
- senders within one subnet can send messages to receivers in another subnet by specifying the fully-qualified receiver id
- messages can be sent to receivers within the same subnut using the receiver id only (the subnet id is implied to be the local subnet)
- each subnet maintains a dynamic routing table that maps receiver addresses to IP addresses (subnet-id, partition-id) => IP address

# todo
- allow receivers to setup timers and tickers
- determine if parent-child relations should be supported (in a cluster only the root should be partitioned)

### quality
- add functional tests
- add benchmark tests

### optimizations
- improve queue memory mangement
- workers should signal executor after idle timeout (rather than locking and clearing the list)
