# HERMES
## CONCEPTS
### introduction
- hermes is an overlay network that routes messages between senders and receivers
- hermes implements a single abstraction for writing both concurrent and distributed systems in Go

#### subnet
- a network of senders and receivers
- each receiver must be uniquely identified by a (subnet-id, receiver-id) pair
- senders within one subnet can send messages to receivers in another subnet by specifying the fully-qualified receiver id
- messages can be sent to receivers within the same subnut using the receiver id only (the subnet id is implied to be the local subnet)
- each subnet maintains a dynamic routing table that maps receiver addresses to IP addresses (subnet-id, partition-id) => IP address

</br>

## TODO
- allow receivers to setup timers and tickers
- determine if parent-child relations should be supported (in a cluster only the root should be partitioned)

### quality
- add functional tests
- add benchmark tests

### optimizations
- improve queue memory mangement
- workers should signal executor after idle timeout (rather than locking and clearing the list)

</br>
</br>

# MQTT
## TODO
### features
* feature packages
    * pubsub (core)
    * retained messages
    * lwt
    * websocket server with MQTT packet codecs
* add compatibility tests to ensure conformance with MQTT 3.1.1 OASIS spec
* need transparency into what is happening

### production readiness
* auth
* integrate logging
* integrate configuration
* implement Redis-based stores
* add support for clustering (using remoting and redis)
* implement plug-in system that acts as an external package that integrates into the core (similar to serer, auth, lwt, etc)