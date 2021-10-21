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

## WHY HERMES
- number of goroutines are reduced since there is one goroutine per active receiver (a receiver receiving messages)
- the lifecycle of groutines (receivers) is managed since idle receivers are deactivated and all receivers deactived when closed
- (planned) provide a single abstraction for moving from a concurrent system to a distributed system
- increases the testability of the system since each receiver can be tested independently
- easily implement state machines by allowing receivers to change receiving functions in response to messages
- supports delayed message deliveries
- receivers linearizes messages by processing one message at a time. this is helpful in domains with contention (collaborative domains)
- receivers can be backed by durable state (from a DB), which allows scalling reads (such as ones found in read-modify-write cycles)
- hermes enables applications that require low latency by allowing state to be cached along with application logic

</br>

## TODO
- add support for deleting hermes instance, should wait for all receivers to be deactived and send a signal

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
* logging, need transparency into what is happening
* add support for wildcards

### production readiness
* auth
* integrate logging
* integrate configuration
* implement Redis-based stores
* add support for clustering (using remoting and redis)
* implement plug-in system that acts as an external package that integrates into the core (similar to serer, auth, lwt, etc)