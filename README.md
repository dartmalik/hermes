### concepts
#### hermes
- a collection of networks (inter-network) that connects senders with receivers

#### subnet
- a collection of senders and receivers
- each receiver must be uniquely identified by a (subnet-id, receiver-id) pair
- senders within one subnet can send messages to receivers in another subnet by specifying the fully-qualified receiver id
- messages can be sent to receivers within the same subnut using the receiver id only (the subnet id is implied to be the local subnet)