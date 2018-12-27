# Distributed Systems - Chord Emulator

* This projects is an implementation of the Chord (peer-to-peer) protocol for ditributed hashed tables. The project is a distributed database containing songs. Every node (simulating a computer database) is responsible for a set of key-value pairs with key being the hash of the title of the song and value the song itself.
User can specify how keys are assigned to nodes, and how a node can discover the value for a given key by first locating the node responsible for that key.
* Replicas exists in the project, with song being stored in other nodes except the responsible one

### Build With

* Java 
* Java sockets

