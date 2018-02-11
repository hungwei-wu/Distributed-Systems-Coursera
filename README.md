# Distributed-Systems-Coursera
Programming assignment from coursera distributed system courses.

### Assignment 1:

Implemented failure detection protocol using all-to-all heartbeat membership.

Working on gossip-style heartbeat membership update.

### Assignment 2:

Implemented distributed key-value store with heartbeat failure detection.

#### Features
- By replicate 3 key-value for each key, the system can tolerate up to 2 failures simultaneously happen to one key.

- Replica node is determined by consistent hashing.

- Use quorum (majority of nodes) to determine whether an operation(CRUD) is successfully done or not.

- Implemented stablization protocol, which would rehash all keys in the system when change of nodes (failure or join) is detected by membership protocol.
