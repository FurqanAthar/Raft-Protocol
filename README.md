# Raft-Protocol
## Building a Fault-Tolerant Key/Value Storage System using Raft Protocol
This assignment involves building a fault-tolerant key/value storage system using the Raft protocol. The Raft protocol is used to manage replica servers for services that must continue operation in the face of failure (e.g. server crashes, broken or flaky networks). The challenge is that, in the face of these failures, the replicas won't always hold identical data. The Raft protocol helps sort out what the correct data is.

### Part 1: Implementing Leader Election
In this part, the leader election features of the Raft protocol were implemented.

### Part 2: Implementing Raft Consensus Algorithm
The Raft consensus algorithm as described in Raft’s extended paper was implemented, including leader and follower code to append new log entries and the start() API, completing the AppendEntries API with relevant data structures.

### Part 3: Handling Failures
The implementation was made robust to handle failures, such as server crashes, and then restart. The Raft servers were able to pick up from where it left off before restart and the Raft’s implementation stored state on the persistent storage every time a state update happens.
