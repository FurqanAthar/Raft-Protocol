# Raft-Protocol

## Building a Fault-Tolerant Key/Value Storage System using Raft Protocol

Building a Fault-Tolerant Key/Value Storage System using Raft Protocol was an assignment that I completed as a part of my computer science program. The goal of the assignment was to develop a key-value storage system that can continue to operate in the face of failures, such as server crashes and broken networks, using the Raft protocol. The assignment was divided into three parts:

### Part 1: Implementing Leader Election

In this part, I was responsible for implementing the leader election features of the Raft protocol. This involved understanding the concept of leader election in the Raft protocol and implementing it in the key-value storage system.

### Part 2: Implementing Raft Consensus Algorithm

In this part, I implemented the Raft consensus algorithm as described in Raft’s extended paper, including leader and follower code to append new log entries and the start() API, completing the AppendEntries API with relevant data structures.

### Part 3: Handling Failures

The last part of the assignment was to make the implementation robust to handle failures, such as server crashes. I had to ensure that the key-value storage system could pick up from where it left off before restart and that the system stored state on the persistent storage every time a state update happens.

Through this project, I was able to gain a deeper understanding of the Raft protocol and how it can be used to build a fault-tolerant key-value storage system. This project also helped me improve my skills in concurrent programming and system design.
