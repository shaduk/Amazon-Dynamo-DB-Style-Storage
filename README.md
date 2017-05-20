# Amazon-Dynamo-Key-Style-Storage

Implemented a simplified version of Amazon Dynamo DB. There are three main pieces which have been implemented: 1) Partitioning, 2) Replication, and 3) Failure handling.

The main goal is to provide both availability and linearizability at the same time. In other words, it provides read and write operations successfully even under failures. At the same time, a read operation should always return the most recent value. 
