# CRDT experiment
This is a simple experiment to test how to implement CRDTs. Conflict-free Replicated Data Type are objects able to work under Strong Eventual Consistency thanks to commuting operations. This is useful for applications that need to work offline or need to synchronize data between multiple devices.

Rely on graphviz for rendering: ```sudo apt install graphviz```.
You may need to create a display directory to contain graphs.

## Framework
We use a DAG to represent the causal order of operations. Each node in the DAG is label with an operation. The edges represent the order of operations. Each process has its own copy of the DAG. To apply an operation to the CRDT, a process appends a new vertex to its DAG and label it with the operation. It also adds an edge to each "head" of the DAG, which are the vertices that are not the target of any edge (leaves). Each CRDT is also defined with a reconciliation function that is used to compute a total order of operation to execute to get the current state of the CRDT.

## Branches
- networking: network features (remote)
- docker-experiments: run multi-processes experiments in docker (local but use networking)
- threads-experiments: run experiments with multiple threads simulating processes (local)


## TODO
- dont rollback conflicting op just order them
- complete tests