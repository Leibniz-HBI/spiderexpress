# Strategy Description

Strategy plug-ins are used to determine which nodes of the network should be the seeds of the next sampling iteration and
which nodes should be visited in order to retrieve new edges and node information.

## Strategy Interface

Each strategy is a pure function that follows the following signature:


```pydocstring
Args:
    edges: the edge table to sample from
    nodes: the nodes table
    state: the last state of the strategy

Returns:
    1. a list of new seed nodes in a list of node names
    2. DataFrame with new edges that needs to be added to the network
    3. DataFrame with new nodes that needs to be added to the network
    4. DataFrame with the new state of the strategy
```

The state table is used to store the state of the strategy between iterations and is initialized with the schema specified in the strategies plug-in definition.
The edge table and the node tables definitions are specified in the project's configuration file.
