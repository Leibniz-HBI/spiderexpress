# ponyexpress

A multi-purpose network sampling tool.

## Project set up

A `ponyexpress` project will need the following files in place in the project directory (here exemplary named `my_project`), whereas the SQLITE-database will be created if it not exists.

```tree
my_project/
|- my_project.sqlite
|- my_project.pe.yml
|- seed_file.txt
```

Whereas `my_project.sqlite` is the resulting database, `my_project.pe.yml` is the project's configuration in which a data source and sampling strategy and other parameters may be specified (see [Configuration](#configuration) for further details). `seed_file.txt` is a text file which contains one node name per line.

## Configuration

`Ponyexpress` utilizes YAML de-/serialization for it's configuration file. As such, initializing a project is as easy as: running `$ ponyexpress create` and a pleasureable and comforting dialogue prompt will guide you through the process.

The resulting file could look like something like this example:

```yaml
project_name: spider
batch_size: 150
db_url: test2.sqlite
max_iteration: 10000
edge_table_name: edge_list
node_table_name: node_list
seeds:
  - ...
connector: telegram
strategy:
  spikyball:
    layer_max_size: 150
    sampler:
      source_node_probability:
        coefficient: 1
        weights:
          subscriber_count: 4
          videos_count: 1
      target_node_probability:
        coefficient: 1
        weights:
      edge_probability:
        coefficient: 1
        weights:
          views: 1

```

## Table Schemas

### Nodes

The nodes of the network are kept in two tables that adhere to the same schema:
*sparse_nodes* and *dense_nodes*, where as in the sparse table only sampled nodes are
persisted and the dense table includes all nodes ponyexpress collected in the process.

The following table informs about the minimally necessary columns it will create,
although more meta data can be stored in the table.

| Column Name | Description                                         |
| ----------- | --------------------------------------------------- |
| name        | node identifier                                     |
| degree      | node's degree                                       |
| in_degree   | node's in degree                                    |
| out_degree  | node's out degree                                   |
| ...         | optionally additional data coming from the connector |

### Edges

The edges of the network are kept in two tables that adhere to the same schema:
*sparse_edges* and *dense_edges*, where as in the sparse table only sampled edges are
persisted and the dense table includes all edges ponyexpress collected in the process.

The following table informs about the minimally necessary columns it will create, although more meta data can be stored in the table.

| Column Name | Description                                 |
| ----------- | ------------------------------------------- |
| source      | source node name                            |
| target      | target node name                            |
| weight      | number of multi-edges between the two nodes |

## Included Connectors

### CSV connector

This connector reads network from CSV files, one for the edges and, optionally, one for node information.
The tables must follow the above stated column names for both kinds of tables.
Required configuration are the following key-value-pairs:

| Key                | Description                                                   |
|--------------------|---------------------------------------------------------------|
| edge_list_location | relative or absolute path to the edge CSV-file.               |
| node_list_location | relative or absolute path to the edge CSV-file.               |
| mode               | either "in", "out" or "both", determines which edges to emit. |


Information must be given in the `ponyexpress`-project configuration file, e.g. consider the following configuration snippet:

```yaml
connector:
  csv:
    edge_list_location: path/to/file.csv
    node_list_location: path/to/file.csv
    mode: out
```

> [!info]
> In the current implementation the tables are reread on each call of the connector, thus,
> loading large networks will lead to long loading times.

## Extending Ponyexpress

`Ponyexpress` is extensible via plug-ins and sports two `setuptools`entry points to register plug-ins with:

- `ponyexpress.connectors` under which a connector may be registered, i.e. a program that retrieves and returns *new* data from a data source.
- `ponyexpress.strategies` under which sampling strategies may be registered.

### Connector Specification

The idea of a `Connector` is to deliver *new* information of the network to be explored. The function takes a `List[str]` which is a list of node names for which we need information about and it returns two dataframes, the edges and the node information.
All Connectors must implement the following function interface:

```python
Connector = Callable[[list[str]], tuple[pd.DataFrame, pd.DataFrame]]
# Connector(node_names: List[str]) -> DataFrame, DataFrame
```

### Strategy Specification

```python
Strategy = Callable[[pd.DataFrame, pd.DataFrame, list[str]], Tuple[list[str], pd.DataFrame, pd.DataFrame]]
# Strategy(edges: DataFrame, nodes: DataFrame, known_nodes: List[str]) -> List[str], DataFrame, DataFrame
```

Where the returns are the following:

- `List[str]` is a list of the new **seed nodes** for the next iteration,
- `DataFrame` is the table of new **edges** to be added to the network,
- `DataFrame` is the table of new **nodes** to be added to the network.

### Additional Parameters and Configurability

The registered plug-ins must follow the below stated function interfaces. Although any additional parameters stated in the configuration file will be passed into the function as well.

E.g. if a configuration file states:

```yaml
strategy:
    layer_max_size: 15
```

will result in the following function call `Strategy(edges, nodes, known_nodes, layer_max_size = 15)`.

### Example 1: a sweet and simple strategy

To futher illustrate the process we consider a implementation of random sampling,
here our strategy is to select 10 random nodes for each layer:

```python

def random_sampler(edges: DataFrame, nodes: DataFrame, known_nodes: List[str]):
  # split the edges table into edges _inside_ and _outside_ of the known network
  mask = edges.target.isin(known_nodes)
  edges_inward  = edges.loc[mask,:]
  edges_outward = edges.loc[~mask, :]
  # select 10 edges to follow
  edges_sampled = edges_outward.sample(n=10, replace=False)

  new_seeds = edges_sampled.target  # select target node names as seeds for the
  # next layer
  edges_to_add = pd.concat(edges_inward, edges_sampled)  # add edges inside the
  # known network as well as the sampled edges to the known network
  new_nodes = nodes.loc[nodes.name.isin(new_seeds), :]
  return new_seeds, edges_to_add, new_nodes
```

## Developer Install

- Install poetry
- Clone repository
- In the cloned repository's root directory run poetry install
- Run poetry shell to start development virtualenv
- Run pytest to run all tests

---

2022, [Philipp Kessling](mailto:p.kessling@leibniz-hbi.de) under the MIT license.
