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

## Extending Ponyexpress

`Ponyexpress` is extensible via plug-ins and sports two `setuptools`entry points to register plug-ins with:

- `ponyexpress.connectors` under which a connector may be registered, i.e. a program that retrieves and returns *new* data from a data source.
- `ponyexpress.strategies` under which sampling strategies may be registered.

Further below we lay out the restrictions both kinds of plug-ins have to adhere to and how further configuration in the above described project file format is passed into the plug-ins.

### Connector Specification

The idea of a `Connector` is to deliver *new* information of the network to be explored. The function takes a `List[str]` which is a list of node names for which we need information about and it returns two dataframes, the edges and the node information. 

All Connectors must implement the following function interface:

```python
Connector = Callable[[List[str], Dict[str, Any]], tuple[pd.DataFrame, pd.DataFrame]]
# Connector(node_names: List[str]) -> DataFrame, DataFrame
```

```python
def csv(
    node_ids: List[str], configuration: Dict[str, Any]
) -> (pd.DataFrame, pd.DataFrame):
    """The CSV connector!"""
    edges = pd.read_csv(configuration["edge_list_location"], dtype=str)
    nodes = (
        pd.read_csv(configuration["node_list_location"], dtype=str)
        if configuration.node_list_location
        else None
    )
    mode = configuration["mode"]
    if mode == "in":
        mask = edges["target"].isin(node_ids)
    elif mode:
        mask = edges["source"].isin(node_ids)
    elif mode:
        mask = edges["target"].isin(node_ids) | edges["source"].isin(node_ids)
    else:
        raise ValueError(f"{configuration.mode} is not one of 'in', 'out' or 'both'.")

    # Filter edges that contain our input nodes
    edge_return: pd.DataFrame = edges.loc[mask]

    node_return = None
    if nodes is not None:
        new_nodes = edge_return.target.unique().tolist()
        node_return = nodes.loc[nodes.name.isin(new_nodes)]

    return edge_return, node_return if node_return is not None else pd.DataFrame()
```

Importantly, `connectors` are allowed to return as many new edges as it finds. But it **must** restrict node
information to rows regarding the requested nodes. Thus, if we request information on one node it should
return a node dataframe with exactly one row.

### Strategy Specification

```python
Strategy = Callable[[pd.DataFrame, pd.DataFrame, list[str], Dict[str, Any]], Tuple[list[str], pd.DataFrame, pd.DataFrame]]
# Strategy(edges: DataFrame, nodes: DataFrame, known_nodes: List[str], configuration: Dict[str, Any]) -> List[str], DataFrame, DataFrame
```

Where the returns are the following:

- `List[str]` is a list of the new **seed nodes** for the next iteration,
- `DataFrame` is the table of new **edges** to be added to the network,
- `DataFrame` is the table of new **nodes** to be added to the network.

### Additional Parameters and Configurability

Any additional parameters stated in the configuration file will be passed into the function as well. E.g. if the configuration file states:

```yaml
strategy:
  spikyball:
    layer_max_size: 150
```

The resulting dictionary will be passed into the function call:

```python
Strategy(edges, nodes, known_nodes, {"layer_max_size" = 150})
```

### Example 1: a sweet and simple strategy

To futher illustrate the process we consider a implementation of random sampling,
here our strategy is to select a configureable number of random nodes for each layer:

```python

def random(edges: pd.DataFrame, nodes: pd.DataFrame, known_nodes: List[str], configuration: Dict[str, int]):
    """Draws a random sample of size $$n$$ from the given edges.
    params:
        edges: pd.dataFrame : edges to be sampled, expected to be simple, weighted
        nodes: pd.DataFrame : node information
        known_nodes : List[str] : already visited nodes
        config: Dict[str, int] : configuration for this sampler

    returns
        new_seeds : List[str] : the next iteration's seeds
        edges : pd.DataFrame : the sampled edges
        new_nodes : pd.DataFrame : sampled nodes
    """
    number_of_nodes: int = configuration["n"]

    # split the edges table into edges _inside_ and _outside_ of the known network
    mask: pd.Series = edges.target.isin(known_nodes)
    edges_inward: pd.DataFrame = edges.loc[mask, :]
    edges_outward: pd.DataFrame = edges.loc[~mask, :]

    # select $$n$$ edges to follow
    if len(edges_outward.index) < number_of_nodes:
        edges_sampled = edges_outward
    else:
        edges_sampled = edges_outward.sample(n=number_of_nodes, replace=False)

    new_seeds = edges_sampled.target.unique().tolist()  # select target node names as seeds for the
    # next layer

    # known network as well as the sampled edges to the known network
    edges_to_add = pd.concat([edges_inward, edges_sampled])  # add edges inside the

    return new_seeds, edges_to_add, edges_outward

```

To register it with `ponyexpress` add an entry to your `pyproject.toml` (for further information on entrypoints and poetry):

```toml
[tool.poetry.plugins."ponyexpress.strategies"]
random = "my_ponyexpress_package:random_sampler"
```

Finally, we can create a new proejct configuration in which we can include the new sampler:

```yaml
strategy:
  random:
    n: 10
```

## Developer Install

- Install poetry,
- Clone repository,
- In the cloned repository's root directory run `poetry install`,
- Run `poetry shell` to start development virtualenv,
- Run `pytest` to run all tests.

---

2023, [Philipp Kessling](mailto:p.kessling@leibniz-hbi.de) under the MIT license.
