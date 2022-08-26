# ponyexpress

A cookiecutter template for SMO/MRML python tools

## Project set up

```
my_project/
|- my_project.sqlite
|- my_project.yml
|- my_projects_seed_file.txt
```

## Configuration

```
db_url: 'sqlite:///test.sqlite'
edge_table_name: 'edge_list'
node_table_name: 'node_list'
connector: 'telegram'
strategy: 'spikyball'
max_iteration: 10000
batch_size: 150
random_wait: True
seeds: # optional, either seeds or seed_file
 - things_to_add
seed_file: 'seeds.txt' # seeds_file takes precedence
```

## Table Schemas

For the

### Nodes

The nodes of the network are kept in two tables that adhere to the same schema:
*sparse_nodes* and *dense_nodes*, where as in the sparse table only sampled nodes are
persisted and the dense table includes all nodes ponyexpress collected in the process.

The following table informs about the minimally necessary columns it will create,
although more meta data can be stored in the table.

| name            | degree                | in_degree                | out_degree               | ...                        |
| --------------- | --------------------- | ------------------------ | ------------------------ | -------------------------- |
| node identifier | number of connections | number of incoming edges | number of outgoing edges | optionally additional data |
|                 |                       |                          |                          |                            |

### Edges

The edges of the network are kept in two tables that adhere to the same schema:
*sparse_edges* and *dense_edges*, where as in the sparse table only sampled edges are
persisted and the dense table includes all edges ponyexpress collected in the process.

The following table informs about the minimally necessary columns it will create,
although more meta data can be stored in the table.

| from                   | to                     | weight                   | ...                        |
| ---------------------- | ---------------------- | ------------------------ | -------------------------- |
| source node identifier | target node identifier | number of parallel edges | optionally additional data |
## Developer Install

- Install poetry
- Clone repository
- In the cloned repository's root directory run poetry install
- Run poetry shell to start development virtualenv
- Run pytest to run all tests


---

[Philipp Kessling](mailto:p.kessling@leibniz-hbi.de) under MIT.
