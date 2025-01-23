from spiderexpress.types import PlugIn

import pandas as pd

def random_walk_strategy(edges, nodes, state, configuration):
    new_seeds = [1,13]
    sparse_edges = pd.DataFrame(columns=['source', 'target'])
    sparse_nodes = pd.DataFrame()
    new_sampler_state = pd.DataFrame()
    
    return new_seeds, sparse_edges, sparse_nodes, new_sampler_state

random_walk = PlugIn(
    callable=random_walk_strategy,
    tables={"state": {"node_id": "Text"}},
    metadata={},
    default_configuration={},
)
