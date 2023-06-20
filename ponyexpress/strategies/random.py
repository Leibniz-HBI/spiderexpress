"""Random sampling strategy."""

from typing import Any, Dict

import pandas as pd

from ponyexpress.types import PlugIn


def random_strategy(
    edges: pd.DataFrame,
    nodes: pd.DataFrame,
    state: pd.DataFrame,
    configuration: Dict[str, Any],
):
    """Random sampling strategy."""
    # split the edges table into edges _inside_ and _outside_ of the known network
    mask = edges.target.isin(state.node_id)
    edges_inward = edges.loc[mask, :]
    edges_outward = edges.loc[~mask, :]

    # select 10 edges to follow
    if len(edges_outward) < configuration["n"]:
        edges_sampled = edges_outward
    else:
        edges_sampled = edges_outward.sample(n=configuration["n"], replace=False)

    new_seeds = edges_sampled.target  # select target node names as seeds for the
    # next layer
    edges_to_add = pd.concat([edges_inward, edges_sampled])  # add edges inside the
    # known network as well as the sampled edges to the known network
    new_nodes = nodes.loc[nodes.name.isin(new_seeds), :]

    return new_seeds, edges_to_add, new_nodes


random = PlugIn(
    callable=random_strategy,
    tables={"node_": "Text"},
    metadata={},
    default_configuration={"n": 10},
)
