"""Random sampling strategy."""

from typing import Any, Dict

import pandas as pd

from spiderexpress.types import PlugIn


def random_strategy(
    edges: pd.DataFrame,
    nodes: pd.DataFrame,
    state: pd.DataFrame,
    configuration: Dict[str, Any],
):
    """Random sampling strategy."""
    # split the edges table into edges _inside_ and _outside_ of the known network
    is_first_round = state.empty
    if is_first_round:
        state = pd.DataFrame({"node_id": edges.source.unique()})
    mask = edges.target.isin(state.node_id)
    edges_outward = edges.loc[~mask, :]

    # select 10 edges to follow
    if len(edges_outward) < configuration["n"]:
        sparse_edges = edges_outward
    else:
        sparse_edges = edges_outward.sample(n=configuration["n"], replace=False)

    new_seeds = (
        sparse_edges.target.unique()
    )  # select target node names as seeds for the
    # next layer
    sparse_nodes = nodes.loc[nodes.name.isin(new_seeds), :]
    new_state = pd.DataFrame({"node_id": new_seeds})
    if is_first_round:
        new_state = pd.concat([state, new_state])
    return new_seeds, sparse_edges, sparse_nodes, new_state


random = PlugIn(
    callable=random_strategy,
    tables={"state": {"node_id": "Text"}},
    metadata={},
    default_configuration={"n": 10},
)
