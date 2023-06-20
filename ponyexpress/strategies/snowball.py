"""Snowball sampling strategy."""

import pandas as pd

from ponyexpress.types import PlugIn


def snowball_strategy(
    edges: pd.DataFrame,
    nodes: pd.DataFrame,
    state: pd.DataFrame,
):
    """Random sampling strategy."""
    # split the edges table into edges _inside_ and _outside_ of the known network
    mask = edges.target.isin(state.node_id)
    edges_inward = edges.loc[mask, :]
    edges_outward = edges.loc[~mask, :]

    edges_sampled = edges_outward.copy()

    new_seeds = (
        edges_sampled.target.unique()
    )  # select target node names as seeds for the

    # next layer
    edges_to_add = pd.concat([edges_inward, edges_sampled])  # add edges inside the
    # known network as well as the sampled edges to the known network
    new_nodes = nodes.loc[nodes.name.isin(new_seeds), :]

    return new_seeds, edges_to_add, new_nodes


snowball = PlugIn(
    callable=snowball_strategy,
    default_configuration=None,
    metadata={},
    tables={
        "node_id": "Text",
    },
)
