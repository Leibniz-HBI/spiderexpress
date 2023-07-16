"""A CSV-reading, network-rippin' connector for your testing purposes."""
import dataclasses
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from ponyexpress.types import PlugIn, fromdict

_cache = {}


@dataclasses.dataclass
class CSVConnectorConfiguration:
    """Configuration items for the csv_connector."""

    edge_list_location: str
    mode: str
    node_list_location: Optional[str] = None
    return_neighbour_info: Optional[bool] = None
    cache: bool = True


def _cache_or_not(configuration: CSVConnectorConfiguration):
    if configuration.cache:
        if configuration.edge_list_location not in _cache:
            _cache[configuration.edge_list_location] = pd.read_csv(
                configuration.edge_list_location, dtype=str
            )
        edges = _cache[configuration.edge_list_location].copy()
        if configuration.node_list_location:
            if configuration.node_list_location not in _cache:
                _cache[configuration.node_list_location] = pd.read_csv(
                    configuration.node_list_location, dtype=str
                )
            nodes = _cache[configuration.node_list_location].copy()
        else:
            nodes = None
    else:
        edges = pd.read_csv(configuration.edge_list_location, dtype=str)
        nodes = (
            pd.read_csv(configuration.node_list_location, dtype=str)
            if configuration.node_list_location
            else None
        )

    return edges, nodes


def csv_connector(
    node_ids: List[str], configuration: Union[Dict, CSVConnectorConfiguration]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """The CSV connector!"""
    if isinstance(configuration, dict):
        configuration = fromdict(CSVConnectorConfiguration, configuration)

    edges, nodes = _cache_or_not(configuration)

    if configuration.mode == "in":
        mask = edges["target"].isin(node_ids)
    elif configuration.mode == "out":
        mask = edges["source"].isin(node_ids)
    elif configuration.mode == "both":
        mask = edges["target"].isin(node_ids) | edges["source"].isin(node_ids)
    else:
        raise ValueError(f"{configuration.mode} is not one of 'in', 'out' or 'both'.")

    # Filter edges that contain our input nodes
    edge_return: pd.DataFrame = edges.loc[mask]
    node_return: Optional[pd.DataFrame] = None
    if nodes is not None:
        nodes_to_retrieve = [node_ids]
        if configuration.return_neighbour_info:
            nodes_to_retrieve.extend(edge_return.target.unique().tolist())
        node_return = nodes.loc[nodes.name.isin(node_ids), :]
    else:
        node_return = pd.DataFrame()
    return (
        edge_return,
        node_return,
    )


csv = PlugIn(
    default_configuration={
        "edge_list_location": "",
        "node_list_location": "",
        "mode": "in",
    },
    callable=csv_connector,
    tables={"edges": {}, "nodes": {}},
    metadata={},
)
