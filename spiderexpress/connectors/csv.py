"""A CSV-reading, network-rippin' connector for your testing purposes."""

import dataclasses
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from spiderexpress.types import PlugIn, from_dict

_cache = {}


@dataclasses.dataclass
class CSVConnectorConfiguration:
    """Configuration items for the csv_connector."""

    edge_list_location: str
    mode: str
    node_list_location: Optional[str] = None
    cache: bool = True


def csv_connector(
    node_ids: List[str], configuration: Union[Dict, CSVConnectorConfiguration]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """The CSV connector!"""
    if isinstance(configuration, dict):
        configuration = from_dict(CSVConnectorConfiguration, configuration)

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

    return (
        edge_return,
        (
            nodes.loc[nodes.name.isin(node_ids), :]
            if nodes is not None
            else pd.DataFrame()
        ),
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
