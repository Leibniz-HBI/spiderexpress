"""A CSV-reading, network-rippin' connector for your testing purposes."""
import dataclasses
from typing import Dict, List, Optional, Union

import pandas as pd

from ponyexpress.types import fromdict


@dataclasses.dataclass
class CSVConnectorConfiguration:
    """Configuration items for the csv_connector."""

    edge_list_location: str
    mode: str
    node_list_location: Optional[str] = None


def csv_connector(
    node_ids: List[str], configuration: Union[Dict, CSVConnectorConfiguration]
) -> (pd.DataFrame, pd.DataFrame):
    """The CSV connector!"""
    if isinstance(configuration, dict):
        configuration = fromdict(CSVConnectorConfiguration, configuration)

    edges = pd.read_csv(configuration.edge_list_location, dtype=str)
    nodes = (
        pd.read_csv(configuration.node_list_location, dtype=str)
        if configuration.node_list_location
        else None
    )

    # Filter edges that contain our input nodes
    edge_return: pd.DataFrame = edges.loc[
        edges["source"].isin(node_ids)
    ]  # directed, out-going case at first

    node_return = None
    if nodes is not None:
        new_nodes = edge_return.target.unique().tolist()
        node_return = nodes.loc[nodes.name.isin(new_nodes)]

    return edge_return, node_return if node_return is not None else pd.DataFrame()
