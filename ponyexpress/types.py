# pylint: disable=R

"""Type definitions for ponyexpress

Philipp Kessling <p.kessling@leibniz-hbi.de>
Leibniz-Institute for Media Research, 2022

"""


from dataclasses import dataclass, fields, is_dataclass
from typing import Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union, Any

import pandas as pd


Connector = Callable[[List[str]], Tuple[pd.DataFrame, pd.DataFrame]]
"""Connector Interface.

Args:
    node_names (List[str]): nodes to get information on

Returns:
    An edge table with new edges (these will be persisted into the dense edge-table).
    A node table with information on the requested nodes.
"""
Strategy = Callable[
    [pd.DataFrame, pd.DataFrame, List[str]],
    Tuple[List[str], pd.DataFrame, pd.DataFrame],
]
"""Strategy Interface.

Args:
    edges (DataFrame): edges table
    nodes (DataFrame): nodes table
    known_nodes (List[str]): known node names

Returns:
    1. a list of new seed nodes in a list of node names
    2. DataFrame with new edges that needs to be added to the network
    3. DataFrame with new nodes that needs to be added to the network
"""
PlugInSpec = Union[str, Dict[str, Union[str, Dict[str, Union[str, int]]]]]
"""Plug-In Definition Notation.

Allows either a ``str`` or a dictionary.
"""
ColumnSpec = Dict[str, Union[str, Dict[str, str]]]
"""Column Specification."""


@dataclass
class Configuration:
    """Project-configuration.

    Attributes:
        project_name (str): the project's name (will be used for default naming of the db)
        db_url (str): a fully-qualified database URL e.g. `sqlite:///` for a in-memory database
        db_schema (Optional[str]): if the database utilizes schema names, give one, else let this be
            empty and None
        iteration_limit (int): hard limit of iterations the sampler will do
        strategy: (Union[str, Dict[str, Any]]): either a string giving the name of a
            configuration less sampling strategy or a dictionary with a sampling strategy
            configuration
        routing (Dict[str, RoutingConfiguration]): configures how data from each connector will be
            parsed and how edges will be emitted
        connectors (Dict[str, ConnectorConfiguration]): configures which connectors to use and which
            data to persist
    """
    project_name: str
    db_url: str
    db_schema: Optional[str]
    iteration_limit: int
    strategy: Union[str, Dict[str, Any]]  # We cannot convert to dataclasses at this stage as
    # each strategy implementation has its own requirements regarding its configuration.
    routing: Dict[str, "RoutingConfiguration"]
    connectors: Dict[str, "ConnectorConfiguration"]
    seeds: List[str]
    tables: Dict[str]


@dataclass
class RoutingConfiguration:
    """Configure a Router."""
    field: str
    pattern: Optional[str]
    dispatch_with: str


@dataclass
class ConnectorConfiguration:
    type: Union[str, Dict[str, Any]]
    columns: Dict[str, str]


T = TypeVar("T")


def from_dict(cls: Type[T], dictionary: Dict) -> T:
    """Converts a dictionary to a dataclass.

    Warnings:
        types and keys in the dataclass and the dictionary must match exactly.

    Args:
        cls (Type[T]): the dataclass to convert to
        dictionary (dict): the dictionary to convert

    Returns:
        The dataclass instance with values from the dictionary.
    """
    field_types: Dict[str, Type] = {f.name: f.type for f in fields(cls)}

    return cls(
        **{
            key: from_dict(field_types[key], value)
            if isinstance(value, dict) and is_dataclass(field_types[key])
            else value
            for key, value in dictionary.items()
        }
    )
