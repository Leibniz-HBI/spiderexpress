# pylint: disable=R

"""Type definitions for ponyexpress

Philipp Kessling <p.kessling@leibniz-hbi.de>
Leibniz-Institute for Media Research, 2022

"""


from dataclasses import dataclass, fields
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union

import pandas as pd
import yaml

Connector = Callable[[List[str]], Tuple[pd.DataFrame, pd.DataFrame]]
"""Connector Interface

args:

    node_names : List[str] : nodes to get information on

returns:

    An edge table with new edges (these will be persisted into the dense edge-table).
    A node table with information on the requested nodes.
"""
Strategy = Callable[
    [pd.DataFrame, pd.DataFrame, List[str]],
    Tuple[List[str], pd.DataFrame, pd.DataFrame],
]
"""Strategy Interface

args:
    edges : DataFrame : edges table

    nodes : DataFrame : nodes table

    known_nodes : List[str] : known node names

returns:

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


class Configuration(yaml.YAMLObject):
    """Configuration-File Wrapper"""

    yaml_tag = "!ponyexpress:Configuration"

    def __init__(
        self,
        seeds: Optional[List[str]] = None,
        seed_file: Optional[str] = None,
        project_name: str = "spider",
        db_url: Optional[str] = None,
        db_schema: Optional[str] = None,
        empty_seeds: str = "stop",
        eager: bool = True,
        edge_raw_table: Optional[ColumnSpec] = None,
        edge_agg_table: Optional[ColumnSpec] = None,
        node_table: Optional[ColumnSpec] = None,
        strategy: PlugInSpec = "spikyball",
        connector: PlugInSpec = "telegram",
        max_iteration: int = 10000,
        batch_size: int = 150,
    ) -> None:
        if seed_file is not None:
            _seed_file = Path(seed_file)
            if _seed_file.exists():
                with _seed_file.open("r", encoding="utf8") as file:
                    self.seeds = list(file.readlines())
        else:
            self.seeds = seeds
        self.strategy = strategy
        self.connector = connector
        self.project_name = project_name
        self.db_url = db_url or f"sqlite://{project_name}.db"
        self.db_schema = db_schema
        self.edge_raw_table = edge_raw_table or {"name": "edge_raw", "columns": {}}
        self.edge_agg_table = edge_agg_table or {"name": "edge_agg", "columns": {}}
        self.node_table = node_table or {"name": "node", "columns": {}}
        self.max_iteration = max_iteration
        self.batch_size = batch_size
        self.empty_seeds = empty_seeds if empty_seeds in ["stop", "retry"] else "stop"
        self.eager = eager


@dataclass
class ConfigurationItem:
    """A minimal class to transport information on available configuration"""

    path: Path
    name: str


T = TypeVar("T")


def fromdict(cls: Type[T], dictionary: dict) -> T:
    """convert a dictionary to a dataclass

    warning:
        types and keys in the dataclass and the dictionary must match exactly.

    args:
        cls : Type[T] : the dataclass to convert to

    dictionary : dict : the dictionary to convert

    returns:
        the dataclass with values from the dictionary
    """
    fieldtypes: Dict[str, Type] = {f.name: f.type for f in fields(cls)}
    return cls(
        **{
            key: fromdict(fieldtypes[key], value)
            # we test whether the current value is a dict and whether it should be kept a dict.
            # py discerns generic types, thus, dict == Dict[unknown, unknown]
            # but Dict != Dict[str, float].
            # Thus, making our life hard and it necessary to test against to name of the type.
            if isinstance(value, dict)
            and not fieldtypes[key].__name__.startswith("dict")
            else value
            for key, value in dictionary.items()
        }
    )
