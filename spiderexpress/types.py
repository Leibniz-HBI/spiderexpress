# pylint: disable=R

"""Type definitions for spiderexpress'DSL.

Philipp Kessling <p.kessling@leibniz-hbi.de>
Leibniz-Institute for Media Research, 2022
"""
import json
from dataclasses import field, fields, is_dataclass
from pathlib import Path
from typing import Callable, Dict, List, Literal, Optional, Tuple, Type, TypeVar, Union

import pandas as pd
from pydantic.dataclasses import dataclass

Connector = Callable[[List[str]], Tuple[pd.DataFrame, pd.DataFrame]]
"""Connector Interface

Args:
    node_names (List[str]): nodes to get information on

Returns:
    An edge table with new edges (these will be persisted into the dense edge-table).
    A node table with information on the requested nodes.
"""
Strategy = Callable[
    [pd.DataFrame, pd.DataFrame, pd.DataFrame],
    Tuple[List[str], pd.DataFrame, pd.DataFrame, pd.DataFrame],
]
"""Strategy Interface.

Args:
    edges (pd.DataFrame): the edge table
    nodes (pd.DataFrame): the node table
    state (pd.DataFrame): the last state of the sampler

Returns:
    (pd.DataFrame): A list of new seed nodes
    (pd.DataFrame): Update for the sparse edge table
    (pd.DataFrame): Update for the sparse edge table
    (pd.DataFrame): Update for the state table
"""
PlugInSpec = Union[str, Dict[str, Union[str, Dict[str, Union[str, int]]]]]
"""Plug-In Definition Notation.

Allows either a ``str`` or a dictionary.
"""
ColumnSpec = Dict[str, Union[str, Dict[str, str]]]
"""Column Specification."""

StopCondition = Literal["stop", "retry"]


@dataclass()
class Configuration:
    """Configuration-File Wrapper.

    Attributes:
        seeds (Dict[str, List[str]], optional): A dictionary of seeds. Defaults to None.
        seed_file (str, optional): A JSON-file containing seeds per layer.
            Takes precedence over `seeds`. Defaults to None.
        project_name (str, optional): The name of the project. Defaults to "spider".
        db_url (str, optional): The database url. Defaults to None.
        db_schema (str, optional): The database schema. Defaults to None.
        empty_seeds (StopCondition, optional): What to do if the seeds are empty.
            Defaults to "stop".
        layers (List[Dict], optional): A list of layer configurations. Defaults to None.
        max_iteration (int, optional): The maximum number of iterations. Defaults to 10000.
    """

    seeds: Optional[Dict[str, List[str]]] = None
    seed_file: Optional[str] = None
    project_name: str = "spider"
    db_url: Optional[str] = None
    db_schema: Optional[str] = None
    empty_seeds: StopCondition = "stop"
    layers: Dict[str, "Layer"] = field(default_factory=dict)
    max_iteration: int = 10000

    def __post_init__(self) -> None:
        """Configuration-File Wrapper for SpiderExpress"""
        if self.seeds is None:
            if self.seed_file is None:
                raise ValueError("Either seeds or seed_file must be provided.")
            _seed_file = Path(self.seed_file)
            if not _seed_file.exists():
                raise FileNotFoundError(f"Seed file {_seed_file.resolve()} not found.")
            with _seed_file.open("r", encoding="utf8") as file:
                self.seeds = json.load(file)
        self.db_url = self.db_url or f"sqlite:///{self.project_name}.db"
        if self.db_url.startswith("sqlite") and self.db_schema is not None:
            raise ValueError("SQLite does not support schemas.")
        self.empty_seeds = (
            self.empty_seeds if self.empty_seeds in ["stop", "retry"] else "stop"
        )


FieldSpec = Dict[str, Union[str, Optional[str]]]
"""Field Specification"""
RouterSpec = Dict[str, Union[str, List[FieldSpec]]]
"""Router Configuration"""


@dataclass
class SamplerSpec:
    """Sampler Configuration"""

    strategy: str
    configuration: Dict


@dataclass
class Layer:
    """Layer Configuration"""

    connector: Dict
    routers: List[Dict[str, RouterSpec]]
    eager = False
    sampler: Dict


@dataclass
class ConfigurationItem:
    """A minimal class to transport information on available configuration"""

    path: Path
    name: str


T = TypeVar("T")


def from_dict(cls: Type[T], dictionary: dict) -> T:
    """convert a dictionary to a dataclass

    warning:
        types and keys in the dataclass and the dictionary must match exactly.

    args:
        cls : Type[T] : the dataclass to convert to

    dictionary : dict : the dictionary to convert

    returns:
        the dataclass with values from the dictionary
    """
    field_types = {f.name: f.type for f in fields(cls)}
    return cls(
        **{
            key: (
                from_dict(field_types[key], value)
                if isinstance(value, dict) and is_dataclass(field_types[key])
                else value
            )
            for key, value in dictionary.items()
        }
    )


@dataclass
class PlugIn:
    """Transports a plug-in and their metadata.

    Attributes:
        default_configuration: a default configuration which should be inserted into
            new projects as default value
        callable: the plug-in's implementation
        tables: which tables are to be created and how for this plug-in
        metadata: additional metadata, as authors, documentation, etc.
    """

    default_configuration: Dict
    callable: Callable
    tables: Dict[str, ColumnSpec]
    metadata: Dict[str, str]


class RetryableException(Exception):
    """Raise if you want spiderexpress to retry the task."""


class FinalException(Exception):
    """Raise if you want spiderexpress to abort."""
