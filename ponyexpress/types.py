# pylint: disable=R

"""Type definitions for ponyexpress

Philipp Kessling <p.kessling@leibniz-hbi.de>
Leibniz-Institute for Media Research, 2022

"""


from dataclasses import dataclass, fields
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple, Type, TypeVar, Union

import pandas as pd
import yaml

Connector = Callable[[list[str]], tuple[pd.DataFrame, pd.DataFrame]]
Strategy = Callable[
    [pd.DataFrame, pd.DataFrame, list[str]],
    Tuple[list[str], pd.DataFrame, pd.DataFrame],
]
PlugInSpec = Union[str, Dict[str, Dict[str, Union[str, int]]]]


class Configuration(yaml.YAMLObject):
    """Configuration-File Wrapper"""

    yaml_tag = "!ponyexpress:Configuration"

    def __init__(
        self,
        seeds: Optional[list[str]] = None,
        seed_file: Optional[str] = None,
        project_name: str = "spider",
        db_url: str = "sqlite:///{project_name}.sqlite",
        edge_table_name: str = "edge_list",
        node_table_name: str = "node_list",
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
        self.db_url = db_url or f"sqlite:///{project_name}.sqlite"
        self.edge_table_name = edge_table_name
        self.node_table_name = node_table_name
        self.max_iteration = max_iteration
        self.batch_size = batch_size


@dataclass
class ConfigurationItem:
    """A minimal class to transport information on available configuration"""

    path: Path
    name: str


T = TypeVar("T")


def fromdict(cls: Type[T], dictionary: dict) -> T:
    """convert a dictionary to a dataclass

    Warning
    -------

    types and keys in the dataclass and the dictionary must match exactly.

    Parameters
    ----------
    cls :
        Type[T] : the dataclass to convert to

    dictionary :
        dict : the dictionary to convert

    Returns
    -------

    T : the dataclass with values from the dictionary
    """
    fieldtypes: dict[str, type] = {f.name: f.type for f in fields(cls)}
    return cls(
        **{
            key: fromdict(fieldtypes[key], value)
            # we test whether the current value is a dict and whether it should be kept a dict.
            # py discerns generic types, thus, dict == dict[unknown, unknown]
            # but dict != dict[str, float].
            # Thus, making our life hard and it necessary to test against to name of the type.
            if isinstance(value, dict)
            and not fieldtypes[key].__name__.startswith("dict")
            else value
            for key, value in dictionary.items()
        }
    )
