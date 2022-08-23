# pylint: disable=R

"""Type definitions for ponyexpress

Philipp Kessling <p.kessling@leibniz-hbi.de>
Leibniz-Institute for Media Research, 2022

"""


from pathlib import Path
from typing import Callable

import pandas as pd
import yaml

Connector = Callable[[list[str]], tuple[pd.DataFrame, pd.DataFrame]]
Strategy = Callable[[pd.DataFrame, pd.DataFrame], list[str]]


class Configuration(yaml.YAMLObject):
    """Configuration-File Wrapper"""

    _yaml_tag = "!telegramspider:Configuration"

    def __init__(
        self,
        seeds: list[str] or None,
        seed_file: str or None,
        project_name: str = "spider",
        db_url: str = "sqlite:///{project_name}.sqlite",
        edge_table_name: str = "edge_list",
        node_table_name: str = "node_list",
        strategy: str = "spikyball",
        connector: str = "telegram",
        max_iteration: int = 10000,
        batch_size: int = 150,
    ) -> None:
        if seed_file is not None:
            self.seed_file = Path(seed_file)
            if self.seed_file.exists():
                with self.seed_file.open("r", encoding="utf8") as file:
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
