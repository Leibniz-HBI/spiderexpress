"""Application class for ponyexpress"""

from importlib.metadata import entry_points
from pathlib import Path
from typing import Optional, Union

import yaml
from loguru import logger as log

from ponyexpress.types import Configuration, Connector, Strategy

CONNECTOR_GROUP = "ponyexpress.connectors"
STRATEGY_GROUP = "ponyexpress.strategies"


class Spider:
    """This is ponyexpress' Spider

    With this animal we traverse the desert of social media networks.
    """

    def __init__(self) -> None:
        """This is the intializer


        Params
        ------

        Returns
        -------

        None : Nothing. Nada.
        """
        # set the loaded configuration to None, as it is not loaded yet
        self.configuration: Optional[Configuration] = None

    @classmethod
    def available_configurations(cls) -> list[dict[str, Union[str, Path]]]:
        """returns the names of available configuration files in the working directory

        Returns
        -------
        list[str] : names of the configurations, [] if no present

        """
        return [
            {"name": _.name.removesuffix(".pe.yml"), "path": _}
            for _ in Path().glob("*.pe.yml")
        ]

    # def start(self):
    #     pass

    # def restart(self):
    #     pass

    def load_config(self, config_name: str) -> None:
        """loads a named configuration

        Params
        ------
        config_name : str : the name of the configuration to load
        """
        if config_name in Spider.available_configurations():
            with (Path() / (config_name + ".yml")).open("r", encoding="urf8") as file:
                self.configuration = yaml.full_load(file)

    def spider(self) -> None:
        """runs the collections loop

        Returns:
        None : Nothing. Nada.
        """
        if not self.configuration:
            log.error("No configuration loaded. Aborting.")
        else:
            # start with seed list
            seeds = self.configuration.seeds
            connector = self.get_connector(self.configuration.connector)
            strategy = self.get_strategy(self.configuration.strategy)

            for _ in range(self.configuration.max_iteration):
                edges, nodes = connector(seeds)
                seeds = strategy(edges, nodes)

    # section: plugin loading

    def get_strategy(self, strategy_name: str) -> Strategy:
        """lazy load a Strategy

        Params
        ------
        strategy_name : str : name of the strategy

        Returns
        -------
        Strategy : the wished for strategy

        Raises
        ------
        IndexError : if the strategy does not exist
        """
        entry_point = entry_points().select(name=strategy_name, group=STRATEGY_GROUP)

        return entry_point[0].load()

    def get_connector(self, connector_name: str) -> Connector:
        """lazy load a Connector

        Params
        ------
        connector_name : str : name of the connector

        Returns
        -------
        Connector : the wished for connector

        Raises
        ------
        IndexError : if the connector does not exist
        """
        entry_point = entry_points().select(name=connector_name, group=CONNECTOR_GROUP)

        return entry_point[0].load()
