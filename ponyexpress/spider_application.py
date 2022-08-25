"""Application class for ponyexpress

Philipp Kessling,
Leibniz-Institute for Media Research, 2022.

Constants
---------

CONNECTOR_GROUP :
    str : group name of our connector entrypoint

STRATEGY_GROUP :
    str : group name of our strategy entrypoint

Todo
-----
- iteration in Spider.spider is not correctly implemented yet
- implement the get_neighbor-methods!
"""

from functools import partial, singledispatchmethod
from importlib.metadata import entry_points
from pathlib import Path
from sqlite3 import Connection
from typing import Optional

import pandas as pd
import yaml
from loguru import logger as log

from ponyexpress.types import (
    Configuration,
    ConfigurationItem,
    Connector,
    PlugInSpec,
    Strategy,
)

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
        self.connector: Optional[Connector] = None
        self.strategy: Optional[Strategy] = None
        self._cache_: Optional[Connection] = None

    @classmethod
    def available_configurations(cls) -> list[ConfigurationItem]:
        """returns the names of available configuration files in the working directory

        Returns
        -------
        list[str] : names of the configurations, [] if no present

        """
        return [
            ConfigurationItem(_, _.name.removesuffix(".pe.yml"))
            for _ in Path().glob("*.pe.yml")
        ]

    def start(self, config: str):
        """start a collection

        Params
        -----

        config : str : the configuration's name which we want to load-
        """
        self.load_config(config)
        if self.configuration:
            self.connector = self.get_connector(self.configuration.connector)
            self.strategy = self.get_strategy(self.configuration.strategy)
            self.spider()

    # def restart(self):
    #     pass

    def load_config(self, config_name: str) -> None:
        """loads a named configuration

        Params
        ------
        config_name : str : the name of the configuration to load
        """
        log.debug(
            f"Choosing from these configurations: {Spider.available_configurations()}"
        )

        config = [_ for _ in Spider.available_configurations() if _.name == config_name]

        if len(config) == 1:
            with config[0].path.open("r", encoding="utf8") as file:
                self.configuration = yaml.full_load(file)
        else:
            log.warning(
                f"A project named {config_name} is not present in current folder"
            )

    def spider(self) -> None:
        """runs the collections loop

        Returns:
        None : Nothing. Nada.
        """
        if not self.configuration:
            log.error("No configuration loaded. Aborting.")
            raise ValueError("Seed list is empty or non-existent.")
        if not self.configuration.seeds or len(self.configuration.seeds) == 0:
            raise ValueError("Seed list is empty or non-existent.")
            # start with seed list

        if self.connector is None or self.strategy is None:
            raise ValueError("Seed list is empty or non-existent.")
        seeds = self.configuration.seeds.copy()

        for _ in range(self.configuration.max_iteration):
            edges, nodes = self.connector(seeds)
            seeds = self.strategy(edges, nodes)

    # section: database/network interactions

    def get_node_info(self, node_names: list[str]) -> pd.DataFrame:
        """returns the selected nodes properties.

        The infos are either read from cache or requested via the connector.

        Params
        ------
        node_names :
            list[str] : selected node names

        Returns
        -------
        pd.DataFrame : a table (node_name is the first column)
        """
        # map each node
        def _mapper_(node_name: str):
            # look for the node in the cache
            # if it is there

            cached_result = self._get_cached_node_data_(node_name)
            if cached_result is not None:
                return cached_result
                # return the table row to the mapper
            # else
            # request infos for for node from connector
            return self._dispatch_connector_for_node_(node_name)

        _ret_ = [_mapper_(_) for _ in node_names]

        return pd.concat(_ret_)

    @singledispatchmethod
    def get_neighbors(self, for_node_name) -> list[str]:
        """retrieve the neighbors for given node or nodes.

        Parameters
        ----------

        for_node_name :
            str OR list[str] : either a single node name as string or a list of those.

        Returns
        -------
        list[str] : the handles of the neighboring nodes
        """
        raise NotImplementedError()

    @get_neighbors.register
    def _(self, for_node_name: str) -> list[str]:
        return [for_node_name]

    @get_neighbors.register
    def _(self, for_node_names: list[str]) -> list[str]:
        return for_node_names

    # section: plugin loading

    def get_strategy(self, strategy_name: PlugInSpec) -> Strategy:
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

        return self._get_plugin_from_spec_(strategy_name, STRATEGY_GROUP)

    def get_connector(self, connector_name: PlugInSpec) -> Connector:
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
        return self._get_plugin_from_spec_(connector_name, CONNECTOR_GROUP)

    # section: private methods

    def _dispatch_connector_for_node_(self, node: str) -> pd.DataFrame:
        if self.configuration and self.connector:
            edges, nodes = self.connector([node])

            edges.to_sql(
                self.configuration.edge_table_name,
                self._cache_,
                if_exists="append",
                index=False,
            )
            nodes.to_sql(
                self.configuration.node_table_name,
                self._cache_,
                if_exists="append",
                index=False,
            )
            return nodes
        raise ValueError("Configuration or Connector are not present")

    def _get_cached_node_data_(self, node_name: str) -> pd.DataFrame:
        if self.configuration and self._cache_:
            table_name = self.configuration.node_table_name
            return pd.read_sql_query(
                f"SELECT * FROM {table_name} WHERE {table_name} = '{node_name}'",
                self._cache_,
            )
        raise ValueError("Configuration or DatabaseConnection are not present")

    @singledispatchmethod
    def _get_plugin_from_spec_(self, spec: PlugInSpec, group: str):
        raise NotImplementedError()

    @_get_plugin_from_spec_.register
    def _(self, spec: str, group: str):
        entry_point = [_ for _ in entry_points()[group] if _.name == spec]
        return entry_point[0].load()

    @_get_plugin_from_spec_.register
    def _(self, spec: dict, group: str):
        for key, values in spec.items():
            entry_point = [_ for _ in entry_points()[group] if _.name == key]
            return partial(entry_point[0].load(), **values)
