"""Application class for ponyexpress

Constants:

CONNECTOR_GROUP :
    str : group name of our connector entrypoint

STRATEGY_GROUP :
    str : group name of our strategy entrypoint

Todo:
- add a flag too eagerly load data for unknown nodes from the connector.
"""

# pylint: disable=W0613

from functools import partial, singledispatchmethod
from importlib.metadata import entry_points
from pathlib import Path
from sqlite3 import Connection, connect
from typing import List, Optional

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

    Attributes:

    configuration: Optional[Configuration] : the configuration
        loaded from disk. None if not (yet) loaded.
    connector: Optional[Connector] : the connector we use. None
        if ``self.configuration`` is not yet loaded.
    strategy: Optional[Strategy] : the strategy we use. None
        if ``self.configuration`` is not yet loaded.
    """

    def __init__(self) -> None:
        """This is the initializer"""
        # set the loaded configuration to None, as it is not loaded yet
        self.configuration: Optional[Configuration] = None
        self.connector: Optional[Connector] = None
        self.strategy: Optional[Strategy] = None
        self._cache_: Optional[Connection] = None
        self._layer_counter_ = 0

    @classmethod
    def available_configurations(cls) -> List[ConfigurationItem]:
        """returns the names of available configuration files in the working directory"""
        return [
            ConfigurationItem(_, _.name.removesuffix(".pe.yml"))
            for _ in Path().glob("*.pe.yml")
        ]

    def start(self, config: str):
        """start a collection

        Args:
          config: str: the configuration's name which we want to load

        Returns:

        A list of ``ConfigurationItem``, which holds information on
            both the configuration's name and location on the file system.
        """
        self.load_config(config)
        if self.configuration:
            self.connector = self.get_connector(self.configuration.connector)
            self.strategy = self.get_strategy(self.configuration.strategy)
            self._cache_ = connect(self.configuration.db_url)
            self.spider()

    # def restart(self):
    #     pass

    def load_config(self, config_name: str) -> None:
        """loads a named configuration

        Args:
          config_name: str: the name of the configuration to load
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
        """runs the collections loop"""
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
            # in order too sample our network we pass the following information into the sampler
            log.debug(f"Starting iteration {_} with seeds: {', '.join(seeds)}.")
            known_nodes = self.get_known_nodes()
            # all the node we know already at this point
            nodes = self.get_node_info(seeds)  # infos on the new seeds
            edges = self.get_neighbors(seeds)  # edges to the seeds neighbors
            seeds, edges_sparse, _ = self.strategy(
                edges, nodes, known_nodes
            )  # finally we call the sampler
            edges_sparse.loc[:, "layer"] = self._layer_counter_
            # persist the sampled_edges in the data base
            edges_sparse.to_sql(
                f"{self.configuration.edge_table_name}_sparse",
                self._cache_,
                if_exists="append",
            )

            self._layer_counter_ += 1

    # section: database/network interactions

    def get_known_nodes(self) -> List[str]:
        """returns the name of all known nodes"""
        if self.configuration and self._cache_:
            try:
                return pd.read_sql(
                    f"SELECT DISTINCT name FROM {self.configuration.node_table_name}",
                    self._cache_,
                )["name"].tolist()
            except pd.io.sql.DatabaseError:
                return []
        raise ValueError("configuration and data base are not set up")

    def get_node_info(self, node_names: List[str]) -> pd.DataFrame:
        """returns the selected nodes properties.
                The infos are either read from cache or requested via the connector.

        Args:
            node_names : List[str] : selected node names

        Returns:
            A DataFrame with all the information we have on these nodes.
        """
        # map each node
        def _mapper_(node_name: str):
            log.debug(f"Searching for {node_name}")
            # look for the node in the cache
            # if it is there

            cached_result = self._get_cached_node_data_(node_name)
            if cached_result is not None and not cached_result.empty:
                return cached_result
                # return the table row to the mapper
            # else
            # request infos for for node from connector
            return self._dispatch_connector_for_node_(node_name)

        _ret_ = [_mapper_(_) for _ in node_names]

        return pd.concat(_ret_)

    @singledispatchmethod
    def get_neighbors(self, for_node_name) -> pd.DataFrame:
        """retrieve the incident edges for given node or nodes.

        Args:
          for_node_name: str OR List[str] : either a single node name as string or a list of those.

        Returns:

            The table of the edges incident to the specified node or nodes.
        """
        raise NotImplementedError()

    @get_neighbors.register
    def _(self, for_node_name: str) -> pd.DataFrame:
        if self.configuration:
            table_name = f"{self.configuration.edge_table_name}_dense"
            if self._db_ready_(table_name):
                query_string = (
                    f"SELECT * FROM {table_name} WHERE source = '{for_node_name}'"
                )
                log.debug(f"Requesting: {query_string}")

                return pd.read_sql(query_string, self._cache_)
            log.warning(f"No edges returned for {for_node_name}.")

            return pd.DataFrame()
        raise ValueError("Configuration is not loaded. Aborting")

    @get_neighbors.register
    def _(self, for_node_names: list) -> pd.DataFrame:
        if self.configuration:
            table_name = f"{self.configuration.edge_table_name}_dense"
            if self._db_ready_(table_name):
                node_name_string = ", ".join([f"'{_}'" for _ in for_node_names])
                query_string = (
                    f"SELECT * FROM {table_name} WHERE source IN ({node_name_string})"
                )
                log.debug(f"Requesting: {query_string}")
                return pd.read_sql(query_string, self._cache_)
            log.warning(
                f"No edges returned for {','.join(for_node_names)} as the table does not exist."
            )
            return pd.DataFrame(columns=["source", "target", "weight"])
        raise ValueError("Configuration is not loaded. Aborting")

    # section: plugin loading

    def get_strategy(self, strategy_name: PlugInSpec) -> Strategy:
        """lazy load a Strategy

        Args:
          strategy_name: PlugInSpec: name of the strategy

        Returns:
          the wished for strategy

        Raises:
         IndexError : if the strategy does not exist


        """

        return self._get_plugin_from_spec_(strategy_name, STRATEGY_GROUP)

    def get_connector(self, connector_name: PlugInSpec) -> Connector:
        """lazy load a Connector

        Args:
          connector_name: PlugInSpec: name of the connector


        Returns:
          the wished for connector.

        Raises:
          IndexError : if the connector does not exist
        """
        return self._get_plugin_from_spec_(connector_name, CONNECTOR_GROUP)

    # section: private methods

    def _dispatch_connector_for_node_(self, node: str) -> pd.DataFrame:
        if self.configuration and self.connector:
            edges, nodes = self.connector([node])
            log.debug(f"edges:\n{edges}\n\nnodes:{nodes}\n")
            if len(edges) > 0:
                log.info(f"Persisting {len(edges)} for layer #{self._layer_counter_}")
                edges.to_sql(
                    name=f"{self.configuration.edge_table_name}_dense",
                    con=self._cache_,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
            if len(nodes) > 0:
                nodes.loc[:, "layer"] = self._layer_counter_
                nodes.to_sql(
                    name=self.configuration.node_table_name,
                    con=self._cache_,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
            return nodes
        raise ValueError("Configuration or Connector are not present")

    def _get_cached_node_data_(self, node_name: str) -> Optional[pd.DataFrame]:
        if self.configuration and self._cache_:
            table_name = self.configuration.node_table_name

            if self._db_ready_(table_name):
                return pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE name = '{node_name}'",
                    self._cache_,
                )
            return None
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
            log.debug(f"Using this configuration: {values}")
            return partial(entry_point[0].load(), configuration=values)

    def _db_ready_(self, table_name: str) -> bool:
        if self._cache_:
            cursor = self._cache_.cursor()
            cursor.execute(
                f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            )
            return cursor.fetchone()[0] == 1
        raise ValueError("Database is not ready.")

    def __del__(self):
        if self._cache_:
            self._cache_.commit()
            self._cache_.close()
