"""Application class for ponyexpress

Constants:

CONNECTOR_GROUP :
    str : group name of our connector entrypoint

STRATEGY_GROUP :
    str : group name of our strategy entrypoint

Todo:
- add a flag too eagerly load data for unknown nodes from the connector.
- refactor to use sqlalchemy instead of sqlite3
- nicer way to pass around the dynamic ORM classes
"""
from datetime import datetime
from functools import partial, singledispatchmethod
from importlib.metadata import entry_points
from pathlib import Path
from typing import Optional

import pandas as pd
import sqlalchemy as sql
import yaml
from loguru import logger as log
from sqlalchemy import orm
from transitions import Machine

from ponyexpress.model import (
    AppMetaData,
    Base,
    SeedList,
    create_aggregated_edge_table,
    create_node_table,
    create_raw_edge_table,
)
from ponyexpress.types import Configuration, Connector, PlugInSpec, Strategy

# pylint: disable=W0613,E1101,C0103


CONNECTOR_GROUP = "ponyexpress.connectors"
STRATEGY_GROUP = "ponyexpress.strategies"

Node, RawEdge, AggEdge = None, None, None
node_factory, raw_edge_factory, agg_edge_factory = None, None, None


class Spider:
    """This is ponyexpress' Spider.

    With this animal we traverse the desert of social media networks.

    Args:
        auto_transitions (bool): If True, the state machine will automatically advance to
        the next state if the current state is done. Defaults to True.

    Attributes:
        configuration: Optional[Configuration] : the configuration
            loaded from disk. None if not (yet) loaded.
        connector: Optional[Connector] : the connector we use. None
            if ``self.configuration`` is not yet loaded.
        strategy: Optional[Strategy] : the strategy we use. None
            if ``self.configuration`` is not yet loaded.
    """

    states = [
        "idle",
        {
            "name": "starting",
            "on_enter": ["open_database", "load_plugins"],
        },
        {
            "name": "gathering",
            "on_enter": ["refresh_task_buffer", "gather_node_data"],
        },
        {
            "name": "sampling",
            "on_enter": "sample_network",
        },
        {
            "name": "stopping",
            "on_enter": "close_database",
        },
    ]
    """List of states the spider can be in.

    `transitions` declares the transitions between these states as either as a string,
    which will simply set the states name or as dicts with convention, that callbacks
    for each state are given by their name. E.g. the callback for the `starting` state
    is `open_database`., which will call the method `open_database` on our spider instance.
    """

    transitions = [
        {
            "trigger": "start",
            "source": "idle",
            "dest": "starting",
            "before": "load_config",
        },
        {
            "trigger": "gather",
            "source": "starting",
            "dest": "gathering",
            "before": "initialize_seeds",
        },
        {
            "trigger": "gather",
            "source": "gathering",
            "dest": "=",
            "conditions": "is_gathering_not_done",
        },
        {
            "trigger": "sample",
            "source": "gathering",
            "dest": "sampling",
            "conditions": "is_gathering_done",
        },
        {
            "trigger": "gather",
            "source": "sampling",
            "dest": "gathering",
            "conditions": "iteration_limit_not_reached",
            "before": "increment_iteration",
        },
        {
            "trigger": "stop",
            "source": "sampling",
            "dest": "stopping",
            "conditions": "iteration_limit_reached",
        },
    ]
    """List of transitions the spider can make."""

    def __init__(self, auto_transitions=True) -> None:

        self.machine = Machine(
            self,
            states=Spider.states,
            initial="idle",
            transitions=Spider.transitions,
            after_state_change="conditional_advance" if auto_transitions else None,
            queued=True,
            auto_transitions=False,
        )

        self.task_buffer = []
        """List of tasks to be executed by the spider."""

        # set the loaded configuration to None, as it is not loaded yet
        self.configuration: Optional[Configuration] = None
        self.connector: Optional[Connector] = None
        self.strategy: Optional[Strategy] = None
        self._cache_: Optional[orm.Session] = None
        self.appstate: Optional[AppMetaData] = None

    def is_gathering_done(self):
        """Checks if the gathering phase is done."""

        log.debug(f"Checking if gathering is done. {len(self.task_buffer)} tasks left.")
        return len(self.task_buffer) == 0

    def is_gathering_not_done(self):
        """Checks if the gathering phase is not done."""
        return not self.is_gathering_done()

    def conditional_advance(self, *args) -> None:
        """Advances the state machine if the current state is done."""
        if self.state == "idle":
            return
        if self.state == "gathering":
            if self.may_sample():
                log.debug("Advancing from gathering to aggregating")
                self.trigger("sample")
                return
            log.debug("Retaining in gathering")
            self.trigger("gather")
            return

        targets = self.machine.get_triggers(self.state)

        log.debug(f"Advancing from {self.state} and I can trigger {', '.join(targets)}")

        for target in targets:
            if self.trigger(target) is True:
                break

    def load_config(self, config_file: Path) -> None:
        """Loads a configuration.

        params:
          config_file: Path: the configuration to load
        """
        log.debug(f"Attempting to load project from {config_file}.")
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file {config_file} does not exist.")

        with config_file.open("r", encoding="utf8") as file:
            self.configuration = yaml.full_load(file)

    def is_config_valid(self):
        """Asserts that the configuration is valid."""
        return self.configuration is not None and isinstance(
            self.configuration, Configuration
        )

    def open_database(self, *args) -> None:
        """Opens the database and initializes the ORM if necessary."""
        global Node, RawEdge, AggEdge, node_factory, raw_edge_factory, agg_edge_factory  # pylint: disable=W0603

        if not self.configuration:
            raise ValueError("No configuration loaded.")
        if not self.configuration.db_url:
            raise ValueError("No database url set.")
        if self.configuration.db_schema is not None:
            sql.event.listen(
                Base.metadata,
                "before_create",
                sql.DDL(f"CREATE SCHEMA IF NOT EXISTS {self.configuration.db_schema}"),
            )

        Base.metadata.schema = self.configuration.db_schema
        AppMetaData.metadata.schema = self.configuration.db_schema
        SeedList.metadata.schema = self.configuration.db_schema

        engine = sql.create_engine(self.configuration.db_url)
        self._cache_ = orm.Session(engine)

        _, Node, node_factory = create_node_table(
            self.configuration.node_table["name"],
            self.configuration.node_table["columns"],
        )
        _, RawEdge, raw_edge_factory = create_raw_edge_table(
            self.configuration.edge_raw_table["name"],
            self.configuration.edge_raw_table["columns"],
        )
        _, AggEdge, agg_edge_factory = create_aggregated_edge_table(
            self.configuration.edge_agg_table["name"],
            self.configuration.edge_agg_table["columns"],
        )

        Base.metadata.create_all(engine)

        # with self._cache_.begin():
        appstate = self._cache_.get(AppMetaData, "1")
        if appstate is None:
            appstate = AppMetaData(id="1", iteration=0, version=0)
            self._cache_.add(appstate)
            self._cache_.commit()
        self.appstate = appstate

        log.info(f"Loaded appstate: {appstate}.")

    def close_database(self, *args) -> None:
        """Closes the database."""
        if self._cache_:
            self._cache_.commit()
            self._cache_.close()
            self._cache_ = None

    def initialize_seeds(self):
        """Initializes the seed list."""
        if not self._cache_:
            raise ValueError("Cache is not present.")

        log.debug(f"Copying seeds to database: {', '.join(self.configuration.seeds)}.")

        # with self._cache_.begin():
        for seed in self.configuration.seeds:
            if self._cache_.get(SeedList, seed) is None:
                self._cache_.add(SeedList(id=seed, iteration=0, status="new"))

    def refresh_task_buffer(self):
        """Refreshes the task buffer."""
        if not self._cache_:
            raise ValueError("Cache is not present.")

        log.debug("Refreshing task buffer.")

        # with self._cache_.begin():
        self.task_buffer = [
            task.id
            for task in self._cache_.query(SeedList)
            .filter(
                SeedList.iteration == self.appstate.iteration,
                SeedList.status == "new",
            )
            .all()
        ]

    def increment_iteration(self):
        """Increments the iteration counter."""
        if not self._cache_:
            raise ValueError("Cache is not present.")

        # with self._cache_.begin():
        self.appstate.iteration += 1
        self._cache_.commit()

    def gather_node_data(self):
        """Gathers node data for the seeds."""
        if not self._cache_:
            raise ValueError("Cache is not present.")
        if Node is None:
            raise ValueError("Node table is not present.")
        # If there are no tasks left, return early to advance to aggregation state
        if len(self.task_buffer) == 0:
            return

        node = self.task_buffer.pop(0)
        log.debug(f"Attempting to gather data for {node}.")

        # Begin transaction with the cache
        # with self._cache_.begin():
        node_info = self._cache_.get(Node, node)

        if node_info is None:
            self._dispatch_connector_for_node_(node)
        # Mark the node as done
        seed = self._cache_.get(SeedList, node)
        seed.status = "done"
        seed.last_crawled_at = datetime.now()
        self._cache_.commit()

    def iteration_limit_not_reached(self):
        """Checks if the iteration limit has been reached."""
        if not self.configuration:
            raise ValueError("No configuration loaded.")

        return self.appstate.iteration < self.configuration.max_iteration

    def sample_network(self):
        """Samples the network."""
        if not self._cache_:
            raise ValueError("Cache is not present.")
        if Node is None:
            raise ValueError("Node table is not present.")
        if AggEdge is None:
            raise ValueError("Aggregated edge table is not present.")

        log.debug("Attempting to sample the network.")

        # with self._cache_.begin():
        edges = pd.read_sql(
            self._cache_.query(
                RawEdge.source,
                RawEdge.target,
                RawEdge.iteration,
                sql.func.count.label("weight"),
            )
            .where(RawEdge.iteration == self.appstate.iteration)
            .group_by(RawEdge.source, RawEdge.target, RawEdge.iteration)
            # .select_from(RawEdge)
            .statement,
            self._cache_.connection(),
        )
        nodes = pd.read_sql(
            self._cache_.query(Node).statement, self._cache_.connection()
        )
        known_nodes = self._cache_.execute(sql.select(SeedList.id)).scalars().all()

        new_seeds, new_edges, _ = self.strategy(edges, nodes, known_nodes)

        if len(new_seeds) == 0:
            log.debug("Found no new seeds.")
            self.trigger("stop")

        for seed in new_seeds:
            if self._cache_.get(SeedList, seed) is None:
                self._cache_.add(
                    SeedList(
                        id=seed, iteration=self.appstate.iteration + 1, status="new"
                    )
                )
        self._cache_.add_all(
            [agg_edge_factory(edge) for edge in new_edges.to_dict(orient="records")]
        )
        self._cache_.commit()

    def load_plugins(self, *args):
        """Loads the plug-ins."""
        self.strategy = self.get_strategy(self.configuration.strategy)
        self.connector = self.get_connector(self.configuration.connector)

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

    def _dispatch_connector_for_node_(self, node: str):
        if not self.configuration or not self.connector:
            raise ValueError("Configuration or Connector are not present")

        edges, nodes = self.connector([node])

        log.debug(f"edges:\n{edges}\n\nnodes:{nodes}\n")

        if len(edges) > 0:
            log.info(
                f"Persisting {len(edges)} for node {node} in iteration #{self.appstate.iteration}."
            )
            edges["iteration"] = self.appstate.iteration
            self._cache_.add_all(
                [raw_edge_factory(edge) for edge in edges.to_dict(orient="records")]
            )
            self._cache_.commit()
        if len(nodes) > 0:
            nodes["iteration"] = self.appstate.iteration
            self._cache_.add_all(
                [node_factory(node) for node in nodes.to_dict(orient="records")]
            )
            self._cache_.commit()

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
