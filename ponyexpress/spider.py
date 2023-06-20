"""Application class for ponyexpress

Constants:

CONNECTOR_GROUP :
    str : group name of our connector entrypoint

STRATEGY_GROUP :
    str : group name of our strategy entrypoint

Todo:
- nicer way to pass around the dynamic ORM classes
"""
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

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
    TaskList,
    create_aggregated_edge_table,
    create_node_table,
    create_raw_edge_table,
    create_sampler_state_table,
)
from ponyexpress.plugin_manager import get_plugin, get_table_configuration
from ponyexpress.types import Configuration, Connector, Strategy

# pylint: disable=W0613,E1101,C0103


CONNECTOR_GROUP = "ponyexpress.connectors"
STRATEGY_GROUP = "ponyexpress.strategies"

MAX_RETRIES = 3

Node, RawEdge, AggEdge, SamplerState = None, None, None, None
node_factory, raw_edge_factory, agg_edge_factory, sampler_state_factory = (
    None,
    None,
    None,
    None,
)


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
            "on_enter": "gather_node_data",
        },
        {
            "name": "sampling",
            "on_enter": "sample_network",
        },
        {
            "name": "retrying",
            "on_enter": "retry_with_unused_seeds",
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
            "conditions": ["iteration_limit_not_reached", "should_not_stop_sampling"],
            "before": "increment_iteration",
        },
        {
            "trigger": "retry",
            "source": "sampling",
            "dest": "retrying",
            "conditions": ["iteration_limit_not_reached", "should_retry"],
        },
        {
            "trigger": "gather",
            "source": "retrying",
            "dest": "gathering",
            "before": "increment_iteration",
        },
        {
            "trigger": "stop",
            "source": "sampling",
            "dest": "stopping",
            #  "conditions": "iteration_limit_reached",
        },
    ]
    """List of transitions the spider can make."""

    def __init__(self, auto_transitions=True) -> None:
        self.machine = Machine(
            self,
            states=Spider.states,
            initial="idle",
            transitions=Spider.transitions,
            after_state_change="_conditional_advance" if auto_transitions else None,
            queued=True,
            auto_transitions=False,
        )
        self.retry_count = 0

        # set the loaded configuration to None, as it is not loaded yet
        self.configuration: Optional[Configuration] = None
        self.connector: Optional[Connector] = None
        self.strategy: Optional[Strategy] = None
        self._cache_: Optional[orm.Session] = None
        self.appstate: Optional[AppMetaData] = None

    @property
    def task_buffer(self):
        """Returns the task buffer, which is a list of tasks that are currently
        being processed."""
        return self._cache_.query(TaskList).filter(TaskList.status == "new").all()

    def is_gathering_done(self):
        """Checks if the gathering phase is done."""

        log.debug(f"Checking if gathering is done. {len(self.task_buffer)} tasks left.")
        return len(self.task_buffer) == 0

    def is_gathering_not_done(self):
        """Checks if the gathering phase is not done."""
        return not self.is_gathering_done()

    def _conditional_advance(self, *args) -> None:
        """Advances the state machine when the current state is done."""
        if self.state == "idle":
            return
        if self.state == "gathering":
            if self.may_sample():
                log.debug("Advancing from gathering to sampling")
                self.trigger("sample")
                return
            log.debug("Retaining in gathering")
            self.trigger("gather")
            return
        if self.state == "sampling":
            if self.may_gather():
                log.debug("Advancing from sampling to gathering")
                self.trigger("gather")
                return
            if self.may_retry():
                self.trigger("retry")
                return
            log.debug("Advancing from sampling to stopping.")
            self.trigger("stop")
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
        # pylint: disable=W0603
        global Node, RawEdge, AggEdge, SamplerState
        global node_factory, raw_edge_factory, agg_edge_factory, sampler_state_factory

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

        engine = sql.create_engine(
            self.configuration.db_url,
        )
        if self.configuration.db_schema is not None:
            engine = engine.execution_options(
                schema_translate_map={None: self.configuration.db_schema}
            )

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

        strategy_name = (
            self.configuration.strategy
            if isinstance(self.configuration.strategy, str)
            else list(self.configuration.strategy.keys())[0]
        )

        _, SamplerState, sampler_state_factory = create_sampler_state_table(
            strategy_name, get_table_configuration(strategy_name, STRATEGY_GROUP)
        )

        Base.metadata.create_all(engine)

        appstate = self._cache_.get(AppMetaData, "1")
        if appstate is None:
            appstate = AppMetaData(id="1", iteration=0, version=0)
            self._cache_.add(appstate)
            self._cache_.commit()
        self.appstate = appstate

        log.info(f"Loaded appstate: {appstate}.")

    def close_database(self, *args) -> None:
        """Closes the database."""
        log.info("Closing database. See you next time.")

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
                _seed = SeedList(id=seed, iteration=0, status="new")
                self._cache_.add(_seed)
                self._add_task(_seed)

    def _add_task(
        self, task: Union[SeedList, Node, str], parent: Optional[TaskList] = None
    ):
        """Adds a task to the task buffer."""
        if not self._cache_:
            raise ValueError("Cache is not present.")
        if not isinstance(task, (SeedList, Node, str)):
            raise ValueError(
                "Task must be a seed, a node or a node-identifier, but is {type(task).__name__}"
            )

        node_id = (
            task.name
            if isinstance(task, Node)
            else task
            if isinstance(task, str)
            else task.id
        )

        if self._cache_.execute(
            sql.select(TaskList).where(TaskList.node_id == node_id).limit(1)
        ).scalar():
            return

        new_task = TaskList(
            node_id=node_id,
            status="new",
            initiated_at=datetime.now(),
            connector="stub_value",
            parent_task_id=parent.id if parent else None,
        )
        self._cache_.add(new_task)
        self._cache_.commit()

    def should_not_stop_sampling(self):
        """Checks if the sampling phase should be stopped.

        Sampling will be indicated to stop if there are no new seeds in the database.
        """
        if not self._cache_:
            raise ValueError("Cache is not present.")

        count = (
            self._cache_.query(SeedList)
            .filter(
                SeedList.iteration == self.appstate.iteration + 1,
                SeedList.status == "new",
            )
            .count()
        )

        log.debug(f"{count} seeds in the data set, should stop or retry sampling.")

        return count > 0

    def should_retry(self):
        """Checks if the sampling phase should be retried."""
        if not self._cache_:
            raise ValueError("Cache is not present.")

        return (
            self.configuration.empty_seeds != "stop" and self.retry_count < MAX_RETRIES
        )

    def retry_with_unused_seeds(self):
        """Retries the sampling phase with unused seeds."""
        if not self._cache_:
            raise ValueError("Cache is not present.")

        candidate_nodes_names = (
            self._cache_.execute(sql.select(SeedList.id)).scalars().all()
        )

        candidates = (
            self._cache_.execute(
                sql.select(Node.name).where(Node.name.not_in(candidate_nodes_names))
            )
            .scalars()
            .all()
        )

        self.retry_count += 1
        self._cache_.add_all(
            [
                SeedList(id=seed, iteration=self.appstate.iteration + 1, status="new")
                for seed in candidates
            ]
        )

        log.debug(
            f"{self.retry_count} retry with unused seeds: {', '.join(candidates)}"
        )

    def increment_iteration(self):
        """Increments the iteration counter."""
        if not self._cache_:
            raise ValueError("Cache is not present.")

        # with self._cache_.begin():
        self.appstate.iteration += 1
        self._cache_.commit()

    def gather_node_data(self):
        """Gathers node data for the first task in queue."""
        if not self._cache_:
            raise ValueError("Cache is not present.")
        if Node is None:
            raise ValueError("Node table is not present.")
        # If there are no tasks left, return early to advance to aggregation state
        if len(self.task_buffer) == 0:
            return

        task: TaskList = self.task_buffer.pop(0)

        log.debug(f"Attempting to gather data for {task.node_id}.")

        # Begin transaction with the cache
        node_info = self._cache_.get(Node, task.node_id)

        if node_info is None:
            self._dispatch_connector_for_node_(task)

        # Mark the node as done
        seed = self._cache_.get(SeedList, task.node_id)

        if seed is not None:
            seed.status = "done"
            seed.last_crawled_at = datetime.now()
        task.status = "done"
        task.finished_at = datetime.now()
        self._cache_.commit()

    def iteration_limit_not_reached(self):
        """Checks if the iteration limit has been reached."""
        if not self.configuration:
            raise ValueError("No configuration loaded.")

        return self.appstate.iteration < self.configuration.max_iteration

    def iteration_limit_reached(self):
        """Checks if the iteration limit has been reached."""
        return not self.iteration_limit_not_reached()

    def sample_network(self):
        """Samples the network."""
        if not self._cache_:
            raise ValueError("Cache is not present.")
        if Node is None:
            raise ValueError("Node table is not present.")
        if AggEdge is None:
            raise ValueError("Aggregated edge table is not present.")

        log.debug("Attempting to sample the network.")

        aggregation_spec = self.configuration.edge_agg_table["columns"]
        aggregation_funcs = {
            "count": sql.func.count,
            "max": sql.func.max,
            "min": sql.func.min,
            "sum": sql.func.sum,
            "avg": sql.func.avg,
        }

        aggregations = [
            sql.func.count().label(  # pylint: disable=E1102 # not-callable, but it is :shrug:
                "weight"
            ),
            *[
                aggregation_funcs[aggregation](getattr(RawEdge, column)).label(column)
                for column, aggregation in aggregation_spec.items()
            ],
        ]
        sql_statement = (
            self._cache_.query(
                RawEdge.source,
                RawEdge.target,
                *aggregations,
            )
            .group_by(RawEdge.source, RawEdge.target)
            .statement
        )

        log.debug(f"Aggregation query: {sql_statement}")

        edges = pd.read_sql(
            sql_statement,
            self._cache_.connection(),
        )
        nodes = pd.read_sql(
            self._cache_.query(Node).statement, self._cache_.connection()
        )
        sampler_state = pd.read_sql(sql.select(SamplerState), self._cache_.connection())

        log.info(
            f"""Sampling from {
            len(edges)
        } edges with {
            len(nodes)
        } nodes while the sampler's state is { len(sampler_state) } long."""
        )

        log.info(f"That's the current state of affairs: { sampler_state }")

        new_seeds, new_edges, _, new_sampler_state = self.strategy(
            edges, nodes, sampler_state
        )

        new_edges["iteration"] = self.appstate.iteration

        if len(new_seeds) == 0:
            log.warning("Found no new seeds.")
        elif self.retry_count > 0:
            # We found new seeds and can reset the retry counter.
            self.retry_count = 0

        for seed in new_seeds:
            if seed is None:
                continue
            if self._cache_.get(SeedList, seed) is None:
                _seed = SeedList(
                    id=seed, iteration=self.appstate.iteration + 1, status="new"
                )
                self._cache_.add(_seed)
                self._add_task(_seed)
        self._cache_.add_all(
            [
                agg_edge_factory(edge)
                for edge in new_edges.to_dict(orient="records")
                if edge["source"] is not None and edge["target"] is not None
            ]
        )
        for state in new_sampler_state.to_dict(orient="records"):
            self._cache_.merge(SamplerState(**state))

        self._cache_.commit()

    def load_plugins(self, *args):
        """Loads the plug-ins."""
        self.strategy = get_plugin(self.configuration.strategy, STRATEGY_GROUP)
        self.connector = get_plugin(self.configuration.connector, CONNECTOR_GROUP)

    # section: private methods

    def _dispatch_connector_for_node_(self, node: TaskList):
        if not self.configuration or not self.connector:
            raise ValueError("Configuration or Connector are not present")

        edges, nodes = self.connector([node.node_id])

        log.debug(f"edges:\n{edges}\n\nnodes:{nodes}\n")

        if len(edges) > 0:
            log.info(
                f"""Persisting {
                    len(edges)
                } edges for node {
                    node.node_id
                } in iteration
                #{
                    self.appstate.iteration
                }."""
            )
            edges["iteration"] = self.appstate.iteration
            for edge in edges.to_dict(orient="records"):
                self._cache_.merge(raw_edge_factory(edge))

            if self.configuration.eager is True and node.parent_task_id is None:
                #  We add only new task if the parent_task_id is None to avoid snowballing
                #  the entire population before we even begin sampling.
                for target in edges["target"].unique().tolist():
                    self._add_task(target, parent=node)
            self._cache_.commit()
        if len(nodes) > 0:
            nodes["iteration"] = self.appstate.iteration
            for _node in nodes.to_dict(orient="records"):
                self._cache_.merge(node_factory(_node))
            self._cache_.commit()
