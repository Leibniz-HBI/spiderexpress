"""Application class for ponyexpress

Constants:

CONNECTOR_GROUP :
    str : group name of our connector entrypoint

STRATEGY_GROUP :
    str : group name of our strategy entrypoint

Todo:
- nicer way to pass around the dynamic ORM classes
"""
import uuid
from datetime import datetime
from functools import partial, singledispatchmethod
from importlib.metadata import entry_points
from pathlib import Path
from typing import Optional, Union, List

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
)
from ponyexpress.types import Configuration, Connector, PlugInSpec, Strategy

FAILED = "failed"
DONE = "done"

# pylint: disable=W0613,E1101,C0103


CONNECTOR_GROUP = "ponyexpress.connectors"
STRATEGY_GROUP = "ponyexpress.strategies"

MAX_RETRIES = 3

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
            # "before": "increment_iteration",  # TODO: increment iteration in the situation
            #  where are retrying?
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
        return self._cache_.execute(
            sql.select(TaskList)
            .where(TaskList.status == "new")
            .order_by(TaskList.id)
        ).scalars().all()

    def is_gathering_done(self):
        """Checks if the gathering phase is done."""

        log.debug(f"Checking if gathering is done. {len(self.task_buffer)} tasks left.")
        return len(self.task_buffer) == 0

    def is_gathering_not_done(self):
        """Checks if the gathering phase is not done."""
        return not self.is_gathering_done()

    def _conditional_advance(self, *args, **kwargs) -> None:
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

    def load_config(self, config_file: Path, **kwargs) -> None:
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

    def open_database(self, *args, reuse: bool = True) -> None:
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

        Base.metadata.create_all(engine)

        if reuse is True:
            # get the last used appstate and set that to be the current appstate
            appstate = self._cache_.execute(
                sql.select(AppMetaData).order_by(AppMetaData.created_at.desc()).limit(1)
            ).scalar_one_or_none()
        else:
            # create a new appstate
            appstate = None

        if appstate is None:
            appstate = AppMetaData(id=str(uuid.uuid4()), iteration=0, version=0,
                                   created_at=datetime.now())
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
        if not self.configuration:
            raise ValueError("No configuration loaded.")
        if self.appstate.iteration != 0:
            return  # seeds are already initialized

        log.debug(f"Copying seeds to database: {', '.join(self.configuration.seeds)}.")

        # with self._cache_.begin():
        for seed in self.configuration.seeds:
            if self._cache_.get(SeedList, (self.appstate.id, seed)) is None:
                _seed = SeedList(job_id=self.appstate.id, id=seed, iteration=0, status="new")
                self._cache_.add(_seed)
                self._add_task(_seed)

    def _add_task(
            self, task: Union[SeedList, Node, str], parent: Optional[TaskList] = None
    ) -> bool:
        """Adds a task to the task buffer.

        params:
            task: Union[SeedList, Node, str]: the task to add
            parent: Optional[TaskList]: the parent task
        returns:
            bool: True if the task was added, False if it already existed.
        """
        if not self._cache_:
            raise ValueError("Cache is not present.")
        if not isinstance(task, (SeedList, Node, str)):
            raise ValueError(
                f"Task must be a seed, a node or a node-identifier, but is {type(task).__name__}"
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
            return False

        new_task = TaskList(
            job_id=self.appstate.id,
            node_id=node_id,
            status="new",
            initiated_at=datetime.now(),
            connector="stub_value",
            parent_task_id=parent.id if parent else None,
        )

        self._cache_.add(new_task)
        self._cache_.commit()

        log.debug(f"Queueing task {new_task.id} for node {node_id}.")
        return True

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

        # Get the ids of all nodes that are in the sample
        sampled_seed_ids = (
            self._cache_.execute(
                sql
                .select(SeedList.id)
                # .where(SeedList.status == "done", SeedList.status == "failed")
            ).scalars().all()
        )

        candidates = (
            self._cache_.execute(
                sql
                .select(Node.name)
                .where(Node.name.not_in(sampled_seed_ids))
            )
            .scalars()
            .all()
        )

        for candidate_seed in candidates:
            if candidate_seed is None:
                continue

            seed = self._cache_.get(SeedList, candidate_seed)

            if seed is not None:
                if seed.status == FAILED:
                    continue
                seed.iteration = self.appstate.iteration
            else:
                if self._add_task(candidate_seed) is False:  # already fetched
                    seed = SeedList(id=candidate_seed, iteration=self.appstate.iteration,
                                    status="done")
                else:
                    seed = SeedList(id=candidate_seed, iteration=self.appstate.iteration,
                                    status="new")
                self._cache_.add(seed)

        self.retry_count += 1

        log.debug(
            f"retry #{self.retry_count} with unused nodes: {', '.join(candidates)}"
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

        task: TaskList = self.task_buffer[0]

        log.debug(f"Gathering data for {task.node_id}.")

        if self._cache_.execute(sql.select(Node).where(Node.name == task.node_id)).scalar_one_or_none() is None:
            self._dispatch_connector_for_node_(task)

        # Mark the node as done or failed
        seed = self._cache_.execute(
            sql.select(SeedList).where(SeedList.job_id == self.appstate.id, SeedList.id ==
                                       task.node_id).limit(1)
        ).scalar_one_or_none()

        if task.status == FAILED:
            if seed is not None:
                seed.status = FAILED
                self._cache_.commit()
            return
        if task.status == DONE:
            if seed is not None:
                log.debug(f"Marking {seed.id} in status {seed.status} as {task.status}.")

                seed.status = DONE
                seed.last_crawled_at = datetime.now()
                self._cache_.commit()
            return

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

        seeds = self._cache_.execute(
            sql
            .select(SeedList.id)
            .where(SeedList.iteration == self.appstate.iteration, SeedList.status == "done")
        ).scalars().all()

        slashtab = "\n    "

        log.debug(f"""Sampling from seeds:\n    {
        slashtab.join([str(_) for _ in self._cache_.execute(
            sql
            .select(SeedList)
            .where(SeedList.iteration == self.appstate.iteration, SeedList.status == "done")
        ).scalars().all()])
        }""")

        edges_query = (
            sql.select(
                RawEdge.source,
                RawEdge.target,
                *aggregations,
            )
            .where(RawEdge.source.in_(seeds))
            .group_by(RawEdge.source, RawEdge.target)
        )
        known_nodes_query = (sql
                             .select(SeedList.id)
                             .where(SeedList.iteration <= self.appstate.iteration)
                             )

        log.debug(f"Aggregation query:\n{edges_query}")

        edges = pd.read_sql(
            edges_query,
            self._cache_.connection(),
        )
        nodes = pd.read_sql(
            self._cache_.query(Node).statement, self._cache_.connection()
        )

        log.debug(f"Known nodes query:\n{known_nodes_query}")

        known_nodes: List[str] = self._cache_.execute(
            known_nodes_query
        ).scalars(SeedList.id).all()

        log.debug(f"Known nodes: {', '.join(known_nodes)}")

        new_seeds, new_edges, _ = self.strategy(edges, nodes, known_nodes)

        log.debug(f"Sampling returned {_} outward edges.")

        if len(new_seeds) == 0:
            log.warning("Found no new seeds.")
        elif self.retry_count > 0:
            self.retry_count = 0

        log.info(f"Found {', '.join(new_seeds)} as seeds for iteration"
                 f" {self.appstate.iteration + 1}")

        for seed in new_seeds:
            if seed is None:
                continue
            if self._cache_.execute(sql.select(SeedList).where(SeedList.id == seed)).scalar_one_or_none() is None:
                _seed = SeedList(
                    job_id=self.appstate.id, id=seed, iteration=self.appstate.iteration + 1, \
                    status="new"
                )
                self._cache_.add(_seed)
                if self._add_task(_seed) is False:
                    _seed.status = DONE
            else:
                log.warning(f"Seed {seed} already exists.")
        new_edges["job_id"] = self.appstate.id
        for edge in new_edges.to_dict(orient="records"):
            if edge["source"] is not None and edge["target"] is not None:
                self._cache_.merge(agg_edge_factory({**edge, "is_dense": True}))
        _["job_id"] = self.appstate.id
        for edge in _.to_dict(orient="records"):
            if edge.get("source") is not None and edge.get("target") is not None:
                self._cache_.merge(agg_edge_factory({**edge, "is_dense": False}))

        self._cache_.commit()

    def load_plugins(self, *args, **kwargs):
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

    def _dispatch_connector_for_node_(self, task: TaskList):
        if not self.configuration or not self.connector:
            raise ValueError("Configuration or Connector are not present")

        edges, nodes = self.connector([task.node_id])

        log.debug(f"edges:\n{edges}\n\nnodes:{nodes}\n")

        if len(nodes) <= 0:
            task.status = "failed"
            self._cache_.commit()
            return
        else:
            nodes["iteration"] = self.appstate.iteration
            nodes["job_id"] = self.appstate.id

            self._cache_.add_all(
                [node_factory(node) for node in nodes.to_dict(orient="records")]
            )
            self._cache_.commit()

        if len(edges) > 0:
            log.info(
                f"""Persisting {
                len(edges)
                } edges for node {
                task.node_id
                } in iteration #{
                self.appstate.iteration
                }."""
            )
            edges["iteration"] = self.appstate.iteration
            edges["job_id"] = self.appstate.id

            self._cache_.add_all(
                [raw_edge_factory(edge) for edge in edges.to_dict(orient="records")]
            )
            if self.configuration.eager is True and task.parent_task_id is None:
                #  We add only new task if the parent_task_id is None to avoid snowballing
                #  the entire population before we even begin sampling.
                for target in edges["target"].unique().tolist():
                    self._add_task(target, parent=task)

        task.status = "done"
        task.finished_at = datetime.now()
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
