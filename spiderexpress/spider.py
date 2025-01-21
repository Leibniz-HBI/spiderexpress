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
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd
import sqlalchemy as sql
import yaml
from loguru import logger as log
from sqlalchemy import orm
from transitions import Machine

from spiderexpress.model import (
    AppMetaData,
    Base,
    LayerDenseEdges,
    LayerDenseNodes,
    RawDataStore,
    SamplerStateStore,
    SeedList,
    TaskList,
    get_open_tasks,
    insert_layer_dense_edge,
    insert_layer_dense_node,
    insert_layer_sparse_edge,
    insert_layer_sparse_node,
    insert_raw_data,
    insert_sampler_state,
    insert_seeds,
)
from spiderexpress.plugin_manager import get_plugin
from spiderexpress.router import Router
from spiderexpress.types import Configuration, Connector, Strategy, from_dict

# pylint: disable=W0613,E1101,C0103,R0902,R0911


CONNECTOR_GROUP = "spiderexpress.connectors"
STRATEGY_GROUP = "spiderexpress.strategies"

MAX_RETRIES = 3


class Spider:
    """This is spiderexpress' Spider.

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
            "on_enter": ["open_database"],
        },
        {
            "name": "gathering",
            "on_enter": "gather_node_data",
        },
        {
            "name": "routing",
            "on_enter": "route_raw_data",
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
        {
            "name": "stopped",
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
            "trigger": "route",
            "source": "gathering",
            "dest": "routing",
            "conditions": "is_gathering_done",
        },
        {
            "trigger": "sample",
            "source": "routing",
            "dest": "sampling",
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
        {
            "trigger": "end",
            "source": "stopping",
            "dest": "stopped",
        },
    ]
    """List of transitions the spider can make."""

    def __init__(
        self,
        auto_transitions=True,
        configuration: Optional[Union[Dict[str, Any], Configuration]] = None,
    ) -> None:
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
        self.configuration: Optional[Configuration] = configuration
        self.connector: Optional[Connector] = None
        self.strategy: Optional[Strategy] = None
        self._cache_: Optional[orm.sessionmaker] = None
        self._engine_: Optional[sql.Engine] = None
        # self.appstate: Optional[AppMetaData] = None

    def is_gathering_done(self):
        """Checks if the gathering phase is done."""
        with self._cache_.begin() as session:
            tasks = get_open_tasks(session)
            log.debug(f"Checking if gathering is done. {len(tasks)} tasks left.")
            return len(tasks) == 0

    def is_gathering_not_done(self):
        """Checks if the gathering phase is not done."""
        return not self.is_gathering_done()

    def _conditional_advance(self, *args) -> None:
        """Advances the state machine when the current state is done."""

        log.debug(
            f"Current state: {self.state}, called with"
            f"{', '.join([str(_) for _ in args]) or 'nothing'}."
        )

        if self.state == "idle":
            return
        if self.state == "stopping":
            self.trigger("end")
            return
        if self.state == "gathering":
            if self.may_route():
                log.debug("Advancing from gathering to routing")
                self.trigger("route")
                return
            log.debug("Retaining in gathering")
            self.trigger("gather")
            return
        if self.state == "routing":
            log.debug("Advancing from routing to sampling")
            self.trigger("sample")
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

        log.debug(
            f"Advancing from {self.state} and I can trigger {', '.join(targets) or 'nothing'}."
        )

        for target in targets:
            if self.trigger(target) is True:
                break

    def load_config(self, config_file: Path) -> None:
        """Loads a configuration.

        params:
          config_file: Path: the configuration to load
        """
        if self.configuration is None:
            log.debug(f"Attempting to load project from {config_file}.")
            if not config_file.exists():
                raise FileNotFoundError(
                    f"Configuration file {config_file} does not exist."
                )

            with config_file.open("r", encoding="utf8") as file:
                self.configuration = from_dict(Configuration, yaml.safe_load(file))

    def is_config_valid(self):
        """Asserts that the configuration is valid."""
        return self.configuration is not None and isinstance(
            self.configuration, Configuration
        )

    def open_database(self, *args) -> None:
        """Opens the database and initializes the ORM if necessary."""

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

        self._engine_ = sql.create_engine(
            self.configuration.db_url,
        )
        if self.configuration.db_schema is not None:
            self._engine_ = self._engine_.execution_options(
                schema_translate_map={None: self.configuration.db_schema}
            )

        self._cache_ = orm.sessionmaker(
            self._engine_,
            autobegin=False,
        )

        Base.metadata.create_all(self._engine_)

    @property
    def iteration(self) -> int:
        """Returns the current iteration."""
        if not self._cache_:
            raise ValueError("Cache is not present.")

        with self._cache_.begin() as session:
            appstate: Optional[AppMetaData] = session.query(AppMetaData).first()
            if appstate is None:

                log.warning(
                    "No appstate found, creating a new one with default values."
                )

                appstate = AppMetaData(id=1, iteration=0, version=1)
                session.add(appstate)
            return appstate.iteration

    def close_database(self, *args) -> None:
        """Closes the database."""
        log.info("Closing database. See you next time.")

        with self._cache_.begin() as session:
            session.commit()
            session.close()
            # self._cache_ = None

    def initialize_seeds(self):
        """Initializes the seed list."""
        if not self._cache_:
            raise ValueError("Cache is not present.")

        log.debug(f"Copying seeds to database: {', '.join(self.configuration.seeds)}.")

        with self._cache_.begin() as session:
            for layer, seeds in self.configuration.seeds.items():
                insert_seeds(session, seeds, layer)

    def should_not_stop_sampling(self):
        """Checks if the sampling phase should be stopped.

        Sampling will be indicated to stop if there are no new seeds in the database.
        """
        if not self._cache_:
            raise ValueError("Cache is not present.")
        iteration = self.iteration
        with self._cache_.begin() as session:
            count = (
                session.query(SeedList)
                .filter(
                    SeedList.iteration == iteration + 1,
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

        iteration = self.iteration

        with self._cache_.begin() as session:
            candidate_nodes_names = (
                session.execute(sql.select(SeedList.id)).scalars().all()
            )

            candidates = (
                session.execute(
                    sql.select(LayerDenseNodes.name).where(
                        LayerDenseNodes.name.not_in(candidate_nodes_names)
                    )
                )
                .scalars()
                .all()
            )

            self.retry_count += 1
            session.add_all(
                [
                    SeedList(id=seed, iteration=iteration + 1, status="new")
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

        with self._cache_.begin() as session:
            appstate = session.query(AppMetaData).first()
            appstate.iteration += 1

    def gather_node_data(self):
        """Gathers node data for the first task in queue."""
        if not self._cache_:
            raise ValueError("Cache is not present.")
        # If there are no tasks left, return early to advance to aggregation state

        iteration = self.iteration

        with self._cache_.begin() as session:
            tasks = get_open_tasks(session)
            if len(tasks) == 0:
                return

            task = tasks.pop(0)

            log.debug(f"Attempting to gather data for {task.node_id}.")

            # Begin transaction with the cache

            node_info = session.get(LayerDenseNodes, task.node_id)

            if node_info is None:
                raw_edges, nodes = self._dispatch_connector_for_node_(task)
                insert_raw_data(
                    session,
                    connector_id=task.connector,
                    output_type="edges",
                    data=raw_edges.to_dict(orient="records"),
                    iteration=iteration,
                )
                insert_raw_data(
                    session,
                    connector_id=task.connector,
                    output_type="nodes",
                    data=nodes.to_dict(orient="records"),
                    iteration=iteration,
                )

            # Mark the node as done
            seed = session.get(SeedList, task.node_id)

            if seed is not None:
                seed.status = "done"
                seed.last_crawled_at = datetime.now()
            task.status = "done"
            task.finished_at = datetime.now()
            session.commit()

    def route_raw_data(self):
        """Routes raw data to the appropriate layer."""
        if not self._cache_:
            raise ValueError("Cache is not present.")
        if not self.configuration:
            raise ValueError("No configuration loaded.")

        iteration = self.iteration

        for layer, layer_configuration in self.configuration.layers.items():
            routers = layer_configuration.routers
            for router_definition in routers:
                for router_name, router_spec in router_definition.items():

                    log.debug(
                        f"Routing data with {router_name} and this spec: {router_spec}."
                    )
                    router = Router(router_name, router_spec)
                    with self._cache_.begin() as session:
                        raw_edges = pd.json_normalize(
                            pd.read_sql(
                                sql.select(RawDataStore.data).where(
                                    (RawDataStore.connector_id == layer)
                                    & (RawDataStore.output_type == "edges")
                                    & (RawDataStore.iteration == iteration)
                                ),
                                session.connection(),
                            ).data
                        )
                        nodes = pd.json_normalize(
                            pd.read_sql(
                                sql.select(RawDataStore.data).where(
                                    (RawDataStore.connector_id == layer)
                                    & (RawDataStore.output_type == "nodes")
                                    & (RawDataStore.iteration == iteration)
                                ),
                                session.connection(),
                            ).data
                        )
                        edges = []
                        for raw_edge in raw_edges.assign(iteration=iteration).to_dict(
                            orient="records"
                        ):
                            edges.extend(router.parse(raw_edge))
                        insert_layer_dense_edge(session, router_name, edges)

                        if len(nodes) > 0:
                            nodes["iteration"] = iteration

                            insert_layer_dense_node(
                                session,
                                layer,
                                "default",
                                nodes.to_dict(orient="records"),
                            )

    def iteration_limit_not_reached(self):
        """Checks if the iteration limit has been reached."""
        if not self.configuration:
            raise ValueError("No configuration loaded.")

        return self.iteration < self.configuration.max_iteration

    def iteration_limit_reached(self):
        """Checks if the iteration limit has been reached."""
        return not self.iteration_limit_not_reached()

    def sample_network(self):
        """Samples the network."""
        # pylint: disable=R0914
        if not self._cache_:
            raise ValueError("Cache is not present.")

        iteration = self.iteration

        with self._cache_.begin() as session:
            for layer_id, layer_config in self.configuration.layers.items():
                # Get data for the layer from the dense data stores
                edges = pd.read_sql(
                    sql.select(
                        LayerDenseEdges.source,
                        LayerDenseEdges.target,
                        sql.func.count("*").label("weight"),  # pylint: disable=E1102
                    )
                    .where(LayerDenseEdges.layer_id == layer_id)
                    .group_by(LayerDenseEdges.source, LayerDenseEdges.target),
                    session.connection(),
                )
                nodes = pd.json_normalize(
                    pd.read_sql(
                        sql.select(LayerDenseNodes.name, LayerDenseNodes.data).where(
                            LayerDenseNodes.layer_id == layer_id
                        ),
                        session.connection(),
                    ).data
                )
                sampler_state = pd.json_normalize(
                    pd.read_sql(
                        sql.select(SamplerStateStore.data).where(
                            SamplerStateStore.layer_id == layer_id
                        ),
                        session.connection(),
                    ).data
                )

                log.debug(
                    f"""
                    Sampling layer {layer_id} with {len(edges)} edges and {len(nodes)} nodes.
                    Edges to sample:
    {edges}

                    Nodes to sample:
    {nodes}

                    Sampler state:
    {sampler_state}
    """
                )

                sampler: Strategy = get_plugin(layer_config.sampler, STRATEGY_GROUP)
                new_seeds, sparse_edges, sparse_nodes, new_sampler_state = sampler(
                    edges, nodes, sampler_state
                )

                log.info(f"That's the current state of affairs:\n\n{new_sampler_state}")

                sparse_edges["iteration"] = iteration
                if len(new_seeds) == 0:
                    log.warning("Found no new seeds.")
                elif self.retry_count > 0:
                    self.retry_count = 0

                insert_seeds(session, new_seeds, layer_id, iteration=iteration + 1)

                insert_layer_sparse_edge(
                    session,
                    layer_id,
                    "test",
                    sparse_edges.query("source.notna() & target.notna()").to_dict(
                        orient="records"
                    ),
                )
                insert_layer_sparse_node(
                    session, layer_id, "test", sparse_nodes.to_dict(orient="records")
                )

                for state in new_sampler_state.to_dict(orient="records"):
                    insert_sampler_state(session, layer_id, iteration, state)

    # section: private methods

    def _dispatch_connector_for_node_(
        self, node: TaskList
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # pylint: disable=R0914
        if not self.configuration:
            raise ValueError("Configuration or Connector are not present")
        # Get the connector for the layer
        layer = node.connector
        layer_configuration = self.configuration.layers[layer]
        connector_spec = layer_configuration.connector
        connector = get_plugin(connector_spec, CONNECTOR_GROUP)

        log.debug(f"Requesting data for {node.node_id} from {connector_spec}.")

        return connector([node.node_id])
