"""Definitions for the spiderexpress ORM-model.

It keeps track of the crawled data, the configuration and the current state of the crawler.
"""

import datetime
from typing import Callable, Dict, List

import sqlalchemy as sql
from loguru import logger as log
from sqlalchemy import JSON, orm

# pylint: disable=R0903, W0622

_DATABASE_BUSY_ = False

mapper_registry = orm.registry()
type_lookup = {
    "Integer": sql.Integer,
    "Text": sql.Text,
    "DateTime": sql.DateTime,
}


class Base(orm.DeclarativeBase):
    """Base class for all models."""

    type_annotation_map = {Dict: JSON}

    def __repr__(self):
        props = [
            f"{key}={value}"
            for key, value in self.__dict__.items()
            if not key.startswith("_")
        ]
        return f"<{self.__class__.__name__} {' '.join(props)} />"


class AppMetaData(Base):
    """Application metadata."""

    __tablename__ = "application_state"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True, index=True)
    version: orm.Mapped[int] = orm.mapped_column()
    iteration: orm.Mapped[int] = orm.mapped_column()


class SeedList(Base):
    """Table of seed nodes for each iteration."""

    __tablename__ = "seed_list"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True, index=True)
    status: orm.Mapped[str] = orm.mapped_column()
    iteration: orm.Mapped[int] = orm.mapped_column()
    layer: orm.Mapped[str] = orm.mapped_column()
    last_crawled_at: orm.Mapped[datetime.datetime] = orm.mapped_column(nullable=True)


class TaskList(Base):
    """Table of tasks for each iteration."""

    __tablename__ = "task_list"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
    node_id: orm.Mapped[str] = orm.mapped_column()
    status: orm.Mapped[str] = orm.mapped_column()
    connector: orm.Mapped[str] = orm.mapped_column()
    parent_task_id: orm.Mapped[str] = orm.mapped_column(nullable=True)
    initiated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(nullable=True)
    finished_at: orm.Mapped[datetime.datetime] = orm.mapped_column(nullable=True)


class RawDataStore(Base):
    """Table for raw data storage.

    Attributes:
        id: Primary key for the table.
        connector_id: Identifier for the connector.
        output_type: Type of the output data.
        created_at: Timestamp when the data was created.
        data: The raw data stored in JSON format.
    """

    __tablename__ = "raw_data_store"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    iteration: orm.Mapped[int] = orm.mapped_column(index=True)
    connector_id: orm.Mapped[str] = orm.mapped_column(index=True)
    output_type: orm.Mapped[str] = orm.mapped_column(index=True)
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        index=True, insert_default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    data: orm.Mapped[Dict] = orm.mapped_column(insert_default={})


class LayerDenseEdges(Base):
    """Table for dense data storage."""

    __tablename__ = "layer_dense_edges"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True, index=True)
    source: orm.Mapped[str] = orm.mapped_column(index=True)
    target: orm.Mapped[str] = orm.mapped_column(index=True)
    edge_type: orm.Mapped[str] = orm.mapped_column(index=True)
    layer_id: orm.Mapped[str] = orm.mapped_column(index=True)
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        index=True, insert_default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    data: orm.Mapped[Dict] = orm.mapped_column(insert_default={})


class LayerDenseNodes(Base):
    """Table for dense data storage."""

    __tablename__ = "layer_dense_nodes"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True, index=True)
    name: orm.Mapped[str] = orm.mapped_column()
    layer_id: orm.Mapped[str] = orm.mapped_column(index=True)
    node_type: orm.Mapped[str] = orm.mapped_column(index=True)
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        index=True, insert_default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    data: orm.Mapped[Dict] = orm.mapped_column()


class LayerSparseEdges(Base):
    """Table for sparse data storage."""

    __tablename__ = "layer_sparse_store"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True, index=True)
    layer_id: orm.Mapped[str] = orm.mapped_column(index=True)
    source: orm.Mapped[str] = orm.mapped_column(index=True)
    target: orm.Mapped[str] = orm.mapped_column(index=True)
    edge_type: orm.Mapped[str] = orm.mapped_column(index=True)
    weight: orm.Mapped[float] = orm.mapped_column()
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        index=True, insert_default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    data: orm.Mapped[Dict] = orm.mapped_column()


class LayerSparseNodes(Base):
    """Table for sparse data storage."""

    __tablename__ = "layer_sparse_nodes"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True, index=True)
    layer_id: orm.Mapped[str] = orm.mapped_column(index=True)
    name: orm.Mapped[str] = orm.mapped_column()
    node_type: orm.Mapped[str] = orm.mapped_column(index=True)
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        index=True, insert_default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    data: orm.Mapped[Dict] = orm.mapped_column()


class SamplerStateStore(Base):
    """Table for storing the state of the sampler."""

    __tablename__ = "sampler_state_store"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
    iteration: orm.Mapped[int] = orm.mapped_column(index=True)
    layer_id: orm.Mapped[str] = orm.mapped_column(index=True)
    data: orm.Mapped[Dict] = orm.mapped_column()
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        insert_default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )


def _merge_list_of_dicts(
    session: orm.Session, data: List[Dict], factory: Callable[[Dict], Base]
) -> None:
    """Merge a list of dictionaries into the database."""
    global _DATABASE_BUSY_  # pylint: disable=W0603
    if _DATABASE_BUSY_ is True:
        log.warning("Database is busy, skipping insert.")
    _DATABASE_BUSY_ = True

    for item in data:
        session.merge(factory(item))

    _DATABASE_BUSY_ = False


def insert_layer_dense_edge(session: orm.Session, edge_type: str, data: List[Dict]):
    """Insert a dense edge into the database."""
    layer_counts = {}

    def _factory_(item):
        log.debug(f"Inserting dense edge for {item}")
        source = item.get("source")
        target = item.get("target")
        layer_id = item.get("dispatch_with")
        if layer_id not in layer_counts:
            layer_counts[layer_id] = 0
        else:
            layer_counts[layer_id] += 1
        id = f"{layer_id}:{source}-{target}"

        return LayerDenseEdges(
            id=id,
            source=source,
            target=target,
            edge_type=edge_type,
            layer_id=layer_id,
            data=item,
        )

    _merge_list_of_dicts(session, data, _factory_)

    _layer_count_str_ = ", ".join(
        (f"{layer}: {count}" for layer, count in layer_counts.items())
    )
    log.info(f"Inserted {_layer_count_str_} dense edges.")


def insert_layer_dense_node(
    session: orm.Session, layer_id: str, node_type: str, data: List[Dict]
):
    """Insert a dense node into the database."""

    def _factory_(item):
        name = item.get("name")
        id = f"{layer_id}:{name}"
        return LayerDenseNodes(
            id=id, name=name, layer_id=layer_id, node_type=node_type, data=item
        )

    _merge_list_of_dicts(session, data, _factory_)

    log.info(f"Inserted {len(data)} dense node in layer {layer_id}")


def insert_layer_sparse_node(
    session: orm.Session, layer_id: str, node_type: str, data: List[Dict]
):
    """Insert a sparse node into the database."""

    def _factory_sparse_node_(item):
        name = item.get("name")
        id = f"{layer_id}:{name}"

        return LayerSparseNodes(
            id=id, name=name, layer_id=layer_id, node_type=node_type, data=item
        )

    _merge_list_of_dicts(session, data, _factory_sparse_node_)
    log.info(f"Inserted {len(data)} sparse nodes in layer {layer_id}.")


def insert_layer_sparse_edge(
    session: orm.Session, layer_id: str, edge_type: str, data: List[Dict]
):
    """Insert a sparse edge into the database."""

    def _factory_sparse_edge_(item):
        source = item.get("source")
        target = item.get("target")
        weight = item.get("weight")
        id = f"{layer_id}:{source}-{target}"
        return LayerSparseEdges(
            id=id,
            source=source,
            target=target,
            weight=weight,
            edge_type=edge_type,
            layer_id=layer_id,
            data=item,
        )

    _merge_list_of_dicts(session, data, _factory_sparse_edge_)

    log.info(f"Inserted {len(data)} sparse edges in layer {layer_id}.")


def insert_sampler_state(session, layer_id, iteration, data):
    """Insert the state of the sampler into the database."""
    sampler_state = SamplerStateStore(iteration=iteration, layer_id=layer_id, data=data)
    session.add(sampler_state)

    log.debug(f"Inserted sampler state for layer {layer_id} at iteration {iteration}")


def insert_seeds(
    session: orm.Session,
    seeds: List[str],
    layer: str,
    iteration: int = 0,
    status: str = "new",
):
    """Insert seeds into the database."""

    def _seed_factory_(seed):
        return SeedList(id=seed, status=status, layer=layer, iteration=iteration)

    _seeds_ = list(filter(lambda x: session.get(SeedList, x) is None, seeds))
    _merge_list_of_dicts(session, _seeds_, _seed_factory_)
    insert_task(session, _seeds_, layer, parent_task=None)

    log.info(f"Inserted {len(seeds)} seeds.")


def insert_task(
    session: orm.Session, node_ids: List[str], connector: str, parent_task: TaskList
):
    """Insert a task into the database."""

    def _task_factory_(node_id):
        return TaskList(
            node_id=node_id,
            status="new",
            connector=connector,
            parent_task_id=parent_task.id if parent_task is not None else None,
        )

    _merge_list_of_dicts(session, node_ids, _task_factory_)

    log.info(f"Inserted {len(node_ids)} tasks.")


def insert_raw_data(
    session: orm.Session,
    connector_id: str,
    output_type: str,
    data: List[Dict],
    iteration: int,
):
    """Insert raw data into the database."""

    _counter_ = {}

    def _raw_data_factory_(item):

        id_stub = f"{connector_id}:{output_type}"
        if id_stub not in _counter_:
            _counter_[id_stub] = 1
        else:
            _counter_[id_stub] += 1
        id = f"{id_stub}:{_counter_[id_stub]}"

        return RawDataStore(
            id=id,
            connector_id=connector_id,
            output_type=output_type,
            data=item,
            iteration=iteration,
        )

    _merge_list_of_dicts(session, data, _raw_data_factory_)

    log.info(f"Inserted raw data for connector {connector_id} of type {output_type}.")


def get_open_tasks(session: orm.Session, limit: int = 10):
    """Get open tasks from the database."""
    return session.query(TaskList).filter(TaskList.status == "new").limit(limit).all()
