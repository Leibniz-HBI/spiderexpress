"""Definitions for the spiderexpress ORM-model.

It keeps track of the crawled data, the configuration and the current state of the crawler.
"""

import datetime
from typing import Dict

import sqlalchemy as sql
from loguru import logger as log
from sqlalchemy import JSON, orm

# pylint: disable=R0903, W0622


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


def insert_layer_dense_edge(session, layer_id, edge_type, data):
    """Insert a dense edge into the database."""
    source = data.get("source")
    target = data.get("target")
    id = f"{layer_id}:{source}-{target}"

    dense_edge = LayerDenseEdges(
        id=id,
        source=source,
        target=target,
        edge_type=edge_type,
        layer_id=layer_id,
        data=data,
    )
    session.merge(dense_edge)
    session.commit()
    log.debug(f"Inserted dense edge from {source} to {target} in layer {layer_id}")


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


def insert_layer_dense_node(session, layer_id, node_type, data):
    """Insert a dense node into the database."""
    name = data.get("name")
    id = f"{layer_id}:{name}"

    dense_node = LayerDenseNodes(
        id=id, name=name, layer_id=layer_id, node_type=node_type, data=data
    )
    session.merge(dense_node)
    session.commit()
    log.debug(f"Inserted dense node {name} in layer {layer_id}")


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


def insert_layer_sparse_edge(session, layer_id, edge_type, data):
    """Insert a sparse edge into the database."""
    source = data.get("source")
    target = data.get("target")
    weight = data.get("weight")
    id = f"{layer_id}:{source}-{target}"

    sparse_edge = LayerSparseEdges(
        id=id,
        source=source,
        target=target,
        weight=weight,
        edge_type=edge_type,
        layer_id=layer_id,
        data=data,
    )
    session.merge(sparse_edge)
    session.commit()
    log.debug(f"Inserted sparse edge from {source} to {target} in layer {layer_id}")


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


def insert_layer_sparse_node(session, layer_id, node_type, data):
    """Insert a sparse node into the database."""
    name = data.get("name")
    id = f"{layer_id}:{name}"

    sparse_node = LayerSparseNodes(
        id=id, name=name, layer_id=layer_id, node_type=node_type, data=data
    )
    session.merge(sparse_node)
    session.commit()
    log.debug(f"Inserted sparse node {name} in layer {layer_id}")


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


def insert_sampler_state(session, layer_id, iteration, data):
    """Insert the state of the sampler into the database."""
    sampler_state = SamplerStateStore(iteration=iteration, layer_id=layer_id, data=data)
    session.add(sampler_state)
    session.commit()
    log.debug(f"Inserted sampler state for layer {layer_id} at iteration {iteration}")
