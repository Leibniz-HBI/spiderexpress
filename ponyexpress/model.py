"""Definitions for the ponyexpress ORM-model.

It keeps track of the crawled data, the configuration and the current state of the crawler.
"""
import datetime
from typing import Any, Callable, Dict, List, Tuple, Type

import sqlalchemy as sql
from loguru import logger as log
from sqlalchemy import orm

# pylint: disable=R0903


mapper_registry = orm.registry()
type_lookup = {
    "Integer": sql.Integer,
    "Text": sql.Text,
    "DateTime": sql.DateTime,
}


class Base(orm.DeclarativeBase):
    """Base class for all models."""

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


def create_factory(
    cls: Type[Any], spec_fixed: List[sql.Column], spec_variadic: Dict[str, Any]
) -> Callable:
    """Create a factory function for a given class."""

    log.info(
        f"Creating factory for {cls.__name__} with {spec_fixed} and {spec_variadic}"
    )

    def _(data: Dict[str, Any]) -> Type[Any]:
        return cls(
            **{
                key: data.get(key)
                for key in [column.name for column in spec_fixed]
                + list(spec_variadic.keys())
            }
        )

    return _


def create_raw_edge_table(
    name: str, spec_variadic: Dict[str, str]
) -> Tuple[sql.Table, Type["RawEdge"], Callable]:
    """Create an edge table dynamically.

    parameters:
        name: name of the table
        spec_variadic: dict of variadic columns

    returns:
        table: the table
    """
    spec_fixed = [
        sql.Column("id", sql.Integer, primary_key=True, index=True, autoincrement=True),
        sql.Column("source", sql.Text, index=True, unique=False),
        sql.Column("target", sql.Text, index=True, unique=False),
        sql.Column("iteration", sql.Integer, index=True, unique=False),
    ]

    table = sql.Table(
        name,
        Base.metadata,
        *spec_fixed,
        *[
            sql.Column(key, type_lookup.get(value))
            for key, value in spec_variadic.items()
        ],
    )

    class RawEdge:
        """Unaggregated, raw edge."""

        def __repr__(self):
            return f"""<RawEdge {
                ' '.join([f'{key}={value}' for key, value in self.__dict__.items() if not key.startswith('_')])
            } />"""

    mapper_registry.map_imperatively(RawEdge, table)

    return table, RawEdge, create_factory(RawEdge, spec_fixed, spec_variadic)


def create_aggregated_edge_table(
    name: str, spec_variadic: Dict[str, str]
) -> Tuple[sql.Table, Type["AggEdge"], Callable]:
    """Create an aggregated edge table dynamically.

    parameters:
        name: name of the table
        spec_variadic: dict of variadic columns

    returns:
        table: the table
    """
    spec_fixed = [
        sql.Column("source", sql.Text, primary_key=True, index=True),
        sql.Column("target", sql.Text, primary_key=True, index=True),
        sql.Column(
            "iteration", sql.Integer, primary_key=True, index=True, unique=False
        ),
        sql.Column("weight", sql.Integer),
    ]

    table = sql.Table(
        name,
        Base.metadata,
        *spec_fixed,
        *[sql.Column(key, sql.Integer) for key, value in spec_variadic.items()],
    )

    class AggEdge:
        """Aggregated edge."""

        def __repr__(self):
            return f"""<AggEdge {
                ' '.join([f'{key}={value}' for key, value in self.__dict__.items() if not key.startswith('_')])
            } />"""

    mapper_registry.map_imperatively(AggEdge, table)

    return table, AggEdge, create_factory(AggEdge, spec_fixed, spec_variadic)


def create_node_table(
    name: str, spec_variadic: Dict[str, str]
) -> Tuple[sql.Table, Type["Node"], Callable]:
    """Create a node table dynamically.

    parameters:
        name: name of the table
        spec_variadic: dict of variadic columns

    returns:
        table: the table
    """
    spec_fixed = [
        sql.Column("name", sql.Text, primary_key=True, index=True),
        sql.Column("iteration", sql.Integer, index=True, unique=False),
    ]

    table = sql.Table(
        name,
        Base.metadata,
        *spec_fixed,
        *[
            sql.Column(key, type_lookup.get(value))
            for key, value in spec_variadic.items()
        ],
    )

    class Node:
        """Node."""

        def __repr__(self):
            return f"""<Node {
                ' '.join([f'{key}={value}' for key, value in self.__dict__.items() if not key.startswith('_')])
            } />"""

    mapper_registry.map_imperatively(Node, table)

    return table, Node, create_factory(Node, spec_fixed, spec_variadic)


def create_sampler_state_table(
    name: str, spec_variadic: Dict[str, str]
) -> Tuple[sql.Table, Type["SamplerState"], Callable]:
    """Create a sampler state table dynamically."""

    table = sql.Table(
        name,
        Base.metadata,
        sql.Column("id", sql.Integer, primary_key=True, index=True, autoincrement=True),
        *[
            sql.Column(key, type_lookup.get(value))
            for key, value in spec_variadic.items()
        ],
    )

    class SamplerState:
        """Sampler state."""

    mapper_registry.map_imperatively(SamplerState, table)

    return table, SamplerState, create_factory(SamplerState, [], spec_variadic)
