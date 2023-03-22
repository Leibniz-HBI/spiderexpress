"""Definitions for the ponyexpress ORM-model.

It keeps track of the crawled data, the configuration and the current state of the crawler.
"""
# pylint: disable=R0903

from typing import Dict, Tuple, Type

import sqlalchemy as sql
from sqlalchemy import orm

mapper_registry = orm.registry()
type_lookup = {"Integer": sql.Integer, "Text": sql.Text}


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
    last_crawled_at: orm.Mapped[str] = orm.mapped_column()


def create_raw_edge_table(
    name: str, spec_variadic: Dict[str, str]
) -> Tuple[sql.Table, Type["RawEdge"]]:
    """Create an edge table dynamically.

    parameters:
        name: name of the table
        spec_variadic: dict of variadic columns

    returns:
        table: the table
    """
    spec_fixed = [
        sql.Column("source", sql.Text, primary_key=True, index=True),
        sql.Column("target", sql.Text, primary_key=True, index=True),
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

    return table, RawEdge


def create_aggregated_edge_table(
    name: str, spec_variadic: Dict[str, str]
) -> Tuple[sql.Table, Type["AggEdge"]]:
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
        sql.Column("weight", sql.Integer),
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

    class AggEdge:
        """Aggregated edge."""

        def __repr__(self):
            return f"""<AggEdge {
                ' '.join([f'{key}={value}' for key, value in self.__dict__.items() if not key.startswith('_')])
            } />"""

    mapper_registry.map_imperatively(AggEdge, table)

    return table, AggEdge


def create_node_table(
    name: str, spec_variadic: Dict[str, str]
) -> Tuple[sql.Table, Type["Node"]]:
    """Create a node table dynamically.

    parameters:
        name: name of the table
        spec_variadic: dict of variadic columns

    returns:
        table: the table
    """
    spec_fixed = [
        sql.Column("id", sql.Text, primary_key=True, index=True),
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

    return table, Node
