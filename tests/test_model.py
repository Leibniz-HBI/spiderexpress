"""Test suite for the model module."""
from datetime import datetime

import pytest
import sqlalchemy as sql
from sqlalchemy.orm import Session

from ponyexpress.model import (
    AppMetaData,
    Base,
    SeedList,
    create_aggregated_edge_table,
    create_node_table,
    create_raw_edge_table,
)

# pylint: disable=W0621


@pytest.fixture
def connection():
    """Creates an in-memory database."""

    connection = sql.create_engine("sqlite:///:memory:", echo=False)
    yield connection
    connection.dispose(close=True)


@pytest.fixture
def session(connection):
    """Creates an in-memory session."""

    session = Session(connection)

    yield session

    session.close()


@pytest.fixture
def create_tables(connection):
    """Creates the tables."""
    yield lambda: Base.metadata.create_all(connection)
    Base.metadata.drop_all(connection)


def test_create_raw_edge_table_with_session(session, create_tables):
    """Test the creation of a raw edge table."""
    _, RawEdge, edge_factory = create_raw_edge_table("raw_edges", {"weight": "Integer"})

    create_tables()

    edge = edge_factory({"source": "a", "target": "b", "weight": 1, "view_count": 1, "job_id":
        "test-job"})
    session.add(edge)
    session.commit()

    assert session.query(RawEdge).count() == 1


def test_create_raw_edge_table():
    """Test the creation of a raw edge table."""
    table, RawEdge, _ = create_raw_edge_table("raw_edges_2", {"weight": "Integer"})

    assert table.name == "raw_edges_2"
    assert len(table.columns) == 6

    assert hasattr(RawEdge, "source")
    assert hasattr(RawEdge, "target")
    assert hasattr(RawEdge, "weight")


def test_create_aggregated_edge_table_with_session(session, create_tables):
    """Test the creation of an aggregated edge table."""
    _, Edge, edge_factory = create_aggregated_edge_table(
        "agg_edges", {"view_count": "Integer"}
    )

    create_tables()

    edge = edge_factory(
        {"source": "a", "target": "b", "weight": 1, "view_count": 1, "iteration": 0, "job_id": "test-job", "is_dense": True}
    )
    session.add(edge)
    session.commit()

    assert session.query(Edge).count() == 1


def test_create_aggregated_edge_table():
    """Test the creation of an aggregated edge table."""
    table, Edge, _ = create_aggregated_edge_table(
        "agg_edges_2", {"view_count": "Integer"}
    )

    assert table.name == "agg_edges_2"
    assert len(table.columns) == 6

    assert hasattr(Edge, "source")
    assert hasattr(Edge, "target")
    assert hasattr(Edge, "view_count")


def test_create_node_table_session(session, create_tables):
    """Test the creation of a node table."""

    _, Node, node_factory = create_node_table("nodes", {"subscriber_count": "Integer"})

    create_tables()

    node = node_factory({"name": "a", "subscriber_count": 1, "job_id": "test-job"})
    session.add(node)
    session.commit()

    assert session.query(Node).count() == 1


def test_create_node_table_with():
    """Test the creation of a node table."""
    table, Node, _ = create_node_table("nodes2", {"subscriber_count": "Integer"})

    assert table.name == "nodes2"
    assert len(table.columns) == 4

    assert hasattr(Node, "name")
    assert hasattr(Node, "subscriber_count")


def test_app_state_table(session, create_tables):
    """Test the creation of a node table."""

    create_tables()

    app_state = AppMetaData(id="a", version=1, iteration=1, created_at=datetime.now())
    session.add(app_state)
    session.commit()

    assert session.query(AppMetaData).count() == 1


def test_seed_list_table(session, create_tables):
    """Test the creation of a node table."""

    create_tables()

    seed_list = SeedList(
        job_id="test"
               "-job", id="a", status="done", iteration=1, last_crawled_at=datetime.now()
    )
    session.add(seed_list)
    session.commit()

    assert session.query(SeedList).count() == 1
