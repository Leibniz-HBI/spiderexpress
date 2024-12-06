"""Test suite for the model module."""

from datetime import datetime

import pytest
import sqlalchemy as sql
from sqlalchemy.orm import Session

from spiderexpress.model import AppMetaData, Base, SeedList

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


def test_app_state_table(session, create_tables):
    """Test the creation of a node table."""

    create_tables()

    app_state = AppMetaData(id="a", version=1, iteration=1)
    session.add(app_state)
    session.commit()

    assert session.query(AppMetaData).count() == 1


def test_seed_list_table(session, create_tables):
    """Test the creation of a node table."""

    create_tables()

    seed_list = SeedList(
        id="a", status="done", iteration=1, last_crawled_at=datetime.now()
    )
    session.add(seed_list)
    session.commit()

    assert session.query(SeedList).count() == 1
