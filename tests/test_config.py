"""test suite for spiderexpress.Configuration"""
from pathlib import Path

import pytest
import yaml
from pytest import skip

from spiderexpress import Configuration

# pylint: disable=W0621


@pytest.fixture
def configuration():
    """Creates a configuration object."""
    return Configuration(
        ["a", "b"],
        None,
        "test",
        "memory",
    )


def test_initializer(configuration):
    """Should instantiate a configuration object."""

    assert configuration.project_name == "test"
    assert configuration.db_url == "memory"
    assert configuration.seeds == ["a", "b"]


def test_fields():
    """should do something"""
    skip()


def test_serialization(configuration, tmpdir: Path):
    """Should write out and re-read the configuration."""
    temp_conf = tmpdir / "test.pe.yml"

    with temp_conf.open("w") as file:
        yaml.dump(configuration, file)

    assert temp_conf.exists()

    with temp_conf.open("r") as file:
        configuration_2 = yaml.full_load(file)

    for key, value in configuration.__dict__.items():
        assert value == configuration_2.__dict__[key]


def test_seed_seedfile():
    """
    It should have either a seed file or a seed list, should throw otherwise.
    """
    skip()
