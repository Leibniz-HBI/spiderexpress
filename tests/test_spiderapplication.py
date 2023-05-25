"""Test suite for ponyexpress.Spider

This is the main application, it should load the config, dispatch jobs and keep
track of its state, handle database connections and manage set up and tear down.
It is loaded by click as soon as the application starts and the configuration
is loaded automatically by the initializer.
"""
# pylint: disable=E1101
from pathlib import Path

import pytest
import yaml
from pytest import skip

from ponyexpress import Configuration
from ponyexpress.spider_application import Spider


@pytest.fixture
def default_configuration():
    with Path("tests/stubs/sevens_grader_random_test.pe.yml").open("r", encoding="utf8") as cf:
        config = yaml.safe_load(cf)
    return config

def test_config_discover():
    """
    The config_discover should search all candidate configs so they can be shown in
    auto-completion and loaded when selected.
    """
    skip()


def test_load_config():
    """should load the config"""
    skip()


def test_spider(default_configuration: Configuration):
    """Should instantiate a spider."""
    spider = Spider(default_configuration, auto_transitions=False)

    assert spider is not None
    assert spider.is_idle()

    spider.start()

    assert spider.is_starting()
    assert spider.configuration is not None

    spider.gather()
    assert spider._cache_ is not None  # pylint: disable=W0212


def test_spider_with_spikyball():
    """Should instantiate a spider."""
    spider = Spider()

    assert spider is not None
    assert spider.is_idle()

    spider.start(Path("tests/stubs/sevens_grader_spikyball_test.pe.yml"))

    assert spider.is_stopping()
    assert spider.configuration is not None


def test_get_node():
    """
    Spider should be able to retrieve node information either from the connected
    database or from a web service.
    """
    skip()


def test_get_neighbors():
    """
    Spider should get a nodes neighbors from the edge table of the connected
    database or from a webservice.
    """
    skip()


def test_get_strategy():
    """
    Spider should be compatible with different network exploration strategies,
    e.g. spiky ball or snow ball. Strategies should be configurable via the
    config interface and load the appropriate algorithm.
    """
    skip()


def test_get_connector():
    """
    Spider should be able to handle requesting networks from different social
    media platforms or web interfaces.
    """
