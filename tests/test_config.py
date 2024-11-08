"""test suite for spiderexpress.Configuration"""

import pytest
import yaml

from spiderexpress import Configuration
from spiderexpress.types import from_dict

# pylint: disable=W0621


def test_parse_configuration_from_file():
    """Should parse a configuration from a YAML file."""
    with open(
        "tests/stubs/sevens_grader_random_test.pe.yml", "r", encoding="utf8"
    ) as file:
        config = yaml.safe_load(file)
        assert from_dict(Configuration, config) is not None


def test_parse_configuration_from_dict():
    """Should parse a configuration from a file."""
    config = from_dict(Configuration, {"project_name": "test", "seeds": {}})
    assert config is not None
    assert config.project_name == "test"
    assert config.db_url == "sqlite:///test.db"
    assert config.max_iteration == 10000
    assert config.empty_seeds == "stop"
    assert config.layers == {}
    assert config.seeds == {}


def test_fail_to_open_seeds_from_file():
    """Should fail to parse a configuration from a file."""
    with pytest.raises(FileNotFoundError):
        from_dict(Configuration, {"seed_file": "non_existent_file"})


def test_open_seeds_from_file():
    """Should parse a configuration from a file with a seed file."""
    config = from_dict(Configuration, {"seed_file": "tests/stubs/seeds.json"})
    assert config is not None
    assert config.seeds == {"test": ["1", "2", "3"]}


def test_parse_layer_configuration():
    """Should parse a layer configuration."""
    config = from_dict(
        Configuration, {"layers": {"test": "test"}, "seeds": {"test": ["1", "13"]}}
    )
    assert config is not None
    assert config.layers == {"test": "test"}
    assert config.project_name == "spider"
    assert config.db_url == "sqlite:///spider.db"
    assert config.max_iteration == 10000
    assert config.empty_seeds == "stop"
    assert config.seeds == {"test": ["1", "13"]}


def test_layers_must_have_a_router_configuration():
    """Should fail to parse a configuration without a router configuration."""
    with pytest.raises(ValueError):
        from_dict(
            Configuration, {"layers": {"test": {}}, "seeds": {"test": ["1", "13"]}}
        )
