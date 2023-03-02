"""Test suite for ponyexpress.connectors.csv_connector."""
import pytest

from ponyexpress.connectors import csv_connector

# pylint: disable=redefined-outer-name


@pytest.fixture
def simple_configuration():
    """Returns a simple test configuration for a stub network."""
    return {
        "edge_list_location": "tests/stubs/edge_list.csv",
        "mode": "out",
    }


@pytest.fixture
def seventh_grader_configuration():
    """Return the sevens grader network."""
    return {
        "edge_list_location": "tests/stubs/7th_graders/edges.csv",
        "node_list_location": "tests/stubs/7th_graders/nodes.csv",
        "mode": "out",
    }


def test_simple_case(simple_configuration):
    """Should run without throwing and return edges that contain the input nodes."""
    edges, nodes = csv_connector(["123"], simple_configuration)

    assert edges is not None
    assert nodes.empty is True
    assert edges.source.tolist() == ["123", "123"]


@pytest.mark.parametrize(
    "mode,shape",
    [
        pytest.param("in", (69, 4), id="in"),
        pytest.param("out", (62, 4), id="out"),
        pytest.param("both", (129, 4), id="both"),
    ],
)
def test_retrieval_modes(mode, shape, seventh_grader_configuration):
    """Should correctly get incoming, outgoing edges as well as both directions."""
    seventh_grader_configuration["mode"] = mode
    edges, nodes = csv_connector(["1", "13"], seventh_grader_configuration)

    assert edges.shape == shape
    assert nodes.empty is False
    assert len(nodes.index) == 2
