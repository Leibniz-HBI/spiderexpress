"""Test suite for ponyexpress.connectors.csv_connector."""

from ponyexpress.connectors import csv_connector

simple_test_configuration = {
    "edge_list_location": "tests/stubs/edge_list.csv",
    "mode": "out",
}

node_information_test_configuration = {
    "edge_list_location": "tests/stubs/7th_graders/edges.csv",
    "node_list_location": "tests/stubs/7th_graders/nodes.csv",
    "mode": "out",
}


def test_simple_case():
    """Should run without throwing and return edges that contain the input nodes."""
    edges, nodes = csv_connector(["123"], simple_test_configuration)

    assert edges is not None
    assert nodes.empty is True
    assert edges.source.tolist() == ["123", "123"]


def test_node_information():
    """Should retrieve node information from files."""
    edges, nodes = csv_connector(["1", "13"], node_information_test_configuration)

    assert edges.shape == (62, 4)
    assert nodes.empty is False
