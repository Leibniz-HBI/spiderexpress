"""Test suite for ponyexpress.connectors.csv_connector."""

from ponyexpress.connectors import csv_connector

simple_test_configuration = {
    "edge_list_location": "tests/stubs/edge_list.csv",
    "mode": "go to hell",
}


def test_simple_case():
    """Should run without throwing and return edges that contain the input nodes."""
    edges, nodes = csv_connector(["123"], simple_test_configuration)

    assert edges is not None
    assert nodes is None
    assert edges.source.tolist() == ["123", "123"]
