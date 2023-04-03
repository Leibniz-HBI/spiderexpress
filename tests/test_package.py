"""Test suite for package wide tests"""

from ponyexpress import __version__


def test_version():
    """Should assert that the package version is current."""

    assert __version__ == "0.1.0"
