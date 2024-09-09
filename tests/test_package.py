"""Test suite for package wide tests"""

import spiderexpress


def test_version():
    """Should assert that the package version is current."""

    assert spiderexpress.__version__ == "0.1.0a0"
