from pytest import fixture

# This is the main application, it should load the config, dispatch jobs and keep
# track of its state, handle database connections and manage set up and tear down.
# It is loaded by click has soon has the application starts and the configuration
# is loaded automatically by the initializer.

def test_initializer():
    """
    The initializer should search all candidate configs so they can be shown in
    auto-completion and loaded when selected.
    """
    pass

def test_load_config():
    pass

def test_spider():
    pass

def test_get_node():
    """
    Spider should be able to retrieve node information either from the connected
    database or from a web service.
    """
    pass

def test_get_neighbors():
    """
    Spider should get a nodes neighbors from the edge table of the connected
    database or from a webservice.
    """
    pass

def test_get_strategy():
    """
    Spider should be compatible with different network exploration strategies,
    e.g. spiky ball or snow ball. Strategies should be configurable via the
    config interface and load the appropriate algorithm.
    """
    pass

def test_get_connector():
    """
    Spider should be able to handle requesting networks from different social
    media platforms or web interfaces.
    """