from pytest import fixture

# This is the main application, it should load the config, dispatch jobs and keep
# track of its state, handle database connections and manage set up and tear down.

def test_initializer():
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