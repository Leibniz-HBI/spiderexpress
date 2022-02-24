"""

"""
from abc import ABC, abstractclassmethod
import pandas as pd

class Connector(ABC):
    """Abstract base class for all Connectors

    ## A Word on Extending `ponyexpress` with your Own Connector

    This modules exports a single, abstract class, which is implemented by all connectors.

    Although, the class is rather simplistic -- as is it's task: retrieving edges and node information --
    for a set of nodes identified by a name, it is crucial to follow the below given interface in the concrete subclasses.

    In your connector the nodes should be retrievebale by a single identifier and all of the logic must be implemented in your subclass!

    """

    @abstractclassmethod
    def get_layer(self, node_names: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Retrieve the next layer in the network given by a list of node names.
    
        Paramters
        ---------
        node_names : list[str]
            names of the nodes to retrieve information for

        Returns
        -------
        type : tuple[pd.DataFrame, pd.DataFrame]
            tuple contaning edges and node information
        """
        pass
    