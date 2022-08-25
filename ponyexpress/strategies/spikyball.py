"""spikyball sampling for ponyexpress

Philipp Kessling
Leibniz-Institute for Media Research, 2022
"""

# pylint: disable=W

from typing import Tuple

import pandas as pd


def spikyball_strategy(
    edges: pd.DataFrame,
    nodes: pd.DataFrame,
) -> list[str]:
    """

    See [this paper](https://arxiv.org/abs/2010.11786).

    @misc{https://doi.org/10.48550/arxiv.2010.11786,
        doi = {10.48550/ARXIV.2010.11786},
        url = {https://arxiv.org/abs/2010.11786},
        author = {Ricaud, Benjamin and Aspert, Nicolas and Miz, Volodymyr},
        keywords = {Social and Information Networks (cs.SI), Information Retrieval (cs.IR), FOS:
            Computer and information sciences, FOS: Computer and information sciences},
        title = {Spikyball sampling: Exploring large networks
            via an inhomogeneous filtered diffusion},
        publisher = {arXiv},
        year = {2020},
        copyright = {arXiv.org perpetual, non-exclusive license}
    }

    """

    def get_neighbors(node_name: str, nodes: pd.DataFrame) -> list[str]:
        raise NotImplementedError()

    def filter_edges(
        edges: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        When the raw data has been collected from the network,
        it is further processed by three different functions.
        The edges are sorted by FilterEdges between edges connecting
        the source nodes to nodes already collected in previous layers E^(in)_k
        and the edges pointing to new nodes E^(out)_k .
        """
        raise NotImplementedError()

    raise NotImplementedError("spikyball_strategy is not yet implemented.")
