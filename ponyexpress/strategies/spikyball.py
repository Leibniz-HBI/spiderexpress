"""spikyball sampling for ponyexpress

ToDo:
  - Nope?
"""

# pylint: disable=W

from typing import Tuple

import pandas as pd


def spikyball_strategy(
    edges: pd.DataFrame, nodes: pd.DataFrame, known_nodes: list[str]
) -> Tuple[list[str], pd.DataFrame, pd.DataFrame]:
    """See [this paper](https://arxiv.org/abs/2010.11786).

    Args:
      edges: pd.DataFrame: the edge table to sample from
      nodes: pd.DataFrame: the node information to consider
      known_nodes: list[str]: the nodes already visited in earlier iterations

    Returns:
        A triple retun of the next seeds as a ``list[str]``, the edges to
        add to the graph as a ``DataFrame`` and the outgoing, unsampled edges as
        a ``DataFrame``.

    Reference:

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

    def filter_edges(
        edges: pd.DataFrame, known_nodes: list[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """When the raw data has been collected from the network,
        it is further processed by three different functions.
        The edges are sorted by FilterEdges between edges connecting
        the source nodes to nodes already collected in previous layers E^(in)_k
        and the edges pointing to new nodes E^(out)_k .

        Args:
          edges: pd.DataFrame:
          known_nodes: list[str]:

        Returns:
          Two DataFrames with first holding the edges in the known network
          and the second DataFrame holding edges into the unknown.
        """
        raise NotImplementedError()

    def sample_edges(
        outward_edges: pd.DataFrame, nodes: pd.DataFrame
    ) -> Tuple[list[str], pd.DataFrame]:
        """this function samples the outward edges (edges to nodes not yet seen)

        Args:
          outward_edges: pd.DataFrame : the edges pointing towards unseen nodes
          nodes: pd.DataFrame : metadata regarding the known nodes

        Returns:
          list[str] : the new seeds for the next iteration
          pd.DataFrame : the sparse edge set to add to the sampled network
          pd.DataFrame : the dense edge set
        """
        raise NotImplementedError("spikyball_strategy is not yet implemented.")

    e_in, e_out = filter_edges(edges, known_nodes)
    seeds, e_sampled = sample_edges(e_out, nodes)

    return seeds, pd.concat([e_in, e_sampled]), e_out
