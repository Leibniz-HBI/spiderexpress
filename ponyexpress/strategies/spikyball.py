"""spikyball sampling for ponyexpress

Philipp Kessling
Leibniz-Institute for Media Research, 2022
"""

# pylint: disable=W

from dataclasses import dataclass
from typing import Tuple, Union

import pandas as pd
from loguru import logger as log

from ponyexpress.types import fromdict


@dataclass
class ProbabilityConfiguration:
    """stores the configuration for a single probability mass function

    Parameters
    ----------

    coefficient :
        float : yup

    weights :
        dict[str, float] : keys are interpreted as columns in the node data
    """

    coefficient: float
    weights: dict[str, float]


@dataclass
class SamplerConfiguration:
    """stores the configuration for all mass probability functions

    Parameters
    ----------

    source_node_probability :
        ProbabilityConfiguration : giving the equation for the source node's sam
        pling probability

    target_node_probability :
        ProbabilityConfiguration : giving the equation for the target node's sam
        pling probability

    edge_probability :
        ProbabilityConfiguration : giving the equation for the edges's sam
        pling probability
    """

    source_node_probability: ProbabilityConfiguration
    target_node_probability: ProbabilityConfiguration
    edge_probability: ProbabilityConfiguration


@dataclass
class SpikyBallConfiguration:
    """stores the configuration for our sampler

    Parameters
    ----------
    sampler :
        SamplerConfiguration : the configuration to use

    layer_max_size :
        int : the maximum numbers of members a layer may have
    """

    sampler: SamplerConfiguration
    layer_max_size: int = 150


def spikyball_strategy(
    edges: pd.DataFrame,
    nodes: pd.DataFrame,
    known_nodes: list[str],
    configuration: Union[SpikyBallConfiguration, dict],
) -> Tuple[list[str], pd.DataFrame, pd.DataFrame]:
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

    def filter_edges(
        edges: pd.DataFrame, known_nodes: list[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        When the raw data has been collected from the network,
        it is further processed by three different functions.
        The edges are sorted by FilterEdges between edges connecting
        the source nodes to nodes already collected in previous layers E^(in)_k
        and the edges pointing to new nodes E^(out)_k .
        """
        raise NotImplementedError()

    def sample_edges(
        outward_edges: pd.DataFrame,
        nodes: pd.DataFrame,
        parameters: SamplerConfiguration,
    ) -> Tuple[list[str], pd.DataFrame]:
        """this function samples the outward edges (edges to nodes not yet seen)

        Description
        -----------

        pk(e_ij ) = pk(j|i) = (f(i)^α  *g(i, j)^β * h(j)^γ) / s_k

        and:

        s_k = ∑_i∈L_k ∑_j∈ N(i) f(i)^α * g(i,j)^β * h(j)^γ


        This function passes

        Parameters
        ----------

        outward_edges :
            pd.DataFrame : the edges pointing towards unseen nodes

        nodes :
            pd.DataFrame : metadata regarding the known nodes

        parameters :
            SamplerConfiguration : coefficients and weights

        Returns
        -------

        list[str] : the new seeds for the next iteration

        pd.DataFrame : the sparse edge set to add to the sampled network

        pd.DataFrame : the dense edge set

        """

        raise NotImplementedError("spikyball_strategy is not yet implemented.")

    if isinstance(configuration, dict):
        configuration = fromdict(SpikyBallConfiguration, configuration)

    e_in, e_out = filter_edges(edges, known_nodes)
    seeds, e_sampled = sample_edges(e_out, nodes, configuration.sampler)

    return seeds, pd.concat([e_in, e_sampled]), e_out
