"""spikyball sampling for ponyexpress

ToDo:
  - Nope?
"""

# pylint: disable=W

from dataclasses import dataclass
from functools import reduce
from typing import Dict, List, Tuple, Union

import pandas as pd
from loguru import logger as log

from ..types import PlugIn, fromdict


@dataclass
class ProbabilityConfiguration:
    """stores the configuration for a single probability mass function

    Parameters
    ----------

    coefficient :
        float : yup

    weights :
        Dict[str, float] : keys are interpreted as columns in the node data
    """

    coefficient: float
    weights: Dict[str, float]


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


def calc_norm(source: pd.Series, edge: pd.Series, target: pd.Series) -> float:
    """calculates the normalization contant for skipyball sampling

    Parameters
    ----------

    source :
        pd.Series : weights for the source nodes

    edge :
        pd.Series : weights for the edges

    target :
        pd.Series : weights for the target nodes

    Returns
    -------

    float : the normalization constant
    """
    return sum(source.fillna(1) * edge.fillna(1) * target.fillna(1))


def calc_prob(table: pd.DataFrame, params: ProbabilityConfiguration) -> pd.Series:
    """calculates the probability for row to be sampled with given configuration.

    This implementation follows the form of: $$p(n) = (\\prod^n_{k=1} t_n)^j$$.

    Parameters
    ----------

    table :
        pd.DataFrame : table with attributes

    params :
        ProbabilityConfiguration : information which columns to consider and the exponent

    Returns
    -------

    pd.Series : of len(table) with the probabilites for each row

    Raises
    ------

    KeyError : if a column specified in ``params.weights`` is not present in ``table``.

    Example
    -------

    If the exponent is 1 and we pass in a single column with weight 1, the original values are kept.

    >>> calc_prob(pd.DataFrame({"a": [1,2,3,4,5,6]}), ProbabilityConfiguration(1, {"a": 1}))
    0    1.0
    1    2.0
    2    3.0
    3    4.0
    4    5.0
    5    6.0
    Name: a, dtype: float64

    If we increase the weight, the values are multiplied with that value:

    >>> calc_prob(pd.DataFrame({"a": [1,2,3,4,5,6]}), ProbabilityConfiguration(1, {"a": 2}))
    0     2.0
    1     4.0
    2     6.0
    3     8.0
    4    10.0
    5    12.0
    Name: a, dtype: float64

    Adding other columns multiples the values in one row:

    >>> calc_prob(
    >>>     pd.DataFrame({"a": [1,2,3,4,5,6], "b": [6,5,4,3,2,1]}),
    >>>     ProbabilityConfiguration(1, {"a": 1, "b": 1})
    >>> )
    0     6.0
    1    10.0
    2    12.0
    3    12.0
    4    10.0
    5     6.0
    dtype: float64
    """
    if params.weights and len(params.weights) != 0:
        weights = [
            table[key].astype(float) * weight for key, weight in params.weights.items()
        ]
        log.debug(f"Using this weight matrix: {weights}")
        return reduce(lambda x, y: x * y, weights) ** params.coefficient
    return pd.Series([1 for _ in range(len(table))], dtype=float)


def sample_edges(
    outward_edges: pd.DataFrame,
    nodes: pd.DataFrame,
    parameters: SamplerConfiguration,
    max_layer_size: int,
) -> Tuple[List[str], pd.DataFrame]:
    """this function samples the outward edges (edges to nodes not yet seen)

    Description
    -----------

    $$p_k(e_{ij}) = p_k(j|i) = \\frac{f(i)^α  *g(i, j)^β * h(j)^γ}{s_k} $$

    and:

    $$s_k = ∑_i∈L_k ∑_j∈N(i) f(i)^α * g(i,j)^β * h(j)^γ$$

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

    List[str] : the new seeds for the next iteration

    pd.DataFrame : the sparse edge set to add to the sampled network

    pd.DataFrame : the dense edge set

    """
    source_nodes = (
        outward_edges.reset_index()
        .merge(nodes, left_on="source", right_on="name", how="left")
        .set_index("index")
    )
    target_nodes = (
        outward_edges.reset_index()
        .merge(nodes, left_on="target", right_on="name", how="left")
        .set_index("index")
    )

    source_prob = calc_prob(source_nodes, parameters.source_node_probability)
    edge_prob = calc_prob(outward_edges, parameters.edge_probability)
    target_prob = calc_prob(target_nodes, parameters.target_node_probability)

    if len(target_prob) != len(source_prob):
        log.warning("Length of source and target table are not the same")
        target_prob = pd.Series(
            [1 for _ in range(len(source_prob))], index=source_prob.index
        )

    s_k = calc_norm(source_prob, edge_prob, target_prob)

    log.debug(f"f:\n{source_prob},\ng:\n{edge_prob},\nh:\n{target_prob}\n s_k:{s_k}\n")

    outward_edges.loc[:, "probability"] = (source_prob * edge_prob * target_prob) / s_k

    outward_edges = outward_edges.loc[outward_edges.probability > 0, :]

    log.debug(f"Sampling these data points:\n{outward_edges}\n")

    if (
        len(outward_edges.target.unique()) <= max_layer_size
    ):  # if we have fewer nodes than wished for, return everything
        seeds = outward_edges["target"].unique().tolist()
        sample = outward_edges.drop(labels="probability", axis=1)
    else:
        sample: pd.DataFrame = outward_edges.sample(
            max_layer_size,
            replace=False,
            weights="probability",
        )
        sample.drop(labels="probability", axis=1, inplace=True)
        seeds = sample["target"].unique().tolist()
    return [_ for _ in seeds if _ is not None], sample


def filter_edges(
    edges: pd.DataFrame, known_nodes: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    When the raw data has been collected from the network,
    it is further processed by three different functions.
    The edges are split by ``FilterEdges`` between edges connecting
    the source nodes to nodes already collected in previous layers \\(E^{(in)}_k\\)
    and the edges pointing to new nodes \\(E^{(out)}_k\\).

    Parameters
    ----------

    edges : pd.DataFrame
        the edges to filter

    known_nodes : List[str]
        the nodes to split the edge table on

    Returns
    -------

    Tuple[pd.DataFrame, pd.DataFrame] : edges to known nodes and edges to unknown nodes
    """
    mask = edges.target.isin(known_nodes)
    groups = {
        "in": edges.loc[mask, :],
        "out": edges.loc[~mask, :],
    }

    return (
        groups["in"] if "in" in groups else pd.DataFrame(),
        groups["out"] if "out" in groups else pd.DataFrame(),
    )


def spikyball_strategy(
    edges: pd.DataFrame,
    nodes: pd.DataFrame,
    state: pd.DataFrame,
    configuration: Union[SpikyBallConfiguration, Dict],
) -> Tuple[List[str], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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

    if isinstance(configuration, dict):
        configuration = fromdict(SpikyBallConfiguration, configuration)

    e_in, e_out = filter_edges(edges, state.node_id.tolist())
    seeds, e_sampled = sample_edges(
        e_out,
        nodes,
        configuration.sampler,
        configuration.layer_max_size,
    )
    state = pd.concat([state, pd.DataFrame({"node_id": seeds})])

    return seeds, pd.concat([e_in, e_sampled]), e_out, state


spikyball = PlugIn(
    callable=spikyball_strategy,
    default_configuration={},
    tables={"node_id": "Text"},
    metadata={},
)
