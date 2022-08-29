"""test suite for ponyexpress/spikyball.py

Philipp Kessling
Leibniz-Institute for Media Research, 2022

Todos
-----

- test for {sample, filter}_edges
"""

from typing import Tuple

import pandas as pd
import pytest

from ponyexpress.strategies.spikyball import (
    ProbabilityConfiguration,
    calc_norm,
    calc_prob,
)


@pytest.mark.parametrize(
    "value,expected",
    [((pd.Series([1, 2, 3]), pd.Series([1, 2, 3]), pd.Series([1, 2, 3])), 36)],
)
def test_calc_norm(value: Tuple[pd.Series, pd.Series, pd.Series], expected: float):
    """this parameterized test looks into `calc_norm

    Params
    ------

    value :
        Tuple[pd.Series, pd.Series, pd.Series] : the probabilites before normalization
        for the edge, source and target node attributes.

    expected :
        float : the expected normalization constant
    `"""
    assert calc_norm(*value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            (
                pd.DataFrame({"a": [1, 2, 3, 4, 5, 6]}),
                ProbabilityConfiguration(1, {"a": 1}),
            ),
            pd.Series([1, 2, 3, 4, 5, 6]),
        ),
        (
            (pd.DataFrame({"a": [1, 2, 3, 4, 5, 6]}), ProbabilityConfiguration(1, {})),
            pd.Series([1, 1, 1, 1, 1, 1]),
        ),
        (
            (pd.DataFrame(dtype=float), ProbabilityConfiguration(1, {})),
            pd.Series(dtype=float),
        ),
    ],
)
def test_calc_prob(
    value: Tuple[pd.DataFrame, ProbabilityConfiguration], expected: pd.Series
):
    """parameterized test looking into `calc_prob`

    value :
        Tuple[pd.DataFrame, ProbabilityConfiguration] : arguments to `calc_prob`

    expected :
        pd.Series : the calculated unnormalized weight for the entity
    """
    for blib, blub in zip(calc_prob(*value), expected):
        assert blib == blub
