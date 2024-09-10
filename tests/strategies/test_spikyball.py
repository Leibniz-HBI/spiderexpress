"""test suite for spiderexpress/spikyball.py

ToDo:

- test for {sample, filter}_edges
"""

from typing import List, Tuple

import pandas as pd
import pytest
from numpy import isnan, nan

from spiderexpress.strategies.spikyball import (
    ProbabilityConfiguration,
    calc_norm,
    calc_prob,
    filter_edges,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        ((pd.Series([1, 2, 3]), pd.Series([1, 2, 3]), pd.Series([1, 2, 3])), 36),
        ((pd.Series([1, 2, nan]), pd.Series([1, 2, 3]), pd.Series([1, 2, 3])), 18),
    ],
)
def test_calc_norm(value: Tuple[pd.Series, pd.Series, pd.Series], expected: float):
    """this parameterized test looks into `calc_norm

    It should:
    - calculate the normalization constant
    - ignore NAs, NaNs, Infs etc. and replace those with 1

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
            (
                pd.DataFrame({"a": [1, 2, 3, 4, 5, 6]}),
                ProbabilityConfiguration(2, {"a": 1}),
            ),
            pd.Series([1, 4, 9, 16, 25, 36]),
        ),
        (
            (pd.DataFrame(dtype=float), ProbabilityConfiguration(1, {})),
            pd.Series(dtype=float),
        ),
        (
            (  # check whether nans propagate
                pd.DataFrame({"a": [1, 2, 3, 4, nan]}),
                ProbabilityConfiguration(1, {"a": 1}),
            ),
            pd.Series([1, 2, 3, 4, nan]),
        ),
        (
            (  # check whether given no variable gives uniform weights
                pd.DataFrame({"a": [1, 2, 3, 4, 5, 6]}),
                ProbabilityConfiguration(1, {}),
            ),
            pd.Series([1, 1, 1, 1, 1, 1]),
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
        pd.Series : the calculated weights for the entity
    """
    for to_test, as_expected in zip(calc_prob(*value), expected):
        if isnan(as_expected):
            assert isnan(to_test)
        else:
            assert to_test == as_expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            (
                pd.DataFrame({"source": ["a", "b", "c"], "target": ["b", "c", "c"]}),
                ["a", "b"],
            ),
            (
                pd.DataFrame({"source": ["a"], "target": ["b"]}),
                pd.DataFrame({"source": ["b", "c"], "target": ["c", "c"]}),
            ),
        )
    ],
)
def test_filter_edges(
    value: Tuple[pd.DataFrame, List[str]], expected: Tuple[pd.DataFrame, pd.DataFrame]
):
    """test ``filter_edges``"""
    e_in, e_out, *_ = expected
    e_in2, e_out2, *_ = filter_edges(*value)
    for source, target in zip(e_in["source"].tolist(), e_in2["source"].tolist()):
        assert source == target
    for source, target in zip(e_out["source"].tolist(), e_out2["source"].tolist()):
        assert source == target
