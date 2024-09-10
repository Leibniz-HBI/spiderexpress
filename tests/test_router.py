"""Test suite for spiderexpress' multi-layer router."""
from typing import Any, Dict, List, Optional, Union

import pytest

from spiderexpress.router import Router, RouterValidationError


@pytest.mark.parametrize(
    ["specification", "context"],
    [
        pytest.param({"from": "column", "to": "wrong_value"}, None, id="to_is_string"),
        pytest.param({"from": "here", "ot": "bad"}, None, id="to_is_missing"),
        pytest.param(
            {"from": "here", "to": [{"field": "yadad"}]},
            {"connectors": {"test": {"columns": {"there": "Text"}}}},
            id="from_column_is_missing_in_context",
        ),
        pytest.param(
            {"from": "column", "to": [{"field": "column", "dispatch_with": "layer_1"}]},
            {"connectors": {"layer_1": {"type": "something", "columns": {"column1"}}}},
            id="column_is_missing_in_context",
        ),
        pytest.param(
            {"from": "column", "to": [{"dispatch_with": "layer_1"}]},
            {"connectors": {"layer_1": {"type": "something", "columns": {"column1"}}}},
            id="field is None",
        ),
    ],
)
def test_router_spec_validation(
    specification: Dict[str, Any], context: Optional[Dict[str, Any]]
):
    """Should validate given specs and raise errors if the configuration is malformed.

    Thus, in these cases we expect the test setup to raise errors.
    """
    with pytest.raises(RouterValidationError):
        Router("test", specification, context)


input_data_1 = {
    "handle": "Tony",
    "forwarded_handle": "Bert",
    "url": "https://www.twitter.com/ernie",
    "view_count": 123,
    "text": """
    Here come's a real, real social media posts with some urls:
    https://www.twitter.com/ernie

    And here some more, yo: https://www.twitter.com/bobafett
    """,
}


@pytest.mark.parametrize(
    ["input_data", "spec", "expected"],
    [
        pytest.param(
            input_data_1,
            {
                "from": "handle",
                "to": [{"field": "forwarded_handle", "dispatch_with": "test"}],
                "view_count": "view_count",
            },
            [
                {
                    "from": "Tony",
                    "to": "Bert",
                    "view_count": 123,
                    "dispatch_with": "test",
                }
            ],
            id="single value",
        ),
        pytest.param(
            input_data_1,
            {
                "from": "handle",
                "to": [
                    {
                        "field": "url",
                        "pattern": r"https://www\.twitter\.com/(\w+)",
                        "dispatch_with": "test",
                    }
                ],
                "view_count": "view_count",
            },
            [
                {
                    "from": "Tony",
                    "to": "ernie",
                    "view_count": 123,
                    "dispatch_with": "test",
                }
            ],
            id="single value from regex",
        ),
        pytest.param(
            input_data_1,
            {
                "from": "handle",
                "to": [
                    {
                        "field": "url",
                        "pattern": r"https://www\.twitter\.com/(\w+)",
                        "dispatch_with": "test",
                        "type": "twitter-url",
                    }
                ],
                "view_count": "view_count",
            },
            [
                {
                    "from": "Tony",
                    "to": "ernie",
                    "view_count": 123,
                    "type": "twitter-url",
                    "dispatch_with": "test",
                }
            ],
            id="single value from regex with directive constant",
        ),
        pytest.param(
            input_data_1,
            {
                "from": "handle",
                "to": [
                    {
                        "field": "text",
                        "pattern": r"https://www\.twitter\.com/(\w+)",
                        "dispatch_with": "test",
                        "type": "twitter-url",
                    }
                ],
                "view_count": "view_count",
            },
            [
                {
                    "from": "Tony",
                    "to": "ernie",
                    "view_count": 123,
                    "dispatch_with": "test",
                    "type": "twitter-url",
                },
                {
                    "from": "Tony",
                    "to": "bobafett",
                    "view_count": 123,
                    "dispatch_with": "test",
                    "type": "twitter-url",
                },
            ],
            id="multiple values from regex with directive constant",
        ),
        pytest.param(
            input_data_1,
            {
                "from": "handle",
                "to": [
                    {
                        "field": "text",
                        "pattern": r"https://www\.twitter\.com/(\w+)",
                        "dispatch_with": "test",
                        "type": "twitter-url",
                    }
                ],
                "view_count": "view_count",
            },
            [
                {
                    "from": "Tony",
                    "to": "ernie",
                    "view_count": 123,
                    "dispatch_with": "test",
                    "type": "twitter-url",
                },
                {
                    "from": "Tony",
                    "to": "bobafett",
                    "view_count": 123,
                    "dispatch_with": "test",
                    "type": "twitter-url",
                },
            ],
            id="multiple values from regex with directive constant",
        ),
    ],
)
def test_router_emittance(
    input_data: Dict[str, Any],
    spec: Dict[str, Any],
    expected: List[Dict[str, Union[str, int]]],
):
    """Should emit the correct edges."""
    router = Router("test", spec)
    result = router.parse(input_data)
    assert len(result) == len(expected)
    for edge in result:
        assert edge in expected
