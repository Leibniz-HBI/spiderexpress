"""unit test for utils.py

ToDo:

- further testing necessary

"""

from dataclasses import asdict

import pytest

from spiderexpress.types import from_dict
from tests.conftest import MyFunkyTestClass, MyOtherFunkyTestClass


@pytest.mark.parametrize(
    "value",
    [
        (MyFunkyTestClass({})),  # base case
        (
            MyFunkyTestClass({"blib": "blub"})
        ),  # see whether dict with value makes it through
        (MyOtherFunkyTestClass(MyFunkyTestClass({"heftig": {"indegree": 1.0}}))),
        (
            MyOtherFunkyTestClass(
                MyFunkyTestClass({"thing": [{"dict": {"test123": ""}}]})
            )
        ),
    ],
)
def test_fromdict(value: object):
    """test fromdict()"""
    ans = from_dict(type(value), asdict(value))
    print(ans)
    assert ans == value
