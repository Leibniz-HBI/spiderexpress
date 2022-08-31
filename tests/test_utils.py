"""unit test for utils.py

Philipp Kessling
Leibniz-Institute for Media Research, 2022

Todo
----

- further testing necessary

"""

from dataclasses import asdict, dataclass

import pytest

from ponyexpress.types import fromdict


@dataclass
class MyFunkyTestClass:
    """test me!"""

    tester: dict


@dataclass
class MyOtherFunkyTestClass:
    """test me harder!"""

    testeringo: MyFunkyTestClass


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
    ans = fromdict(type(value), asdict(value))
    print(ans)
    assert ans == value
