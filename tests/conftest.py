"""helpers for tests"""

from dataclasses import dataclass


@dataclass
class MyFunkyTestClass:
    """test me!"""

    tester: dict


@dataclass
class MyOtherFunkyTestClass:
    """test me harder!"""

    testeringo: MyFunkyTestClass
