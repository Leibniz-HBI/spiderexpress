"""Edge router/parser for spiderexpress.


"""

import re
from typing import Any, Dict, List, Optional

from loguru import logger as log

from spiderexpress.types import RouterSpec


class RouterValidationError(ValueError):
    """Something went wrong with validating the spec."""


class Router:
    r"""Creates an edge router from the given specification.

    Arguments:
        name (str): the layer name to bind to
        spec (Dict[str, Union[str, List[TargetSpec]]]): how data should be routed

    Raises:
          RouterValidationError: if the specification is malformed

    Example:
        spec = {
            "source": "handle",
            "target": [
                {
                    "field": "text",
                    "pattern": r"https://www\.twitter\.com/(\w+)",
                    "dispatch_with": "test",
                    "type": "twitter-url",
                }
            ],
            "view_count": "view_count",
        }
        input_data = {
            "handle": "Tony",
            "text": "Check this out: https://www.twitter.com/ernie",
            "view_count": 123,
        }
        router = Router("test", spec)
        result = router.parse(input_data)
        print(result)
        # Output: [{
            'source': 'Tony', 'target': 'ernie', 'view_count': 123,
            'dispatch_with': 'test', 'type': 'twitter-url'
        }]
    """

    TARGET = "target"
    SOURCE = "source"

    def __init__(self, name: str, spec: RouterSpec, context: Optional[Dict] = None):
        # Store the layer name
        self.name = name
        # Validate the spec against the rule set
        Router.validate_spec(name, spec, context)
        self.spec: RouterSpec = spec

    @classmethod
    def validate_spec(cls, name, spec, context):
        """Validates a spec in a context."""
        # pylint: disable=R0912
        if Router.TARGET not in spec:
            raise RouterValidationError(
                f"{name}: Key {Router.TARGET} is missing from {spec}."
            )
        if Router.SOURCE not in spec:
            raise RouterValidationError(
                f"{name}: Key {Router.SOURCE} is missing from {spec}."
            )
        if not isinstance(spec.get(Router.TARGET), list):
            raise RouterValidationError(
                f"{name}: {Router.TARGET} is not a list but '{spec.get(Router.TARGET)}'."
            )
        if isinstance(spec.get(Router.TARGET), list):
            for target_spec in spec.get(Router.TARGET):
                mandatory_fields = ["field", "dispatch_with"]
                for field in mandatory_fields:
                    if target_spec.get(field) is None:
                        raise RouterValidationError(
                            f"{name}: '{field}' cannot be None in {target_spec}"
                        )
        if context is None:
            return

        # Context dependent tests
        connectors = context.get("connectors")
        if name not in connectors:
            raise RouterValidationError(f"{name}: no connector found.")
        this_connector = connectors.get(name)
        for _, data_column_name in spec.items():
            if data_column_name not in this_connector:
                raise RouterValidationError(f"{name}: {data_column_name} not found.")
        for target_spec in spec.get(Router.TARGET):
            field = target_spec.get("field")
            if field not in connectors.get(name).get("columns"):
                raise RouterValidationError(
                    f"{name}: reference to  {field} not found in " f"context."
                )

    def parse(self, input_data) -> List[Dict[str, Any]]:
        """Parses data with the given spec and emits edges."""
        ret = []
        constant = {}

        log.debug(f"Router '{self.name}' parsing {input_data}")

        # First we calculate all constants
        for edge_key, spec in self.spec.items():
            if isinstance(spec, str):
                constant[edge_key] = input_data.get(spec)
        if isinstance(self.spec.get(Router.TARGET), list):
            for directive in self.spec.get(Router.TARGET, []):
                value = input_data.get(directive.get("field"))
                # Add further constants if there are some defined in the spec
                local_constant = {
                    **{
                        key: value
                        for key, value in directive.items()
                        if key not in ["field", "pattern"]
                    },
                    **constant,
                }
                if "pattern" not in directive:
                    # Simply get the value and return a
                    ret.append({Router.TARGET: value, **local_constant})
                    continue
                # Get all matches from the string and return an edge for each
                matches = re.findall(directive.get("pattern"), value)
                for match in matches:
                    ret.append({Router.TARGET: match, **local_constant})
            return ret

        return [constant]
