"""Edge router/parser for spiderexpress.


"""

import re
from typing import Any, Dict, Optional


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
            'from': 'Tony', 'to': 'ernie', 'view_count': 123,
            'dispatch_with': 'test', 'type': 'twitter-url'
        }]
    """

    def __init__(self, name: str, spec: Dict[str, Any], context: Optional[Dict] = None):
        # Store the layer name
        self.name = name
        # Validate the spec against the rule set
        Router.validate_spec(name, spec, context)
        self.spec = spec.copy()

    @classmethod
    def validate_spec(cls, name, spec, context):
        """Validates a spec in a context."""
        if "to" not in spec:
            raise RouterValidationError(f"{name}: Key 'to' is missing from {spec}.")
        if not isinstance(spec.get("to"), list):
            raise RouterValidationError(
                f"{name}: 'to' is not a list but {spec.get('to')}."
            )
        for target_spec in spec.get("to"):
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
                raise RouterValidationError(
                    f"{ name }: { data_column_name } not found."
                )
        for target_spec in spec.get("to"):
            field = target_spec.get("field")
            if field not in connectors.get(name).get("columns"):
                raise RouterValidationError(
                    f"{name}: reference to  {field} not found in " f"context."
                )

    def parse(self, input_data):
        """Parses data with the given spec and emits edges."""
        ret = []
        constant = {}
        # First we calculate all constants
        for edge_key, spec in self.spec.items():
            if isinstance(spec, str):
                constant[edge_key] = input_data.get(spec)
        for directive in self.spec.get("to", []):
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
                ret.append({"to": value, **local_constant})
                continue
            # Get all matches from the string and return an edge for each
            matches = re.findall(directive.get("pattern"), value)
            for match in matches:
                ret.append({"to": match, **local_constant})

        return ret
