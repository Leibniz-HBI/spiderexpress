# Routing Documentation

## 1. Router Format

In each project has to declare a router, which resolves edges with a given ruleset.
Thus, each configuration must contain a `routing` field.

The router specification is named and by stating the name as the key of a dictionary, and it's specification as the value.
The router's name is utilized to access data from a named connector, thus, each connector must have a router specified in order to emit edges.

A router specification has to follow a key-value format, where each key gives a key to be emitted.
Each value is a single string, in the `to` field a list is expected.

If a router specification value is a `string`, a single value will be retrieved from the incoming data dictionary.
If the value is not existing in the data, it will return `None`.

If the router specification value is a `list` each element of the list must be a dictionary.
It must state a `field` to be accessed, optionally a RegEx-`pattern` with a single capture group if values in the specified field needs to be further processed and a `connector` name with which the emitted edge mus be walked.
Further metadata can be specified as constants, e.g. an edge type.

For each (multiple) match of each capture group an edge will be emitted.

For brevity, consider the following YAML schema.

```yaml
routing:  # necessary top-level key
    $name:
        from: $column
        to:
            - $specification
        $column: $column
```

```yaml
$specification:
    field: $column
    pattern?: $regex
    dispatch_with: $name
    $column: value
```

---
