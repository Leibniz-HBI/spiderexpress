"""Manages access and information about ponyexpress' plug-in system."""
import functools
import importlib.metadata as mt
import sys
from typing import Callable, Optional

from loguru import logger as log

from ponyexpress.types import PlugIn, PlugInSpec


def _access_entry_point(name: str, group: str) -> Optional[PlugIn]:
    candidates = (
        mt.entry_points().select(name=name, group=group)
        if sys.version_info.minor >= 10
        else [_ for _ in mt.entry_points()[group] if _.name == name]
    )

    log.info(f"Accessed this. { candidates }.")

    if len(candidates) == 1:
        plugin: PlugIn = candidates[0].load()

        log.debug(f"Got { plugin }")
        return plugin
    return None


@functools.singledispatch
def get_plugin(spec: PlugInSpec, group: str) -> Callable:
    """Get a plug-in.

    Args:
        spec: which plug-in and optional configuration
        group: plug-in group to retrieve from
    Returns:
        The function associated with the spec
    Raises:
        ValueError: if the spec's name is not found
    """
    raise NotImplementedError("This function was intentionally not implemented.")


@get_plugin.register(str)
def _(spec: str, group: str) -> Callable:
    plugin = _access_entry_point(spec, group)
    if not plugin:
        raise ValueError(f"{ spec } could not be found in { group }")
    return plugin.callable


@get_plugin.register(dict)
def _(spec: dict, group: str) -> Callable:
    if len(spec.keys()) > 1:
        log.warning(
            "Requesting specification has more than one type."
            "Using the first instance found"
        )
    for name, configuration in spec.items():
        plugin = _access_entry_point(name, group)
        if not plugin:
            raise ValueError(f"{spec} could not be found in {group}")
        return functools.partial(plugin.callable, configuration=configuration)


def get_default_configuration(name: str, group: str):  # pylint: disable=W0613
    """Get the default configuration for a plug-in.

    Args:
        name: the plug-in to get
        group: the group to retrieve from
    """


def get_table_configuration(name: str, group: str):  # pylint: disable=W0613
    """Get the table configuration for a plug-in.

    Args:
        name: the plug-in to get
        group: the group to retrieve from
    """
    plugin = _access_entry_point(name, group)
    if not plugin:
        raise ValueError(f"{name} could not be found in {group}")
    return plugin.tables


def list_plugins(group: str, metadata: bool = True):  # pylint: disable=W0613
    """List all plug-ins in a group.

    Args:
        group: the plu_in group to look into
        metadata: whether to look up and return additional metadata
    """
