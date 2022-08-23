"""Command Lie Interface definitions for ponyexpress.

Author
------

Philipp Kessling, Leibniz-Institute for Media Research, 2022

Todo
----
    * Refine verbs/commands  for the CLI
    * find a mechanism for stopping/starting collections
"""
import click
from loguru import logger as log

from .spider_application import Spider


@click.group()
@click.pass_context
def cli(ctx):
    """traverse the desert"""
    ctx.ensure_object(Spider)
    log.debug("Hello!")


def complete_project_name(*_, incomplete: str) -> list[str]:
    """
    Searches/lists all project confs in the current context
    """
    return [
        _["name"]
        for _ in Spider.available_configurations()
        if isinstance(_["name"], str) and _["name"].startswith(incomplete)
    ]


@cli.command()
@click.argument("config", type=click.STRING, shell_complete=complete_project_name)
@click.pass_context
def start(ctx: click.Context, config: str):
    """start a job"""
    ctx.obj.start(config)
