"""Command Lie Interface definitions for ponyexpress.

Author
------

Philipp Kessling, Leibniz-Institute for Media Research, 2022

Todo
----
    * Refine verbs/commands  for the CLI
    * find a mechanism for stopping/starting collections
"""
from pathlib import Path

import click
import yaml
from loguru import logger as log

from .spider_application import Spider
from .types import Configuration


@click.group()
@click.pass_context
def cli(ctx):
    """traverse the desert"""
    ctx.ensure_object(Spider)
    log.debug("Hello!")


def complete_project_name(*args) -> list[str]:
    """
    Searches/lists all project confs in the current context
    """
    return [
        _.name
        for _ in Spider.available_configurations()
        if isinstance(_.name, str) and _.name.startswith(args[2])
    ]


@cli.command()
@click.argument("config", type=click.STRING, shell_complete=complete_project_name)
@click.pass_context
def start(ctx: click.Context, config: str):
    """start a job"""
    ctx.obj.start(config)


@cli.command()
@click.argument("config", type=click.STRING)
@click.option("--interactive/--non-interactive", default=False)
def create(config: str, interactive: bool):
    """create a new configuration"""
    args = {"seeds": None, "seed_file": None}

    if interactive:
        for key, description in [
            ("seeds", "add seeds?"),
            ("seed_file", "do you wish to read a file for seeds?"),
        ]:
            args[key] = click.prompt(description)

    conf = Configuration(**args)

    with (Path() / f"{config}.pe.yml").open("w", encoding="utf8") as file:
        yaml.dump(conf, file)
