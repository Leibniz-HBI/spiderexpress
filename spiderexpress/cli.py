"""Command Line Interface definitions for spiderexpress.

Author
------

Philipp Kessling, Leibniz-Institute for Media Research, 2022

Todo
----

- Refine verbs/commands for the CLI
- find a mechanism for stopping/starting collections
"""
from importlib.metadata import entry_points
from pathlib import Path

import click
import yaml

from .spider import CONNECTOR_GROUP, STRATEGY_GROUP, Spider
from .types import Configuration


@click.group()
@click.pass_context
def cli(ctx):
    """Traverse the deserts of the internet."""
    ctx.ensure_object(Spider)


@cli.command()
@click.argument("config", type=click.Path(path_type=Path, exists=True))
@click.pass_context
def start(ctx: click.Context, config: Path):
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


@cli.command()
def list():  # pylint: disable=W0622
    """list all plugins"""
    click.echo("--- connectors ---", color="blue")
    for connector in entry_points(group=CONNECTOR_GROUP):
        click.echo(connector.name)
    click.echo("--- strategies ---")
    for strategy in entry_points(group=STRATEGY_GROUP):
        click.echo(strategy.name)
