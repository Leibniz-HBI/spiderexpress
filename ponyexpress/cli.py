"""Command Line Interface definitions for ponyexpress.

Author
------

Philipp Kessling, Leibniz-Institute for Media Research, 2022

Todo
----

- Refine verbs/commands for the CLI
- find a mechanism for stopping/starting collections
"""
from pathlib import Path

import click
import yaml

from .spider_application import Spider
from .types import Configuration


@click.group()
@click.pass_context
def cli(ctx):
    """Traverse the deserts of the internet."""
    ctx.ensure_object(Spider)


@cli.command()
@click.argument("config", type=click.Path(path_type=Path, exists=True))
@click.option("--reuse/--create",
              default=False,
              help="Create a new job or reuse an existing one. [REUSE]"
              )
@click.pass_context
def start(ctx: click.Context, config: Path, reuse: bool):
    """start a job"""
    ctx.obj.start(config, reuse=reuse)


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
