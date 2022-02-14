from pathlib import Path
import click
from loguru import logger as log
from .Spider import Spider

@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(Spider)
    log.debug('Hello!')


def complete_project_name(ctx, param, incomplete):
    """
    Searches/lists all project confs in the current context
    """
    return [_.name for _ in ctx.app.projects if _.name.startswidth(incomplete)]

@cli.command()
@click.argument(
    'config',
    type=click.STRING(),
    shell_complete=complete_project_name
)
@click.pass_context
def start(ctx):
    pass

@click.pass_context
def add(ctx):
    pass


if __name__ == 'main':
    cli(app = Spider())
