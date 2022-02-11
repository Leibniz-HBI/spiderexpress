from pathlib import Path
import click
from loguru import logger as log
from .Spider import Spider

@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(Spider)
    log.debug('Hello!')


@cli.command()
@click.argument('config', type=click.File())
@click.pass_context
def start(ctx, config: Path):
    pass

if __name__ == 'main':
    cli(app = Spider())
