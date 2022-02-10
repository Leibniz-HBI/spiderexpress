import click
from loguru import logger as log

import click
from .Spider import Spider

@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(Spider)
    log.debug('Hello!')


@cli.command()
@click.pass_context
def start(ctx):
    pass

@cli.command()
@click.pass_context
def restart(ctx):
    pass



if __name__ == 'main':
    cli(app = Spider())
