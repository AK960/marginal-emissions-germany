import click

from .commands.inspect import inspect_group
from .commands.listapis import listapis_group
from .commands.fetch import *

@click.group()
@click.version_option(version="1.0.0")
@click.option('--verbose', '-v', is_flag=True, help='Ausführliche Ausgabe')
@click.pass_context
def cli(ctx, verbose):
    """
    Marginal-Emissions CLI Tool

    Use 'mef-tool <command> --help' for more information.
    """
    ctx.ensure_object(dict)
    ctx.obj = {'VERBOSE': verbose}

cli.add_command(inspect_group, name='inspect')
cli.add_command(fetch_group, name='fetch')
cli.add_command(listapis_group, name='listapis')
#cli.add_command(run_group, name='run') # TODO: Implement run_group to process data and print report

if __name__ == "__main__":
    cli()