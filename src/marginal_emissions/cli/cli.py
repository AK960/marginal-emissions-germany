import click
from .commands import fetch, inspect, listapis

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

cli.add_command(inspect.inspect_group, name='inspect')
#cli.add_command(fetch.fetch_group, name='fetch')
cli.add_command(listapis.listapis_group, name='listapis')

if __name__ == "__main__":
    cli()