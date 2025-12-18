import click
from marginal_emissions.vars import AVAILABLE_APIS

@click.command(name='listapis')
def listapis_group():
    """List all available APIs to fetch data from."""

    click.echo(f"Available APIs: {AVAILABLE_APIS}")