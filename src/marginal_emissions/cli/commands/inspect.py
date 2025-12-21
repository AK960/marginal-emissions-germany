import click
from marginal_emissions.utils.helper import get_all_subdirs

@click.group(name='inspect')
def inspect_group():
    """Inspect data from an API."""
    pass

@inspect_group.command(name='dirs')
@click.option(
    '--path', '-p',
    default='./data',
    help='Base path to inspect'
)
def inspect_dirs(path):
    """List all directories in a given path."""
    click.echo(f"Inspecting directories in {path}")

    directories = get_all_subdirs(path)

    if not directories:
        click.echo("No directories found.")
        return

    for d in directories:
        try:
            display_path = d.relative_to(path)
            click.echo(f"./{display_path}")
        except ValueError:
            click.echo(f"./{d}")