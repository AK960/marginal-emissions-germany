import click

from marginal_emissions.cli.validate_cli import validation_group
from marginal_emissions.cli.inspect_cli import inspect_group
from marginal_emissions.cli.listapis_cli import listapis_group
from marginal_emissions.cli.fetch_cli import fetch_group
from marginal_emissions.cli.preprocess_cli import prep
from marginal_emissions.cli.analyze_cli import analysis_group

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
# noinspection PyTypeChecker
cli.add_command(inspect_group, name='inspect')
cli.add_command(fetch_group, name='fetch')
cli.add_command(listapis_group, name='listapis')
# cli.add_command(synchtex_group, name='synchtex')
cli.add_command(prep)
cli.add_command(analysis_group, name='analysis')
cli.add_command(validation_group, name='validation')

if __name__ == "__main__":
    cli()
