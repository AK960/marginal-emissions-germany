import click

from marginal_emissions.cli.validate_cli import validation_group
from marginal_emissions.cli.inspect_cli import inspect_group
from marginal_emissions.cli.listapis_cli import listapis_group
from marginal_emissions.cli.fetch_cli import fetch_group
from marginal_emissions.cli.preprocess_cli import prep
from marginal_emissions.cli.analyze_cli import analysis_group
from marginal_emissions.cli.evaluate_cli import evaluation
from importlib.metadata import version

@click.group()
@click.version_option(version=version("marginal-emissions-germany"))
@click.option('--verbose', '-v', is_flag=True, help='Extended Output')
@click.pass_context
def cli(ctx, verbose):
    """
    Marginal-Emissions CLI Tool

    Use 'mef <command> --help' for more information.
    """
    ctx.ensure_object(dict)
    ctx.obj = {'VERBOSE': verbose}

# noinspection PyTypeChecker
def register_commands():
    cli.add_command(inspect_group, name='inspect')
    cli.add_command(fetch_group, name='fetch')
    cli.add_command(listapis_group, name='listapis')
    cli.add_command(prep)
    cli.add_command(analysis_group, name='analysis')
    cli.add_command(validation_group, name='validation')
    cli.add_command(evaluation, name='evaluation')

register_commands()

if __name__ == "__main__":
    cli()
