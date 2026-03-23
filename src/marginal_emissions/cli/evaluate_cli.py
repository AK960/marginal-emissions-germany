"""
CLI for evaluating MEF results.
"""
import click
from marginal_emissions.core.evaluate import MEFEvaluator

TSO_CHOICES = click.Choice(['50Hertz', 'Amprion', 'TenneT', 'TransnetBW', 'All'], case_sensitive=False)

@click.group()
def evaluation():
    """Commands for evaluating MEF results."""
    pass

@evaluation.command()
@click.option(
    '--tso',
    type=TSO_CHOICES,
    default='All',
    help='Select TSO for whose area to run evaluation (not case sensitive).'
)
@click.option(
    '--skip-fitting',
    is_flag=True,
    help='Skip fitting the global MSAR model and only generate plots.'
)
def run(tso, skip_fitting):
    """Run the MEF evaluation for one or all TSOs."""
    tsos_to_run = [tso.lower()] if tso.lower() != 'all' else ['50hertz', 'amprion', 'tennet', 'transnetbw']
    
    for tso_val in tsos_to_run:
        tso_display = "50Hertz" if tso_val == "50hertz" else tso_val.capitalize()
        click.echo(f"Running evaluation for {tso_display}...")
        try:
            evaluator = MEFEvaluator(tso=tso_val, skip_fitting=skip_fitting)
            evaluator.run_evaluation()
        except Exception as e:
            click.echo(f"Failed to evaluate {tso_display}: {e}", err=True)
