"""
CLI command for running the validation.
"""
import click
import pandas as pd
from pathlib import Path

from marginal_emissions import logger
from marginal_emissions.core.validate import MEFValidator
from marginal_emissions.vars import RESULTS_DIR

def _get_validation_files(operator, year):
    """Finds result files based on operator and year."""
    base_path = RESULTS_DIR / "msar"
    
    operators = [operator.lower()] if operator.lower() != 'all' else ['50hertz', 'amprion', 'tennet', 'transnetbw']
    years = [year] if year.lower() != 'all' else ['2023', '2024']

    files_to_process = []
    for op in operators:
        for yr in years:
            # Correctly construct the path and check for existence
            file_path = base_path / op / yr / "mef_final.csv"
            if file_path.exists():
                files_to_process.append(file_path)
    
    if not files_to_process:
        logger.warning(f"No result files found for operator(s) {operators} and year(s) {years}.")
        
    return files_to_process

def _run_validation(file_path: Path):
    """Runs the validation for a single file."""
    try:
        # Extract TSO and year from the file path structure
        tso = file_path.parent.parent.name
        year = file_path.parent.name
        logger.info(f"Starting validation for {tso.capitalize()} in {year}")

        # Create the validation directory
        validation_dir = file_path.parent / "validation"
        validation_dir.mkdir(exist_ok=True)
        logger.info(f"Created validation directory: {validation_dir}")

        df = pd.read_csv(file_path, index_col='datetime', parse_dates=True)
        
        validator = MEFValidator(
            data=df,
            tso=tso.capitalize(),
            year=year,
            save_dir=validation_dir  # Pass the directory to the validator
        )
        
        validator.run_validation()

        logger.info(f"Finished validation for {tso.capitalize()} in {year}")

    except Exception as e:
        logger.error(f"Validation for {file_path.name} failed with error: {e}")


@click.group(name='validate')
def validation_group():
    """Run validation for single or all result dataframes."""
    pass

@validation_group.command(name='run')
@click.option(
    '--operator', '-tso',
    type=click.Choice(['50Hertz', 'Amprion', 'TenneT', 'TransnetBW', 'All'], case_sensitive=False),
    default='All',
    help='Select TSO for whose area to run validation (not case sensitive).'
)
@click.option(
    '--year', '-y',
    type=click.Choice(['2023', '2024', 'All'], case_sensitive=False),
    default='All',
    help='Select year for which to run validation.'
)
def run_validation_command(operator, year):
    """Run the validation based on the selected operator and year."""
    files_to_process = _get_validation_files(operator, year)
    if not files_to_process:
        return

    for file_path in files_to_process:
        _run_validation(file_path)
