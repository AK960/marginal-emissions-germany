"""
CLI command for running the MSAR analysis.
"""
import click
import pandas as pd
from pyprojroot import here
import os

from marginal_emissions import logger
from marginal_emissions.core.msar import MSARAnalyzer

def _get_analysis_files(operator, year):
    """Finds analysis files based on operator and year."""
    base_path = here() / "data" / "processed"
    all_files = [f for f in base_path.glob("*.csv")]
    
    files_to_process = []

    operators = [operator] if operator.lower() != 'all' else ['50hertz', 'amprion', 'tennet', 'transnetbw']
    years = [year] if year.lower() != 'all' else ['2023', '2024']

    for op in operators:
        for yr in years:
            pattern = f"final_{op.lower()}_{yr}"
            for f in all_files:
                if pattern in f.name:
                    files_to_process.append(f)
    
    if not files_to_process:
        logger.warning(f"No files found for operator(s) {operators} and year(s) {years}.")
        
    return files_to_process

def _run_analysis(file_path, is_test, test_rows):
    """Runs the MSAR analysis for a single file."""
    try:
        tso, year = file_path.stem.split('_')[1:3]
        logger.info(f"Starting analysis for {tso.capitalize()} in {year}")

        df = pd.read_csv(file_path, index_col='datetime', parse_dates=True)
        
        if is_test:
            logger.info(f"PERFORMING TEST RUN with {test_rows} rows.")
            df = df.head(test_rows)

        analyzer = MSARAnalyzer(
            data=df,
            tso=tso.capitalize(),
            year=year,
            test=is_test,
            test_rows=test_rows if is_test else None
        )
        
        analyzer.prepare()
        analyzer.fit_compute()
        
        if analyzer.final_df is not None and not analyzer.final_df.empty:
            analyzer.save_to_file(data=analyzer.final_df, filename='mef_final.csv')
            analyzer.save_to_file(data=analyzer.coeffs_df, filename='coefficients.csv')
            analyzer.save_to_file(data=analyzer.indicators, filename='indicators.json')
        else:
            logger.warning(f"No results generated for {tso.capitalize()} in {year}. Skipping file saving.")

        logger.info(f"Finished analysis for {tso.capitalize()} in {year}")

    except Exception as e:
        logger.error(f"Analysis for {file_path.name} failed with error: {e}")


@click.group(name='analysis')
def analysis_group():
    """Run MSAR analysis for single or all dataframes."""
    pass

@analysis_group.command(name='run')
@click.option(
    '--operator', '-tso',
    type=click.Choice(['50Hertz', 'Amprion', 'TenneT', 'TransnetBW', 'All'], case_sensitive=False),
    default='All',
    help='Select TSO for whose area to run analysis (not case sensitive).'
)
@click.option(
    '--year', '-y',
    type=click.Choice(['2023', '2024', 'All'], case_sensitive=False),
    default='All',
    help='Select year for which to run analysis.'
)
@click.option(
    '--is-test', '-t',
    is_flag=True,
    help='Flag to indicate a test run.'
)
@click.option(
    '--test-rows',
    type=click.IntRange(100, 1000),
    default=1000,
    help='Number of rows to use for the test run (e.g., 100 or 1000).'
)
def run_analysis_command(operator, year, is_test, test_rows):
    """Run the MSAR analysis based on selected operator and year."""
    files_to_process = _get_analysis_files(operator, year)
    if not files_to_process:
        return
    
    for file_path in files_to_process:
        _run_analysis(file_path, is_test, test_rows)
