"""
CLI command for running the MSAR analysis.
"""
import click
import pandas as pd

from marginal_emissions import logger
from marginal_emissions.core.analyze_msar import MSARAnalyzer
from marginal_emissions.vars import DATA_DIR

# Constants from the MSAR model
WINDOW_SIZE = 672
STEP_SIZE = 32

def _get_analysis_files(operator, year):
    """Finds analysis files based on operator and year."""
    base_path = DATA_DIR / "processed"
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

def _run_analysis(file_path, is_test, num_iterations):
    """Runs the MSAR analysis for a single file."""
    try:
        tso, year = file_path.stem.split('_')[1:3]
        logger.info(f"Starting analysis for {tso.capitalize()} in {year}")

        rows_to_load = None
        if is_test:
            # Calculate the number of rows needed to perform the requested number of iterations
            rows_to_load = WINDOW_SIZE + (num_iterations - 1) * STEP_SIZE
            logger.info(f"PERFORMING TEST RUN with {num_iterations} iterations, loading {rows_to_load} rows.")

        df = pd.read_csv(file_path, index_col='datetime', parse_dates=True, nrows=rows_to_load)
        
        # Additional check to ensure enough data was loaded
        if is_test and len(df) < rows_to_load:
            logger.warning(f"Not enough data in {file_path.name} to perform {num_iterations} iterations. "
                           f"File has {len(df)} rows, but {rows_to_load} are needed.")
            return

        analyzer = MSARAnalyzer(
            data=df,
            tso=tso.capitalize(),
            year=year,
            test=is_test,
            test_rows=rows_to_load if is_test else None,
            num_iterations=num_iterations if is_test else None
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
    '--num-iterations',
    type=click.IntRange(min=1),
    default=50,
    help='Number of sliding window iterations for the test run.'
)
def run_analysis_command(operator, year, is_test, num_iterations):
    """Run the MSAR analysis based on the selected operator and year."""
    files_to_process = _get_analysis_files(operator, year)
    if not files_to_process:
        return

    for file_path in files_to_process:
        _run_analysis(file_path, is_test, num_iterations)
