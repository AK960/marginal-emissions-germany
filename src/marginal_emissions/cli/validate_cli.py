"""
CLI command for running the validation.
"""
import click
import pandas as pd
from pathlib import Path

from marginal_emissions import logger
from marginal_emissions.core.validate import MEFValidator
from marginal_emissions.core.validate_cross_regional import CrossRegionalValidator
from marginal_emissions.vars import RESULTS_DIR, MAX_EF_DICT, DATA_DIR

import numpy as np

def add_max_carbon_intensity_static(df):
    """
    Ermittelt die max_carbon_intensity (gCO2/kWh) und den verantwortlichen
    Energieträger basierend auf den ineffizientesten Einzelanlagen.
    """
    # Threshold in MW (to consider as active)
    threshold = 10

    max_possible_efs = pd.DataFrame(index=df.index)
    
    # Get a list of fuel columns that are actually in the dataframe
    fuel_cols = [col for col in MAX_EF_DICT.keys() if col in df.columns]

    for col in fuel_cols:
        max_ef = MAX_EF_DICT[col]
        max_possible_efs[col] = np.where(df[col] > threshold, max_ef, 0)

    # Calculate the max intensity and identify the source fuel
    df['max_carbon_intensity'] = max_possible_efs[fuel_cols].max(axis=1)
    df['max_carbon_source'] = max_possible_efs[fuel_cols].idxmax(axis=1)
    
    # Correct the source for timestamps where the max intensity is 0
    df.loc[df['max_carbon_intensity'] == 0, 'max_carbon_source'] = 'None'

    return df

def _load_residual_load(tso: str, year: str) -> pd.DataFrame | None:
    """Finds and loads the residual load data from SMARD files."""
    smard_path = DATA_DIR / "raw" / "other" / "smard"
    
    # Find residual load file
    search_pattern = f"realisierter_stromverbrauch_{tso.lower()}_*.csv"
    found_files = list(smard_path.glob(search_pattern))
    if not found_files:
        logger.warning(f"No SMARD residual load file found for {tso} with pattern {search_pattern}")
        return None

    file_path = found_files[0]
    logger.info("Loading residual load ...")

    try:
        df_smard = pd.read_csv(file_path, delimiter=';')
        df_smard.rename(columns={
            'Datum von': 'datetime',
            'Residuallast [MWh] Originalauflösungen': 'net_demand'
        }, inplace=True)

        # Filter out columns that are not needed right away
        df_smard = df_smard[['datetime', 'net_demand']]
        
        # Convert to datetime and filter by year
        df_smard['datetime'] = pd.to_datetime(df_smard['datetime'], format='%d.%m.%Y %H:%M')
        df_smard = df_smard.set_index('datetime')
        df_smard = df_smard[df_smard.index.year == int(year)]

        # Localize and convert timezone
        df_smard.index = df_smard.index.tz_localize('Europe/Berlin', ambiguous='infer').tz_convert('UTC')

        # Clean and convert net_demand
        if df_smard['net_demand'].dtype == 'object':
            cleaned = (
                df_smard['net_demand']
                .str.strip()
                .replace('-', pd.NA)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
            )
            df_smard['net_demand'] = pd.to_numeric(cleaned, errors='coerce')
        
        return df_smard[['net_demand']]

    except Exception as e:
        logger.error(f"Failed to load or process residual load file {file_path.name}: {e}")
        return None

def _get_validation_files(operator, year, is_test, num_iterations):
    """Finds result files based on operator and year."""
    base_path = RESULTS_DIR / ("test/msar" if is_test else "msar")
    operators = [operator.lower()] if operator.lower() != 'all' else ['50hertz', 'amprion', 'tennet', 'transnetbw']
    years = [year] if year.lower() != 'all' else ['2023', '2024']

    files_to_process = []
    for op in operators:
        for yr in years:
            if is_test:
                # Use lowercase for folder names
                folder_name = f"{op}_{yr}_{num_iterations}"
                file_path = base_path / folder_name / "mef_final.csv"
            else:
                # Use lowercase for folder names
                file_path = base_path / op / yr / "mef_final.csv"
            
            if file_path.exists():
                files_to_process.append(file_path)
    
    if not files_to_process:
        logger.warning(f"No result files found for operator(s) {operators}, year(s) {years} "
                       f"(is_test={is_test}, num_iterations={num_iterations if is_test else 'N/A'}).")
    return files_to_process

def _find_processed_file(tso: str, year: str) -> Path | None:
    """Finds the corresponding processed data file for a given TSO and year."""
    processed_path = DATA_DIR / "processed"
    # Use lowercase for file search
    search_pattern = f"final_{tso.lower()}_{year}_*.csv"
    found_files = list(processed_path.glob(search_pattern))
    if found_files:
        return found_files[0]
    logger.warning(f"No processed file found for {tso} {year} with pattern {search_pattern}")
    return None

def _run_validation(file_path: Path, is_test: bool):
    """Runs the validation for a single file."""
    try:
        if is_test:
            parts = file_path.parent.name.split('_')
            tso = parts[0].lower()
            year = parts[1]
        else:
            tso = file_path.parent.parent.name.lower()
            year = file_path.parent.name
        
        # Capitalize for display purposes only, with special handling for 50Hertz
        tso_display = "50Hertz" if tso == "50hertz" else tso.capitalize()
        logger.info(f"Starting validation for {tso_display} in {year}")

        # Get files for validation
        df_results = pd.read_csv(file_path, index_col='timestamp', parse_dates=True)
        processed_file_path = _find_processed_file(tso, year)
        if not processed_file_path:
            raise FileNotFoundError(f"Could not find processed source file for {tso} {year}.")
        df_processed = pd.read_csv(processed_file_path, index_col='datetime', parse_dates=True)

        # Define all potential columns that might be needed
        all_potential_cols = list(MAX_EF_DICT.keys()) + [
            'total_generation_all', 'total_emissions', 'total_generation'
        ]
        # Dynamically filter for columns that actually exist in df_processed
        cols_to_use = [col for col in all_potential_cols if col in df_processed.columns]

        # Merge only the existing columns
        df = pd.merge(df_results, df_processed[cols_to_use], left_index=True, right_index=True, how='inner')

        df_residual = _load_residual_load(tso, year)
        if df_residual is not None:
            df = pd.merge(df, df_residual, left_index=True, right_index=True, how='left')
            logger.info("Successfully merged residual load data.")
        else:
            logger.warning("Could not load residual load data. Net-demand tests will be skipped.")
        
        validation_dir = file_path.parent / "validation"
        validation_dir.mkdir(exist_ok=True)
        logger.info(f"Created validation directory successfully.")

        df = add_max_carbon_intensity_static(df)
        
        validator = MEFValidator(
            data=df,
            tso=tso,  # Pass lowercase tso to validator
            year=year,
            save_dir=validation_dir
        )
        
        validator.run_validation()

        logger.info(f"Finished validation for {tso_display} in {year}\n")

    except Exception as e:
        logger.error(f"Validation for {file_path.name} failed with error: {e}")

@click.group(name='validation')
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
def run_validation_command(operator, year, is_test, num_iterations):
    """Run the validation based on the selected operator and year."""
    files_to_process = _get_validation_files(operator, year, is_test, num_iterations)
    if not files_to_process:
        return

    for file_path in files_to_process:
        _run_validation(file_path, is_test)

@validation_group.command(name='cross-regional')
@click.option(
    '--is-test', '-t',
    is_flag=True,
    help='Flag to indicate a test run.'
)
def cross_regional_command(is_test):
    """Run the cross-regional validation test (Test 2.2)."""
    logger.info("Starting cross-regional validation...")
    validator = CrossRegionalValidator(is_test=is_test)
    
    results = validator.collect_results()
    
    if results:
        correlation = validator.run_correlation_test(results)
        validator.plot_correlation(results, correlation)
        validator.update_individual_summaries(results, correlation)
        logger.info("Cross-regional validation completed successfully.")
    else:
        logger.warning("Cross-regional validation could not be performed.")
