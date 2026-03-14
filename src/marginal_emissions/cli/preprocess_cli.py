"""
CLI command for data preprocessing.
"""
import click

from marginal_emissions.core.preprocess import MEFPreprocessor
from marginal_emissions import logger

@click.command()
@click.option('--skip-validation', is_flag=True, help="Skip the validation step after allocation.")
def prep(skip_validation):
    """
    Run the data preprocessing pipeline.

    This command will:
    1. Prepare the emissions data.
    2. Prepare the generation data for all TSOs.
    3. Allocate emissions to the TSO level.
    4. Validate the allocation (can be skipped).
    """
    logger.info("Starting data preprocessing...")
    preprocessor = MEFPreprocessor()

    try:
        logger.info("Step 1: Preparing emissions data...")
        preprocessor.prep_emissions()
        logger.info("Emissions data prepared successfully.")

        logger.info("Step 2: Preparing generation data...")
        preprocessor.prep_generation()
        logger.info("Generation data prepared successfully.")

        logger.info("Step 3: Allocating emissions...")
        regional_emissions = preprocessor.alloc_emissions()
        logger.info("Emissions allocated successfully.")

        if not skip_validation:
            logger.info("Step 4: Validating allocation...")
            preprocessor.validate_allocation(regional_emissions)
            logger.info("Allocation validated successfully.")
        else:
            logger.info("Step 4: Skipping validation.")

        logger.info("Data preprocessing completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during preprocessing: {e}")
        raise click.Abort()
