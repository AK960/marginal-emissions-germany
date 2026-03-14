"""
Class for validating the MEF time series.
"""
from marginal_emissions import logger
from pathlib import Path

class MEFValidator:
    def __init__(self, data, tso, year, save_dir: Path):
        """
        Initialize a MEF Validator Object.
        :param data: DataFrame with the final MEF results.
        :param tso: Name of the Transmission System Operator (TSO).
        :param year: The year of the data.
        :param save_dir: The directory where validation results should be saved.
        """
        # Base
        self.df = data
        self.tso = tso
        self.year = year
        self.save_dir = save_dir
        # Validation Input
        logger.info(f"Initialized validator for {self.tso} ({self.year}) with {len(self.df)} rows.")
        logger.info(f"Validation results will be saved to: {self.save_dir}")


    # ____________________ Entrypoint ____________________#
    def run_validation(self):
        logger.info("Running validation checks...")
        # Placeholder for future validation logic
        # Example of how you could use the save_dir:
        # self.df.head().to_csv(self.save_dir / "sample_output.csv")
        logger.info("Validation checks completed (placeholder).")

    # ____________________ Validation Functions ____________________#
    # Rubric 1: Expected Carbon Intensities (tests if the computed MEF aligns with a naive computation of an empirical annual average)
    def _test_non_negativity(self):
        """
        The MEF is not expected to be negative
        """
        pass

    def _test_max_carbon_intensity(self):
        """
        The MEF is not expected to exceed the carbon intensity of the most-carbon-intensive plant in that region to that hour
        """
        pass
    # Rubric 2: Empirical Annual Averages Across grid regions (the Annual Average MEF is expected to be positively correlated with the share of coal in the fuel mix)
    def _test_ea_mef_alignment(self):
        """
        It is not expected that MEF vastly differs from the Empirical Annual MEF, unless they are biased towards a particular fuel source. To apply the test_msdr, mean absolute percentage error rates are calculated across grid regions
        """
        pass

    def _test_coal_share_correlation(self):
        """
        Across grid regions, the Annual Average MEF is expected to be positively correlated with the share of coal in the fuel mix.
        """
        pass

    # Rubric 3: Expected net-demand (residual load) temporal patterns (it is expected that MEFs will differ during periods of low vs. high net-demand)
    def _test_mef_lower_at_peak(self):
        """
        MEF is lower at peak compared to low demand times, in regions with much coal generation.
        """
        pass

    def _test_mef_higher_at_peak(self):
        """
        MEF is higher at peak compared to low demand times, in regions without coal.
        """
        pass

    # Rubric 4: Curtailment (not to be implemented, is inherited in the data and not specifically considered)
