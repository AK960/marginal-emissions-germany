"""
Class for validating the MEF time series.
"""
from pyprojroot import here

from marginal_emissions import logger

class MEFValidator:
    def __init__(
        self,
        tso,
        year
    ):
        """
        Initialize a MEF Validator Object
        :param tso: Name of the Transmission System Operator (TSO)
        :param year: Year of the data
        """
        # Base
        self.root = here()
        self.tso = tso
        self.year = year

    # ____________________ Public functions ____________________#
    # Rubric 1: Expected Carbon Intensities (tests if the computed MEF aligns with a naive computation of an empirical annual average)
    def test_non_negativity(self):
        """
        The MEF is not expected to be negative
        """
        pass

    def test_max_carbon_intensity(self):
        """
        The MEF is not expected to exceed the carbon intensity of the most-carbon-intensive plant in that region to that hour
        """
        pass
    # Rubric 2: Empirical Annual Averages Across grid regions (the Annual Average MEF is expected to be positively correlated with the share of coal in the fuel mix)
    def test_ea_mef_alignment(self):
        """
        It is not expected that MEF vastly differs from the Empirical Annual MEF, unless they are biased towards a particular fuel source. To apply the test, mean absolute percentage error rates are calculated across grid regions
        """
        pass

    def test_coal_share_correlation(self):
        """
        Across grid regions, the Annual Average MEF is expected to be positively correlated with the share of coal in the fuel mix.
        """
        pass

    # Rubric 3: Expected net-demand (residual load) temporal patterns (it is expected that MEFs will differ during periods of low vs. high net-demand)
    def test_mef_lower_at_peak(self):
        """
        MEF is lower at peak compared to low demand times, in regions with much coal generation.
        """
        pass

    def test_mef_higher_at_peak(self):
        """
        MEF is higher at peak compared to low demand times, in regions without coal.
        """
        pass

    # Rubric 4: Curtailment (not to be implemented, is inherited in the data and not specifically considered)