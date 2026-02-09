"""
Class for preparing the data for the following MSDR analysis
"""

import pandas as pd
import pytz
from sklearn.preprocessing import StandardScaler
from marginal_emissions import logger

class TimeSeriesProcessor:
    def __init__(self, tso):
        self.scaler = StandardScaler()

    def prepare_data(self, df):
        """
        Prepares input data for later analysis. Index must be set to 'datetime' col.
        :param df: Input dataframe with
        :return df: Processed dataframe
        """
        # Check input data
        print("I. DATA INSPECTION")
        df = self._check_input_data(df)
        self._inspect_input_data(df)

        print("II. DATA PREPARATION")
        # Calculating the delta between two consecutive rows to eliminate trends
        print(f"  1) Calculation delta for time series:")
        delta_df = df - df.shift(1)
        delta_df = delta_df[1:] # Dropping the first row will be NaN

        # Ensure the frequency is set after shifting/dropping
        delta_df = delta_df.asfreq('15min')

        # Fill any NaNs created by asfreq (if gaps existed) or shift
        if delta_df.isnull().values.any():
            # Interpolate is often better than ffill for physical time series
            delta_df = delta_df.interpolate(method='time')
            # If NaNs remain (e.g., at start), drop them
            delta_df = delta_df.dropna()

        print(delta_df.head())

        # Scaling data to have zero mean and unit variance (z-transformation)
        print("")
        print(f"  2) Scaling data to have zero mean and unit variance:")
        delta_df[['total_generation', 'total_emissions']] = self.scaler.fit_transform(delta_df[['total_generation', 'total_emissions']])
        print(delta_df.head())

        # Final inspection
        self._inspect_input_data(delta_df)

        return delta_df

    def inverse_transform(self, df):
        """
        Function to transform scaled data back to the original scale for evaluation.
        :param df:
        :return:
        """
        pass

    @staticmethod
    def _check_input_data(df):
        """
        Check and transform df for further preparation.
        :param df:
        :return df:
        """
        # Check if the index is set to 'datetime' col
        try:
            if not df.index.name == 'datetime':
                try:
                    df.set_index('datetime', inplace=True)
                except ValueError:
                    logger.error("Index must be set to 'datetime' col")
            # Check if the index is set to the datetime type
            if not df.index.dtype == 'datetime64[ns, UTC]':
                try:
                    df.index = pd.to_datetime(df.index, format='ISO8601')
                    df.sort_index(inplace=True)
                except ValueError:
                    logger.error("Failed to set index to datetime")
            # Check if the index is set to the right timezone
            if df.index.tz is None:
                try:
                    df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
                except pytz.exceptions.AmbiguousTimeError:
                    df.index = df.index.tz_localize('Europe/Berlin', ambiguous=True)
                df.index = df.index.tz_convert('UTC')

            return df

        except IndexError:
            logger.error("Failed to prepare index")

    @staticmethod
    def _inspect_input_data(df):
        """
        Print data characteristics for inspection
        :param df:
        :return: None
        """
        print(f"  - Index data type: {df.index.dtype}")
        print(f"  - Duplicate Entries: {(df.index.duplicated()).sum()}")
        print(f"  - Negative Values for Generation: {(df['total_generation'] < 0).sum()}")
        print(f"  - Negative Values for Emissions: {(df['total_emissions'] < 0).sum()}")
        print(f"  - Rows with NaN Values: {(df.isnull().sum()).sum()}")
        print("")



