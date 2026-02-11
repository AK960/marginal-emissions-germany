"""
Class for performing the MSDR analysis
"""

import os
import warnings
from datetime import datetime

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytz
import statsmodels.api as sm
from joblib import Parallel, delayed
from pyprojroot import here
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.model_selection import ParameterGrid
from sklearn.preprocessing import StandardScaler
from statsmodels.tools.sm_exceptions import ValueWarning, ConvergenceWarning
from tqdm import tqdm

from marginal_emissions import logger

# Suppress specific statsmodels warnings
warnings.simplefilter('ignore', ValueWarning)
# Suppress ConvergenceWarning to avoid cluttering output during the rolling window
warnings.simplefilter('ignore', ConvergenceWarning)
# Suppress RuntimeWarning from numpy, which often happens during optimization of unstable models
warnings.simplefilter('ignore', RuntimeWarning)

class MSDRAnalyzer:
    def __init__(
        self,
        tso, 
        window_length=672, # 1 week = 7*24*4
        param_grid=None,
        n_jobs=-1,
        run = 1
    ):
        """
        Initialize a MSDR Analysis Object
        :param tso: Name of the Transmission System Operator (TSO)
        :param window_length: Size of the rolling window (default: 1 week = 672 quarters)
        :param param_grid: Dictionary for grid search parameters
        :param n_jobs: Number of parallel jobs (-1 for all CPUs)
        """
        # Base
        self.root = here()
        self.tso = tso
        self.run = run
        # Preprocessing
        self.scaler = StandardScaler()
        self.inv_transformer = 0.0
        # Analysis
        ## Params
        self.window_length = window_length
        self.n_jobs = n_jobs
        self.param_grid = param_grid if param_grid is not None else {
            'k_regimes': [2, 3], # Tests 2 or 3 regimes
            'trend': ['c'], # Allows for intercept; captures / absorbs all effects that are not proportional to marginal changes in generation (allows for better fit of slope coefficient) (Default = 'c')
            'order': 0, # Remove autoregression effects (MEF shall be explained by delta generation, not by prev MFE (Default = 0)
            'switching_trend': True, # Allows for different intercept for each regime (Default = True)
            'switching_exog': True, # Allows different slope for each regime (Default = True)
            'switching_variance': True # Allows different variance for each regime (Default = False)
        }
        ## Outcomes
        self.prep_df = None # Prepared data (shifted & scaled)
        self.best_model_results = [] # Will store the best model estimated and its parameters for each timestamp
        self.best_model_coefficients = pd.DataFrame(columns=['Intercept_regime0', 'Intercept_regime1', 'Intercept_regime2', 'Generation_regime0', 'Generation_regime1', 'Generation_regime2']) # Will store mef factors of each regime
        self.df_mef = pd.DataFrame(columns=['Intercept', 'Generation_combined']) # Stores smooth_prop weighed combined intercept and coefficient (total mef)

    def run_msdr_analysis(self, data):
        # TODO: Implement analysis of models (computing key metrics for model evaluation)
        """
        Fits a msdr model for each timestamp in the series. Because it is computationally expensive, a list of the best models is saved in s stateful variable and as file.
        :param data: The full time series dataframe (must contain 'total_emissions' and 'total_generation')
        :return: List of result objects (one per window)
        """
        print(f"Starting MSDR analysis for {self.tso} on {len(data) - self.window_length + 1} rows...")

        # Parallel execution with a progress bar
        self.best_model_results = Parallel(n_jobs=self.n_jobs)(
            delayed(self._process_window)(i, data)
            for i in tqdm(range(len(data) - self.window_length + 1), desc=f"Analyzing {self.tso}...")
        )

        # Saving best model results to file
        self._save_instance_to_file()

        print(f"Analysis for {self.tso} complete.\n")
        return self.best_model_results

    def estimate_emissions(self, data=None):
        """
        Estimates the emission time series using the best model for each window.
        :param data: Optional. The prepared (scaled) dataframe. If None, uses self.prep_df.
        :return: DataFrame with 'estimated_emissions' column
        """
        # TODO: Add check if data is in right shape
        # Check if data was prepared yet
        if data is None:
            data = self.prep_df
        if data is None:
            raise ValueError("No data provided and self.prep_df is not set. Run prepare_data() first.")

        if not self.best_model_results:
            raise ValueError("Analysis not run yet. Call run_msdr_analysis() on prepared data first.")

        estimated_emi = data[['total_emissions']].copy()
        estimated_emi['estimated_emissions'] = np.nan

        # Iterate through results and predict the next step
        for i in range(len(data) - self.window_length + 1):
            reg_win = data.iloc[i: i + self.window_length]
            msdr_results = self.best_model_results[i]

            if msdr_results is not None:
                # Predict for the last point in the window (or next step if forecasting)
                # Based on notebook logic: predict(start=end, end=end)
                forecast = msdr_results.predict(start=reg_win.index[-1], end=reg_win.index[-1])
                estimated_emi.loc[reg_win.index[-1], 'estimated_emissions'] = forecast.iloc[0]
            else:
                print(f'No results for window ending on {reg_win.index[-1]}')

        self._plot_estimated_emissions(estimated_emi)

        return estimated_emi

    def compute_mef(self, data):
        """
        Extracts the Marginal Emission Factor (MEF) from the best models by calculating a weighted average of the regime-specific coefficients based on smoothed probabilities.
        :param data: DataFrame with 'total_emissions' and 'total_generation' columns
        :returns: None, alters stateful object
        """
        iterator = len(data) - self.window_length + 1
        
        for i in range(iterator):
            msdr_results = self.best_model_results[i]
            
            # Timestamp for the result is the END of the window
            timestamp = data.index[i + self.window_length - 1]

            if msdr_results is not None:
                params = msdr_results.params
                
                # 1. Find Intercepts
                intercepts = {}
                for r in range(3): # Check for up to 3 regimes
                    name = f'const[{r}]'
                    if name in params:
                        intercepts[r] = params[name]
                
                # Fallback if no switching intercept (global const)
                if not intercepts and 'const' in params:
                    probs = msdr_results.smoothed_marginal_probabilities.iloc[-1]
                    for r in range(len(probs)):
                        intercepts[r] = params['const']
                
                # 2. Find Generation Coefficients (MEFs)
                gen_coeffs = {}
                for r in range(3): # Check for up to 3 regimes
                    # Possible names for Regime r
                    candidates = [f'x1[{r}]', f'total_generation[{r}]']
                    for name in candidates:
                        if name in params:
                            gen_coeffs[r] = params[name]
                            break
                    
                    # If no regime-specific param found, check for global param
                    if r not in gen_coeffs:
                        if 'x1' in params: gen_coeffs[r] = params['x1']
                        elif 'total_generation' in params: gen_coeffs[r] = params['total_generation']

                # Store raw coefficients for debugging/analysis
                self.best_model_coefficients.loc[timestamp] = [
                    intercepts.get(0, np.nan), 
                    intercepts.get(1, np.nan),
                    intercepts.get(2, np.nan),
                    gen_coeffs.get(0, np.nan), 
                    gen_coeffs.get(1, np.nan), 
                    gen_coeffs.get(2, np.nan)
                ]
                
                # 3. Calculate Weighted Averages (Combined MEF and Intercept)
                probs = msdr_results.smoothed_marginal_probabilities.iloc[-1]
                
                combined_gen_coeff = 0
                combined_intercept = 0
                
                # Iterate over the actual number of regimes found (length of probs)
                for r in range(len(probs)):
                    prob = probs[r]
                    combined_gen_coeff += prob * gen_coeffs.get(r, 0)
                    combined_intercept += prob * intercepts.get(r, 0)
                
                # Store the combined coefficients
                self.df_mef.loc[timestamp] = [combined_intercept, combined_gen_coeff] 
            else:
                # Handle missing results
                self.df_mef.loc[timestamp] = [np.nan, np.nan]

        # Save to file
        self._save_instance_to_file()

    def _process_window(self, i, data):
        """
        Tests many different model parameters to determine the best model for a given rolling time window. For every last timestamp in the window, it returns the best model.
        :param i: Index of the current window
        :param data: DataFrame with 'total_emissions' and 'total_generation' columns
        :returns best_result: Determined model parameters
        """
        current_window = data.iloc[i : i + self.window_length]

        # Ensure the window has the frequency set (important for statsmodels)
        if current_window.index.freq is None:
            current_window = current_window.asfreq('15min')

        best_model = None
        best_mae = np.inf

        for params in ParameterGrid(self.param_grid):
            result, aic, mae = self._fit_markov_model(current_window, params)
            if mae < best_mae:
                best_model = result
                best_mae = mae

        return best_model

    @staticmethod
    def _fit_markov_model(window_data, params):
        """
        Fits a single Markov Regression model for a given window and parameters. The fitted model is used for in-sample prediction and error computation.
        :param window_data: DataFrame with 'total_emissions' and 'total_generation' columns
        :param params: Dictionary with model parameters
        :returns msdr_results: Determined model parameters
        """
        try:
            msdr_model = sm.tsa.MarkovRegression(
                endog=window_data['total_emissions'],
                exog=window_data[['total_generation']],
                k_regimes=params['k_regimes'],
                trend=params['trend'],
                order=params['order'],
                switching_trend=params['switching_trend'],
                switching_exog=params['switching_exog'],
                switching_variance=params['switching_variance']
            )

            # Train model on the window data
            msdr_result = msdr_model.fit(disp=False)

            # Use fitted model parameters from training for prediction
            msdr_predict = msdr_result.predict(start=window_data.index[1], end=window_data.index[-1])

            # Compute MAE (Mean Absolute Error)
            mae = np.mean(np.abs(window_data['total_emissions'] - msdr_predict))

            return msdr_result, msdr_result.aic, mae

        except Exception:
            # logger.error(f"Model fitting failed with error: {e}")
            return None, np.inf, np.inf

    def _plot_estimated_emissions(self, data):
        """
        Plots the estimated vs. original emissions and calculates performance metrics.
        Saves the plot as a PNG file.
        :param data: DataFrame with 'total_emissions' and 'estimated_emissions'
        """
        # Filter data to remove NaNs (e.g., the first window)
        df_plot = data.dropna(subset=['estimated_emissions', 'total_emissions'])
        
        if df_plot.empty:
            print("Warning: No valid data points for plotting (maybe window length > data length?)")
            return

        # Calculate metrics
        r2 = r2_score(df_plot['total_emissions'], df_plot['estimated_emissions'])
        mae = mean_absolute_error(df_plot['total_emissions'], df_plot['estimated_emissions'])
        rmse = np.sqrt(mean_squared_error(df_plot['total_emissions'], df_plot['estimated_emissions']))

        # Create Plot
        plt.figure(figsize=(12, 6))
        plt.plot(df_plot.index, df_plot['total_emissions'], label='Original Emissions (Scaled)', alpha=0.7)
        plt.plot(df_plot.index, df_plot['estimated_emissions'], label='Model Estimation (Scaled)', alpha=0.7, linestyle='--')

        plt.title(f"MSDR Model Validation - {self.tso}\nR² = {r2:.4f} | MAE = {mae:.4f} | RMSE = {rmse:.4f}")
        plt.ylabel("Scaled Emissions")
        plt.xlabel("Time")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Save plot
        try:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            save_dir = self.root / "results" / "figures"
            os.makedirs(save_dir, exist_ok=True)
            
            filename = save_dir / f"{self.tso}_run_{self.run}_msdr_validation_{timestamp_str}.png"
            plt.savefig(filename)
            print(f"Validation plot saved to {filename}")
            plt.close() # Close figure to free memory
        except Exception as e:
            logger.error(f"Failed to save image to file: {e}. Continuing...")
            plt.close() # Ensure the figure is closed even on error

    def _save_instance_to_file(self, filename=None):
        """
        Saves the current analyzer instance to a file using joblib.
        :param filename: Optional filename. If None, saves to results/models/{tso}_analyzer_models.pkl
        """
        if filename is None:
            save_dir = self.root / "results" / "models"
            os.makedirs(save_dir, exist_ok=True)
            filename = save_dir / f"{self.tso}_run_{self.run}_analyzer_models.pkl"
        
        try:
            joblib.dump(self, filename)
            print(f"Model saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save model: {e}")

    def load_instance_from_file(self, name):
        """
        Loads a saved MSDRAnalysis instance.
        :param name: File of the model that shall be loaded. Only input the filename string.
        :return: MSDRAnalysis instance
        """
        try:
            filepath = f"{self.root}/results/models/{name}"
            return joblib.load(filepath)
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return None

    #-------------------- Preprocessing --------------------#
    def prepare_data(self, df):
        """
        Prepares input data for later analysis. Index must be set to 'datetime' col.
        :param df: Input dataframe with
        :return df: Processed dataframe
        """
        try:
            # Check input data
            print("I. DATA INSPECTION")
            df = self._set_index(df)
            self._inspect_data(df)

            print("II. DATA PREPARATION")
            # Calculating the delta between two consecutive rows to eliminate trends
            print(f"  1) Calculation delta for time series:")
            delta_df = df - df.shift(1)
            delta_df = delta_df[1:] # Dropping the first row will be NaN

            # Ensure the frequency and index are set after shifting/dropping
            delta_df = delta_df.asfreq('15min')
            delta_df = self._set_index(delta_df)

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
            delta_df[['total_generation', 'total_emissions']] = self.scaler.fit_transform(
                delta_df[['total_generation', 'total_emissions']]
            )
            self.inv_transformer = self.scaler.scale_[1] / self.scaler.scale_[0]

            print(delta_df.head())

            # Final inspection
            self._inspect_data(delta_df)

            # Save to instance
            self.prep_df = delta_df

        except Exception as e:
            logger.error(f"Failed to prepare data: {e}")

        return self.prep_df

    def inverse_transform(self, df):
        # TODO: Add function based on sd_emi and sd_gen
        """
        Function to transform scaled data back to the original scale for evaluation.
        :param df:
        :return:
        """
        pass

    @staticmethod
    def _set_index(df):
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
    def _inspect_data(df):
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
