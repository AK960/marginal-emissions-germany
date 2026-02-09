"""
Class for performing the MSDR analysis
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.regime_switching.markov_regression import MarkovRegression
import matplotlib.pyplot as plt
import pytz
import warnings
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import ParameterGrid
from joblib import Parallel, delayed
import warnings
from statsmodels.tools.sm_exceptions import ValueWarning, ConvergenceWarning
from marginal_emissions import logger
from tqdm import tqdm

# Suppress specific statsmodels warnings
warnings.simplefilter('ignore', ValueWarning)
# Suppress ConvergenceWarning to avoid cluttering output during the rolling window
warnings.simplefilter('ignore', ConvergenceWarning)
# Suppress RuntimeWarning from numpy, which often happens during optimization of unstable models
warnings.simplefilter('ignore', RuntimeWarning)

class MSDRAnalysis:
    def __init__(
        self,
        window_length=672, # 1 week = 7*24*4
        param_grid=None,
        n_jobs=-1
    ):
        """
        Initialize the MSDR Analysis.
        :param window_length: Size of the rolling window (default: 1 week = 672 quarters)
        :param param_grid: Dictionary for grid search parameters
        :param n_jobs: Number of parallel jobs (-1 for all CPUs)
        """
        self.window_length = window_length
        self.param_grid = param_grid if param_grid is not None else {
            'k_regimes': [2, 3],
            'switching_variance': [True, False],
            'trend': ['c', 't'],
            'regularization': [None, 'l1', 'l2'],
            'penalty': [0.01, 0.1, 1.0]
        }
        self.n_jobs = n_jobs
        self.best_model_results = [] # Will store the best model estimated and its parameters for each timestamp
        self.best_model_coefficients = pd.DataFrame(columns=['Intercept', 'Generation_regime1', 'Generation_regime2', 'Generation_regime3'])
        self.df_mef = pd.DataFrame(columns=['Intercept', 'Generation_combined'])

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
                k_regimes=params['k_regimes'],
                trend=params['trend'],
                exog=window_data[['total_generation']], 
                switching_variance=params['switching_variance']
            )

            # Train model on the window data
            msdr_result = msdr_model.fit(disp=False)

            # Use fitted model parameters from training for prediction
            msdr_predict = msdr_result.predict(start=window_data.index[1], end=window_data.index[-1])

            # Compute MAE (Mean Absolute Error)
            mae = np.mean(np.abs(window_data['total_emissions'] - msdr_predict))

            return msdr_result, msdr_result.aic, mae

        except Exception as e:
            # logger.error(f"Model fitting failed with error: {e}")
            return None, np.inf, np.inf

    def _process_window(self, i, data):
        """
        Tests many different model parameters to determine the best model for a given rolling time window. For each window it returns the best model and a list of its parameters.
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
    
    def _compute_mef(self, data):
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
                
                # 1. Find Intercept
                intercepts = {}
                for r in range(3):
                    name = f'const[{r}]'
                    if name in params:
                        intercepts[r] = params[name]
                
                # Fallback if no switching intercept (global const)
                if not intercepts and 'const' in params:
                    # Assign global const to all regimes present
                    # We need to know how many regimes we have. We can infer from probs length.
                    probs = msdr_results.smoothed_marginal_probabilities.iloc[-1]
                    for r in range(len(probs)):
                        intercepts[r] = params['const']
                
                # 2. Find Generation Coefficients (MEFs)
                gen_coeffs = {}
                for r in range(3):
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

                # Store raw coefficients for debugging/analysis (fill missing with NaN or 0)
                self.best_model_coefficients.loc[timestamp] = [
                    intercepts.get(0, np.nan), 
                    gen_coeffs.get(0, np.nan), 
                    gen_coeffs.get(1, np.nan), 
                    gen_coeffs.get(2, np.nan)
                ]
                
                # 3. Calculate Weighted Averages (Combined MEF)
                probs = msdr_results.smoothed_marginal_probabilities.iloc[-1]
                
                combined_gen_coeff = 0
                combined_intercept = 0
                
                # Iterate over the actual number of regimes found (length of probs)
                for r in range(len(probs)):
                    prob = probs[r]
                    # Add weighted MEF
                    combined_gen_coeff += prob * gen_coeffs.get(r, 0)
                    # Add weighted Intercept
                    combined_intercept += prob * intercepts.get(r, 0)
                
                # Store the combined coefficients
                self.df_mef.loc[timestamp] = [combined_intercept, combined_gen_coeff] 
            else:
                # Handle missing results
                self.df_mef.loc[timestamp] = [np.nan, np.nan]

    def run_msdr_analysis(self, tso, data):
        """
        Runs the rolling window analysis in parallel.
        :param tso: Name of the Transmission System Operator (TSO)
        :param data: The full time series dataframe (must contain 'total_emissions' and 'total_generation')
        :return: List of result objects (one per window)
        """
        print(f"Starting MSDR analysis for {tso} on {len(data)} rows...")

        # Parallel execution with a progress bar
        self.best_model_results = Parallel(n_jobs=self.n_jobs)(
            delayed(self._process_window)(i, data)
            for i in tqdm(range(len(data) - self.window_length + 1), desc=f"Analyzing {tso}...")
        )
        
        # Compute MEF from weighed coefficients in self.best_model_results
        print("Computing MEF from model results...")
        self._compute_mef(data=data)        

        print("Analysis complete.")
        return self.best_model_results

    def estimate_emissions(self, data):
        """
        Estimates the emission time series using the best model for each window.
        :param data: The original data dataframe used for analysis
        :return: DataFrame with 'estimated_emissions' column
        """
        if not self.best_model_results:
            raise ValueError("Analysis not run yet. Call run_msdr_analysis() on prepared data first.")

        estimated_emi = data[['total_emissions']].copy()
        estimated_emi['estimated_emissions'] = np.nan

        # Iterate through results and predict the next step
        for i in range(len(data) - self.window_length + 1):
            reg_win = data.iloc[i : i + self.window_length]
            msdr_results = self.best_model_results[i]

            if msdr_results is not None:
                # Predict for the last point in the window (or next step if forecasting)
                # Based on notebook logic: predict(start=end, end=end)
                forecast = msdr_results.predict(start=reg_win.index[-1], end=reg_win.index[-1])
                estimated_emi.loc[reg_win.index[-1], 'estimated_emissions'] = forecast.iloc[0]
            else:
                print(f'No results for window ending on {reg_win.index[-1]}')
        
        return estimated_emi
