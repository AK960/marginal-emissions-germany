"""
Class for performing the MSDR analysis.
"""

import json
import os
import warnings
from pathlib import Path

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
        year,
        data,
        window_length=672, # 1 week = 7*24*4
        param_grid=None,
        n_jobs=-1,
        run = '1'
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
        self.year = year
        self.run = run # Number of the run to track progress
        # Preprocessing
        self.scaler = StandardScaler()
        # Analysis
        ## Data
        self.df = data
        ## Params
        self.window_length = window_length
        self.n_jobs = n_jobs
        self.param_grid = param_grid if param_grid is not None else {
            'k_regimes': [2, 3], # Tests 2 or 3 regimes
            'trend': ['c'], # Allows for intercept; captures / absorbs all effects that are not proportional to marginal changes in generation (allows for better fit of slope coefficient) (Default = 'c')
            'order': [1], # Remove autoregression effects (MEF shall be explained by delta generation, not by prev MFE (Default = 0)
            'switching_trend': [True], # Allows for different intercept for each regime (Default = True)
            'switching_exog': [True], # Allows different slope for each regime (Default = True)
            'switching_variance': [True]#, # Allows different variance for each regime (Default = False)
            # regularization: ['l1', 'l2']
        }
        ## Outcomes
        self.indicators = []            # Contains indicators of the best model for evaluation --> not flat thus list not df
        self.prep_df = None             # Shifted and z-transformed original df
        self.estimated_emi = None       # DataFrame with estimated emissions from predict(), used to plot true vs. estimated emissions
        self.best_model_results = []    # Will store a MarkovRegressionWrapper Object for each best model per timestamp, used for prediction
        self.best_maes = []             # Stores best maes from chosen best model in fit()
        self.df_mef_scaled = pd.DataFrame(columns=['intercept_scaled', 'mef_scaled']) # Stores smooth_prop weighed combined intercept and coefficient (total mef)
        self.df_mef_absolute = None

    # ____________________ Public functions ____________________#
    def prepare(self):
        """
        Prepares input data for later analysis.
        """
        logger.info("Preparing data...")
        try:
            # Check input data
            df = self._set_types(self.df)

            # Ensure the frequency and index are set after shifting/dropping
            df = df.asfreq('15min')
            df = self._set_types(df)

            # Fill any NaNs created by asfreq (if gaps existed) or shift
            if df.isnull().values.any():
                # Interpolate is often better than ffill for physical time series
                df = df.interpolate(method='time')
                # If NaNs remain (e.g., at start), drop them
                df = df.dropna()

            # Scaling data to have zero mean and unit variance (z-transformation)
            df[['delta_generation', 'delta_emissions']] = self.scaler.fit_transform(
                df[['delta_generation', 'delta_emissions']]
            )

            # Print inspection
            self._inspect_data(df)

            # Set state
            self.prep_df = df

        except Exception as e:
            logger.error(f"Failed to prepare data: {e}")

        return self.prep_df

    def fit(self):
        """
        Fits a msdr model for each timestamp in the time series.
        """
        logger.info(f"Fitting model for {len(self.prep_df) - self.window_length + 1} observations...")
        try:
            # Parallel execution with a progress bar
            results = Parallel(n_jobs=self.n_jobs)(
                delayed(self._process_window)(i, self.prep_df)
                for i in tqdm(range(len(self.prep_df) - self.window_length + 1), desc=f"Analyzing {self.tso}")
            )
            self.best_model_results, self.best_maes = zip(*results)
        except Exception as e:
            logger.error(f"Failed to run analysis. Exit with error: {e}")

    def predict(self):
        """
        Estimates the emission time series using the best model for each window.
        """
        if self.prep_df.empty:
            raise ValueError("Data not prepared yet. Call prepare() first.")
        if not self.best_model_results:
            raise ValueError("Analysis not run yet. Call fit() on prepared data first.")

        logger.info("Performing in-sample prediction with best models...")
        self.estimated_emi = self.prep_df[['delta_emissions']].copy()
        self.estimated_emi['estimated_emissions'] = np.nan

        # Iterate through results and predict the next step
        for i in range(len(self.prep_df) - self.window_length + 1):
            reg_win = self.prep_df.iloc[i: i + self.window_length]
            msdr_results = self.best_model_results[i]

            if msdr_results is not None:
                # Predict for the last point in the window (or next step if forecasting)
                # Based on notebook logic: predict(start=end, end=end)
                forecast = msdr_results.predict(start=reg_win.index[-1], end=reg_win.index[-1])
                self.estimated_emi.loc[reg_win.index[-1], 'estimated_emissions'] = forecast.iloc[0]
            else:
                logger.warning(f'No results for window ending on {reg_win.index[-1]}')

        logger.info("Plotting estimated emissions...")
        self._save_to_file(data=self.estimated_emi, sub_dir='tables', filename='df_estimated_emissions.csv')
        self._plot_estimated_emissions()

    def compute(self):
        """
        Computes the Marginal Emission Factor (MEF) from the best models by calculating a weighted average of the regime-specific coefficients based on smoothed probabilities.
        """
        if self.prep_df.empty:
            raise ValueError("Data not prepared yet. Call prepare() first.")
        if not self.best_model_results:
            raise ValueError("Analysis not run yet. Call fit() on prepared data first.")

        logger.info("Computing MEF...")

        # Loop vars
        iterator = len(self.prep_df) - self.window_length + 1

        for i in range(iterator):
            msdr_results = self.best_model_results[i]
            mae = self.best_maes[i]
            timestamp = self.prep_df.index[i + self.window_length - 1] # Timestamp for the result is the END of the window

            if msdr_results is not None:
                # For each timestamp, save .summary() coefficients in list
                df_summary_coeffs = pd.concat(
                    [
                        msdr_results.params,    # coef
                        msdr_results.bse,       # std_err
                        msdr_results.tvalues,   # z
                        msdr_results.pvalues,   # P>|z|
                        msdr_results.conf_int() # Conf_int [0.025, 0.975]
                    ],
                    axis=1
                )
                df_summary_coeffs.columns = ['coef', 'std_err', 'tval', 'pval', 'ci_lower', 'ci_upper']

                indicator_row = {
                    'timestamp': timestamp,
                    'coeffs': df_summary_coeffs.to_dict(orient='index'),
                    'k_regimes': int(msdr_results._results.k_regimes),
                    'smoothed_probs': msdr_results.smoothed_marginal_probabilities.iloc[-1].to_dict(),
                    'mae': float(mae),
                    'aic': float(msdr_results.aic),    # aic = 2k - 2 ln(L) // k = no. model params, L = max Log Likelihood function (no params vs. model fit)
                    'bic': float(msdr_results.bic),
                    'hqic': float(msdr_results.hqic),
                    'llf': float(msdr_results.llf),
                    'mle_converged': bool(msdr_results.mle_retvals['converged'])
                }
                self.indicators.append(indicator_row)

                # For extracting coeffs and iterating
                params = msdr_results.params.to_dict()
                smoothed_probs = indicator_row['smoothed_probs']

                # 1. Find Intercepts
                intercepts = {}
                for r in range(3): # Check for up to 3 regimes
                    name = f'const[{r}]'
                    if name in params:
                        intercepts[r] = params[name]
                
                # Fallback if no switching intercept (global const)
                if not intercepts and 'const' in params:
                    for r in range(len(smoothed_probs)):
                        intercepts[r] = params['const']
                
                # 2. Find Generation Coefficients (MEFs)
                gen_coeffs = {}
                for r in range(3): # Check for up to 3 regimes
                    # Possible names for Regime r
                    candidates = [f'x1[{r}]', f'delta_generation[{r}]']
                    for name in candidates:
                        if name in params:
                            gen_coeffs[r] = params[name]
                            break
                    
                    # If no regime-specific param found, check for global param
                    if r not in gen_coeffs:
                        if 'x1' in params: gen_coeffs[r] = params['x1']
                        elif 'delta_generation' in params: gen_coeffs[r] = params['delta_generation']

                # 3. Calculate Weighted Averages (Combined MEF and Intercept)
                combined_gen_coeff = 0
                combined_intercept = 0
                
                # Iterate over the actual number of regimes found (length of probs)
                for r in range(len(smoothed_probs)):
                    prob = smoothed_probs[r]
                    combined_gen_coeff += prob * gen_coeffs.get(r, 0)
                    combined_intercept += prob * intercepts.get(r, 0)
                
                # Store the combined coefficients
                self.df_mef_scaled.loc[timestamp] = {
                    'intercept_scaled': combined_intercept,
                    'mef_scaled': combined_gen_coeff
                }

            else:
                # Handle missing results
                logger.warn(f"No results for timestamp {timestamp}. Writing NaN.")
                self.df_mef_scaled.loc[timestamp] = {
                    'intercept_scaled': np.nan,
                    'mef_scaled': np.nan
                }

        # Save to file
        self._save_to_file(data=self.df_mef_scaled, sub_dir='tables', filename='df_mef_scaled.csv')
        self._save_to_file(data=self.indicators, sub_dir='summary', filename='indicators.json')
        self._inverse_transform_mef()

    def merge_mef(self):
        """
        Merges the calculated absolute MEF back to the original input data.
        Returns a DataFrame with the original 'delta_generation', 'delta_emissions' and the new 'MEF'.
        :return merged_df:
        """
        if self.prep_df.empty:
            raise ValueError("Data not prepared yet. Call prepare() first.")
        if self.df_mef_scaled.empty:
            raise ValueError("MEF not computed yet. Call compute() first.")

        logger.info("Merging MEF back to original data...")

        # Merge back to original data & remove training data (does not have MEF value)
        merged_df = self.df.join(
            self.df_mef_absolute[['mef_t_MWh', 'mef_g_kWh', 'intercept']],
            how='left'
        )
        merged_df.dropna(subset=['mef_t_MWh'], inplace=True)

        self._save_to_file(data=merged_df, sub_dir='tables', filename='df_mef_final.csv')
        return merged_df

    # ____________________ Private functions ____________________#
    # ---------- Model fitting ----------#
    def _process_window(self, i, data):
        """
        Tests many different model parameters to determine the best model for a given rolling time window. For every last timestamp in the window, it returns the best model.
        :param i: Index of the current window
        :param data: DataFrame with 'delta_emissions' and 'delta_generation' columns
        :returns best_result: Determined model parameters
        """
        current_window = data.iloc[i : i + self.window_length]

        # Ensure the window has the frequency set (important for statsmodels)
        if current_window.index.freq is None:
            current_window = current_window.asfreq('15min')

        # Model selection params
        best_converged = False
        best_model = None
        best_mae = np.inf
        best_aic = np.inf

        for params in ParameterGrid(self.param_grid):
            result, aic, mae = self._fit_markov_model(current_window, params)

            if result is None:
                continue

            # Check if the model converged
            is_converged = result.mle_retvals['converged']

            ## mae model selection
            """
            # Case 1: No best model yet
            if best_model is None:
                best_model = result
                best_mae = mae
                best_aic = aic
                best_converged = is_converged
            # Case 2: The new model converged, the old did not -> Take converged one
            elif is_converged and not best_converged:
                best_model = result
                best_mae = mae
                best_aic = aic
                best_converged = True
            # Case 3: Both converged / did not converge -> Take the one with better mae
            elif is_converged == best_converged:
                if mae < best_mae:
                    best_model = result
                    best_mae = mae
            """
            ## aic model selection
            # Case 1: No best model yet
            if best_model is None:
                best_model = result
                best_mae = mae
                best_aic = aic
                best_converged = is_converged

            # Case 2: The new model converged, the old did not -> Take converged one
            elif is_converged and not best_converged:
                best_model = result
                best_mae = mae
                best_aic = aic
                best_converged = True

            # Case 3: Both converged / did not converge -> Take the one with better aic
            elif is_converged == best_converged:
                if aic < best_aic:
                    best_model = result
                    best_mae = mae
                    best_aic = aic

        return best_model, best_mae

    @staticmethod
    def _fit_markov_model(window_data, params):
        """
        Fits a single Markov Regression model for a given window and parameters. The fitted model is used for in-sample prediction and error computation.
        :param window_data: DataFrame with 'delta_emissions' and 'delta_generation' columns
        :param params: Dictionary with model parameters
        :returns msdr_results: Determined model parameters
        """
        try:
            msdr_model = sm.tsa.MarkovRegression(
                endog=window_data['delta_emissions'],
                exog=window_data[['delta_generation']],
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
            mae = np.mean(np.abs(window_data['delta_emissions'] - msdr_predict))

            return msdr_result, msdr_result.aic, mae

        except Exception as e:
            logger.error(f"Model fitting failed with error: {e}")
            return None, np.inf, np.inf

    def _plot_estimated_emissions(self):
        """
        Plots the estimated vs. original emissions and calculates performance metrics.
        Saves the plot as a PNG file.
        """
        # Filter data to remove NaNs (e.g., the first window)
        df_plot = self.estimated_emi.dropna(subset=['estimated_emissions', 'delta_emissions'])

        if df_plot.empty:
            print("Warning: No valid data points for plotting (maybe window length > data length?)")
            return

        # Calculate metrics
        r2 = r2_score(df_plot['delta_emissions'], df_plot['estimated_emissions'])
        mae = mean_absolute_error(df_plot['delta_emissions'], df_plot['estimated_emissions'])
        mse = mean_squared_error(df_plot['delta_emissions'], df_plot['estimated_emissions'])
        rmse = np.sqrt(mse)

        # Create Plot
        plt.figure(figsize=(12, 6))
        plt.plot(df_plot.index, df_plot['delta_emissions'], label='Original Emissions (Scaled)', alpha=0.7)
        plt.plot(df_plot.index, df_plot['estimated_emissions'], label='Model Estimation (Scaled)', alpha=0.7, linestyle='--')

        plt.title(f"MSDR Model Validation - {self.tso}\nR² = {r2:.4f} | MAE = {mae:.4f} | MSE = {mse:.4f} | RMSE = {rmse:.4f}")
        plt.ylabel("Scaled Emissions")
        plt.xlabel("Time")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Save plot
        try:
            save_dir = self.root / "results" / f"{self.tso}_run_{self.run}_{self.year}" / "figures"
            os.makedirs(save_dir, exist_ok=True)
            
            filename = save_dir / f"{self.tso}_{self.year}_prediction.png"
            plt.savefig(filename)
            plt.close() # Close figure to free memory
            logger.info(f"Estimated plot saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save image to file: {e}. Continuing...")
            plt.close() # Ensure the figure is closed even on error

    # ---------- File handling ----------#
    def _save_to_file(self, data, sub_dir, filename):
        """
        Saves a dataframe to a file.
        :param data: Dataframe to save
        :param sub_dir: Subdirectory of results folder
        :param filename: Filename
        """
        ext = Path(filename).suffix.lower().lstrip('.')
        save_dir = self.root / "results" / f"{self.tso}_run_{self.run}_{self.year}" / sub_dir
        os.makedirs(save_dir, exist_ok=True)
        filepath = save_dir / filename

        match ext:
            case "csv":
                try:
                    if str(filename).endswith('.csv'):
                        data.to_csv(filepath)
                    logger.info(f"Dataframe saved to {filepath}")
                except Exception as e:
                    logger.error(f"Failed to save dataframe to csv: {e}")
            case "json":
                try:
                    with open(filepath, 'w', encoding='utf-8') as file:
                        json.dump(data, file, default=self._json_converter, ensure_ascii=False, indent=4)
                        logger.info(f"Summary data saved to {filepath}")
                except Exception as e:
                    logger.error(f"Failed to save summary data to json: {e}")

    @staticmethod
    def _json_converter(obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64)):
            return int(obj)
        if isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        raise TypeError(f"Type {type(obj)} is not JSON serializable")

    # ---------- Preprocessing ----------#
    def _inverse_transform_mef(self):
        """
        Function to transform scaled data back to the original scale for evaluation.
        """
        logger.info("Inverse transforming coefficients to get absolute MEF...")

        col_slope_scaled = self.df_mef_scaled['mef_scaled']
        col_intercept_scaled = self.df_mef_scaled['intercept_scaled']

        # Get transforming factors (sd & mean) from the scaler instance
        # [0] = Generation (X), [1] = Emissions (Y)
        std_gen = self.scaler.scale_[0]
        mw_gen = self.scaler.mean_[0]
        std_emi = self.scaler.scale_[1]
        mw_emi = self.scaler.mean_[1]
        slope_factor = std_emi / std_gen    # is slope coefficient, thus: beta_orig = beta_scaled * (std_emi / std_gen)

        # Compute columns values
        mef_t_mwh = col_slope_scaled * slope_factor
        mef_g_kwh = mef_t_mwh * 1000
        intercept_abs = (
                col_intercept_scaled * std_emi
                + mw_emi
                - (mef_t_mwh * mw_gen)
        )

        # Save columns in df
        self.df_mef_absolute = pd.DataFrame(
            {
                'mef_t_MWh': mef_t_mwh,
                'mef_g_kWh': mef_g_kwh,
                'intercept': intercept_abs
            },
            index=self.df_mef_scaled.index
        )

        self._save_to_file(data=self.df_mef_absolute, sub_dir='tables', filename='df_mef_absolute.csv')

    @staticmethod
    def _set_types(df):
        """
        Check and transform df for further preparation.
        :param df:
        :return df:
        """
        # Check if the index is set to 'datetime' col and if columns are numeric
        try:
            logger.info("Setting index to datetime...")
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

        except IndexError:
            logger.error("Failed to prepare datetime index.")

        # Ensure numeric types for relevant columns
        try:
            logger.info("Setting columns to numeric...")
            cols_to_check = ['delta_generation', 'delta_emissions']
            for col in cols_to_check:
                if col in df.columns and not pd.api.types.is_float_dtype(df[col]):
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        except Exception as e:
            logger.error(f"Failed to prepare float columns. Exit with: {e}")

        return df

    @staticmethod
    def _inspect_data(df):
        """
        Print data characteristics for inspection
        :param df:
        :return: None
        """
        print("[INSPECTION]")
        print(f"  - Index Type: {df.index.dtype}")
        print(f"  - Duplicates: {(df.index.duplicated()).sum()}")
        print(f"  - NaNs: {(df.isnull().sum()).sum()}")
        print(f"  - Delta Gen Type: {df.delta_generation.dtype}")
        print(f"  - Neg. Gen: {(df['delta_generation'] < 0).sum()}")
        print(f"  - Delta Emi Type: {df.delta_emissions.dtype}")
        print(f"  - Neg. Emi: {(df['delta_emissions'] < 0).sum()}")
