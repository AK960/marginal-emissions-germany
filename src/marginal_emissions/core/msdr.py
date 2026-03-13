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
from scipy import stats
from pandas.plotting import autocorrelation_plot

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
        data,
        tso=None,
        year=None,
        window_length = 672, # 1 week = 7*24*4
        param_grid = None,
        n_jobs = -1,
        run:str = "msdr"
    ):
        """
        Initialize a base MSDR Analysis Object. Requires the input of a dataset with detrended emissions and generation time series.
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
        self.df = data # Contains original data
        ## Params
        self.window_length = window_length
        self.n_jobs = n_jobs
        self.param_grid = param_grid if param_grid is not None else {
            'k_regimes': [2, 3], # Tests 2 or 3 regimes
            'trend': ['c'], # Allows for intercept; captures / absorbs all effects that are not proportional to marginal changes in generation (allows for better fit of slope coefficient) (Default = 'c')
            'switching_trend': [True], # Allows for different intercept for each regime (Default = True)
            'switching_exog': [True], # Allows different slope for each regime (Default = True)
            'switching_variance': [True] # Allows different variance for each regime (Default = False)
        }
        ## Outcomes
        self.prep_df = None     # Df, specifically prepped for analysis: contains only z-transformed delta_generation and delta_emissions
        self.indicators = []    # Contains indicators of the best model for evaluation --> not flat thus list not df
        self.coeffs_df = None   # Contains regime coefficients with descriptive statistics params
        self.final_df = None    # Df, that stores interim and final results

    # ____________________ Public functions ____________________#
    def prepare(self):
        """
        Prepares input data for later analysis.
        """
        logger.info("Preparing data...")
        try:
            # Select delta columns
            df = self.df[['delta_generation', 'delta_emissions']].copy()

            # Check input data
            df = self._set_types(df)

            # Fill any NaNs created by asfreq (if gaps existed) or shift
            if df.isnull().values.any():
                # Interpolate by time to avoid hard jumps & drop remaining NaNs
                df = df.dropna()

            # Scaling data to have zero mean and unit variance (z-transformation)
            df[['delta_generation', 'delta_emissions']] = self.scaler.fit_transform(
                df[['delta_generation', 'delta_emissions']]
            )

            # Print inspection
            self._inspect_data(df)

            # Set analysis df as state
            self.prep_df = df

        except Exception as e:
            logger.error(f"Failed to prepare data: {e}")

    def fit_compute(self):
        """
        Fits a msdr model for each timestamp in the time series.
        """
        if self.prep_df.empty:
            raise ValueError("Data not prepared yet. Call prepare() first.")

        logger.info(f"Fitting model and computing MEF for {len(self.prep_df) - self.window_length + 1} observations...")
        try:
            # Parallel execution with a progress bar
            results = Parallel(n_jobs=self.n_jobs)(
                delayed(self._process_window)(i=i, prep_data=self.prep_df)
                for i in tqdm(range(len(self.prep_df) - self.window_length + 1), desc=f"Analyzing {self.tso}")
            )

            valid_results = [r for r in results if r is not None]

            if valid_results:
                self.final_df = pd.DataFrame([r['data'] for r in valid_results]).set_index('timestamp').sort_index()
                self.indicators = [r['indicator'] for r in valid_results]

                all_coeffs_list = [r['coeffs'] for r in valid_results]
                self.coeffs_df = pd.concat(all_coeffs_list, ignore_index=True)
                self.coeffs_df.set_index(['timestamp', 'parameter'], inplace=True)
            else:
                logger.warning("No valid models were fitted across the entire dataset.")
                self.final_df = pd.DataFrame()
                self.coeffs_df = pd.DataFrame()

            logger.info(f"Finished model fitting and MEF computation!")

        except Exception as e:
            logger.error(f"Failed to run analysis. Exit with error: {e}")

    # ____________________ Private functions ____________________#
    # ---------- Model fitting ----------#
    def _process_window(self, i, prep_data):
        """
        Tests many different model parameters to determine the best model for a given rolling time window. For every last timestamp in the window, it returns the best model.
        :param i: Index of the current window
        :param prep_data: DataFrame with 'delta_emissions' and 'delta_generation' columns
        :returns best_result: Determined model parameters
        """
        current_window = prep_data.iloc[i : i + self.window_length]
        timestamp = current_window.index[-1]   # timestamp of the last observation of the window:
                                               # mef and estimated_emissions are computed for this observation

        # Model selection params
        best_converged = False
        best_model = None
        # best_mae = np.inf
        best_aic = np.inf

        # (1) Determine the best model for each window and store it in best_model
        for params in ParameterGrid(self.param_grid):
            # result contains the returned model object
            # result, mae = self._fit_markov_model(current_window, params) # Model selection based on MAE
            result, aic = self._fit_markov_model(current_window, params) # Model selection based on AIC

            if result is None:
                continue

            # Check if the model converged
            is_converged = result.mle_retvals['converged']

            ## Legacy: Select model based on MAE
            """
            # Case 1: No best model yet
            if best_model is None:
                best_model = result
                best_mae = mae
                best_converged = is_converged
            # Case 2: The new model converged, the old did not -> Take converged one
            elif is_converged and not best_converged:
                best_model = result
                best_mae = mae
                best_converged = True
            # Case 3: Both converged / did not converge -> Take the one with better mae
            elif is_converged == best_converged:
                if mae < best_mae:
                    best_model = result
                    best_mae = mae
            """
            ## Select model based on AIC
            # Case 1: No best model yet
            if best_model is None:
                best_model = result
                best_aic = aic
                best_converged = is_converged

            # Case 2: The new model converged, the old did not -> Take converged one
            elif is_converged and not best_converged:
                best_model = result
                best_aic = aic
                best_converged = True

            # Case 3: Both converged / did not converge -> Take the one with better aic
            elif is_converged == best_converged:
                if aic < best_aic:
                    best_model = result
                    best_aic = aic

        # (2) Compute MEF with best_model by passing it to _compute_mef()
        if best_model is not None:
            pred_data = self._predict_emissions(model=best_model, timestamp=timestamp)
            mef_data = self._compute_mef(model=best_model, timestamp=timestamp)
            indicator_data, coeff_table = self._save_indicators(model=best_model, timestamp=timestamp)

            # Making sure potential NaN values don't crash the unpacking
            pred_data = pred_data if pred_data else {}
            mef_data = mef_data if mef_data else {}

            res_row = {
                'timestamp': timestamp,
                'delta_generation': current_window['delta_generation'].iloc[-1],
                'delta_emissions': current_window['delta_emissions'].iloc[-1],
                **pred_data,
                **mef_data
            }

            return {'data': res_row, 'indicator': indicator_data, 'coeffs': coeff_table}

        else:
            logger.error(f"Failed to fit model for {timestamp}. Model is None.")
            return None

    # ---------- Methods in the loop ----------#
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
                switching_trend=params['switching_trend'],
                switching_exog=params['switching_exog'],
                switching_variance=params['switching_variance']
            )

            # Train model on the window data
            msdr_result = msdr_model.fit(disp=False)

            # Legacy: Compute MAE for later model selection
            """
            msdr_predict = msdr_result.predict(start=window_data.index[1], end=window_data.index[-1])
            mae = np.mean(np.abs(window_data['delta_emissions'] - msdr_predict))
            return msdr_result, mae
            """

            return msdr_result, msdr_result.aic

        except Exception as e:
            logger.error(f"Model fitting failed with error: {e}")
            return None, np.inf

    @staticmethod
    def _predict_emissions(model, timestamp):
        """
        Estimates the emission time series using the best model for each window.
        """
        if model is not None:
            logger.debug("Performing in-sample prediction with best models...")

            # Get in-sample estimation of the model for the entire window to save computation time compared to another predict
            fitted_values = model.fittedvalues
            estimated_val = fitted_values.iloc[-1]

            return {'delta_estimated_emissions': float(estimated_val)}
        else:
            logger.error(f"Failed to predict emissions for {timestamp}. Model is None.")
            return None

    def _compute_mef(self, model, timestamp):
        """
        Computes the Marginal Emission Factor (MEF) from the best models by calculating a weighted average of the regime-specific coefficients based on smoothed probabilities.
        """
        if model is not None:
            logger.debug(f"Computing MEF for {timestamp}...")

            # For extracting coeffs and iterating
            params = model.params.to_dict()
            smoothed_probs = model.smoothed_marginal_probabilities.iloc[-1].to_dict()

            # 1. Find Intercepts
            intercepts = {}
            for r in range(3):  # Check for up to 3 regimes
                name = f'const[{r}]'
                if name in params:
                    intercepts[r] = params[name]

            # Fallback if no switching intercept (global const)
            if not intercepts and 'const' in params:
                for r in range(len(smoothed_probs)):
                    intercepts[r] = params['const']

            # 2. Find Generation Coefficients (MEFs)
            gen_coeffs = {}
            for r in range(3):  # Check for up to 3 regimes
                # Possible names for Regime r
                candidates = [f'x1[{r}]', f'delta_generation[{r}]']
                for name in candidates:
                    if name in params:
                        gen_coeffs[r] = params[name]
                        break

                # If no regime-specific param found, check for global param
                if r not in gen_coeffs:
                    if 'x1' in params:
                        gen_coeffs[r] = params['x1']
                    elif 'delta_generation' in params:
                        gen_coeffs[r] = params['delta_generation']

            # 3. Calculate Weighted Averages (Combined MEF and Intercept)
            combined_gen_coeff = 0
            combined_intercept = 0

            # Iterate over the actual number of regimes found (length of probs)
            for r in range(len(smoothed_probs)):
                prob = smoothed_probs[r]
                combined_gen_coeff += prob * gen_coeffs.get(r, 0)
                combined_intercept += prob * intercepts.get(r, 0)

            # Inverse transform the mef and intercept
            mef_t_mwh, mef_g_kwh, intercept = self._inverse_transform_coeffs(
                mef_scaled=combined_gen_coeff,
                icpt_scaled=combined_intercept
            )

            return {
                'intercept_scaled': combined_intercept,
                'mef_scaled': combined_gen_coeff,
                'mef_t_MWh': mef_t_mwh,
                'mef_g_kWh': mef_g_kwh,
                'intercept': intercept
            }

        else:
            logger.error(f"Failed to compute MEF for {timestamp}. Model is None.")
            return None

    def _inverse_transform_coeffs(self, mef_scaled, icpt_scaled):
        """
        Function to transform scaled data back to the original scale for evaluation.
        """
        logger.debug("Inverse transforming coefficients to get absolute MEF...")

        # Get transforming factors (sd & mean) from the scaler instance
        # [0] = Generation (X), [1] = Emissions (Y)
        std_gen = self.scaler.scale_[0]
        mw_gen = self.scaler.mean_[0]
        std_emi = self.scaler.scale_[1]
        mw_emi = self.scaler.mean_[1]
        slope_factor = std_emi / std_gen    # is slope coefficient, thus: beta_orig = beta_scaled * (std_emi / std_gen)

        # Compute columns values
        mef_t_mwh = mef_scaled * slope_factor
        mef_g_kwh = mef_t_mwh * 1000
        intercept = (
                icpt_scaled * std_emi
                + mw_emi
                - (mef_t_mwh * mw_gen)
        )

        return mef_t_mwh, mef_g_kwh, intercept

    @staticmethod
    def _save_indicators(model, timestamp):
        # For each timestamp, save .summary() coefficients in list
        if model is not None:
            logger.debug(f"Storing indicators for {timestamp}...")
            df_summary_coeffs = pd.concat(
                [
                    model.params,  # coef
                    model.bse,  # std_err
                    model.tvalues,  # z
                    model.pvalues,  # P>|z|
                    model.conf_int()  # Conf_int [0.025, 0.975]
                ],
                axis=1
            )
            df_summary_coeffs.columns = ['coef', 'std_err', 'tval', 'pval', 'ci_lower', 'ci_upper']

            df_summary_coeffs = df_summary_coeffs.reset_index().rename(columns={'index': 'parameter'})
            df_summary_coeffs['timestamp'] = timestamp

            indicator_row = {
                'timestamp': timestamp,
                'k_regimes': int(model._results.k_regimes),
                'smoothed_probs': model.smoothed_marginal_probabilities.iloc[-1].to_dict(),
                'aic': float(model.aic),  # 2k - 2 ln(L) // k = no. params, L = max llf (no. params vs. model fit)
                'bic': float(model.bic),
                'hqic': float(model.hqic),
                'llf': float(model.llf),
                'mle_converged': bool(model.mle_retvals['converged'])
            }

            return indicator_row, df_summary_coeffs
        else:
            logger.error(f"Failed to store indicators for {timestamp}. Model is None.")
            return None, None

    # ---------- Data & File handling ----------#
    def save_to_file(self, data, filename):
        """
        Saves a dataframe to a file.
        :param data: Dataframe to save
        :param filename: Filename
        """
        ext = Path(filename).suffix.lower().lstrip('.')
        if self.run is None:
            save_dir = self.root / "results" / "test"
        else:
            save_dir = self.root / "results" / f"run_{self.run}" / f"{self.tso}_{self.year}"
        os.makedirs(save_dir, exist_ok=True)
        filepath = save_dir / filename

        match ext:
            case "csv":
                try:
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

    @staticmethod
    def _set_types(df, cols_to_check=None):
        """
        Check and transform df for further preparation.
        :param df: Dataframe whose types to set
        :param cols_to_check: List of columns to check and transform
        :return df: DataFrame with transformed columns
        """
        # Check if the index is set to 'datetime' col and if columns are numeric
        if cols_to_check is None:
            cols_to_check = ['delta_generation', 'delta_emissions']
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
            for col in cols_to_check:
                if col in df.columns and not pd.api.types.is_float_dtype(df[col]):
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        except Exception as e:
            logger.error(f"Failed to prepare float columns. Exit with: {e}")

        try:
            logger.info("Setting frequency to 15min...")
            df = df.asfreq('15min')

        except Exception as e:
            logger.error(f"Failed to set frequency to 15min. Exit with: {e}")

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
