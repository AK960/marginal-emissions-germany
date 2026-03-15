"""
Class for performing the MSDR analysis with time varying transition probabilities.
"""

import json
import os
import warnings
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytz
import statsmodels.api as sm
from joblib import Parallel, delayed
from pyprojroot import here
from scipy import stats
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sklearn.model_selection import ParameterGrid
from sklearn.preprocessing import StandardScaler
from statsmodels.tools.sm_exceptions import ValueWarning, ConvergenceWarning
from tqdm import tqdm

from marginal_emissions import logger
from marginal_emissions.vars import RESULTS_DIR

# Suppress specific statsmodels warnings
warnings.simplefilter('ignore', ValueWarning)
# Suppress ConvergenceWarning to avoid cluttering output during the rolling window
warnings.simplefilter('ignore', ConvergenceWarning)
# Suppress RuntimeWarning from numpy, which often happens during optimization of unstable models
warnings.simplefilter('ignore', RuntimeWarning)


class MSARAnalyzer:
    def __init__(
        self,
        data,
        max_lags=4,
        ic='bic',
        tso=None,
        year=None,
        window_length=672,  # 1 week = 7*24*4
        step_size=32,  # Move window by for 8 hours
        param_grid=None,
        n_jobs=-1,
        test=False,
        test_rows=None,
        num_iterations=None,
        run: str = "msar"
    ):
        """
        Initialize a MSAR analysis object that considers autoregression and time varying transition probabilities.
        :param tso: Name of the Transmission System Operator (TSO)
        :param window_length: Size of the rolling window (default: 1 week = 672 quarters)
        :param param_grid: Dictionary for grid search parameters
        :param n_jobs: Number of parallel jobs (-1 for all CPUs)
        """
        # Base
        self.root = here()
        self.tso = tso
        self.tso_display = "50Hertz" if tso == "50hertz" else tso.capitalize()
        self.year = year
        self.test = test
        self.test_rows = test_rows
        self.num_iterations = num_iterations
        self.run = run  # Number of the run to track progress
        # Preprocessing
        self.scaler = StandardScaler()
        # Analysis
        ## Data
        self.df = data  # Contains original data
        ## Params
        self.window_length = window_length
        self.step_size = step_size
        self.n_jobs = n_jobs
        self.max_lags = max_lags
        self.ic = ic
        self.param_grid = param_grid if param_grid is not None else {
            'k_regimes': [2, 3],  # Tests 2 or 3 regimes
            'trend': ['c'],                 # Allows for intercept; captures / absorbs all effects that are not proportional to marginal changes in generation (allows for better fit of slope coefficient) (Default = 'c')
            'switching_trend': [True],      # Allows for different intercept for each regime (Default = True)
            'switching_exog': [True],       # Allows different slope for each regime (Default = True)
            'switching_variance': [True]    # Allows different variance for each regime (Default = False)
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

            # Add tvtp phases
            # (1) Extract local time for phases
            local_times = df.index.tz_convert('Europe/Berlin').time

            # (2) Init columns (constant and dummy vars)
            # Overnight Trough as baseline
            df['tvtp_const'] = 1.0
            # Morning Peak
            df['tvtp_phase2'] = ((local_times >= pd.to_datetime('06:00:00').time()) &
                                 (local_times < pd.to_datetime('10:00:00').time())).astype(float)
            # Solar Trough
            df['tvtp_phase3'] = ((local_times >= pd.to_datetime('10:00:00').time()) &
                                 (local_times < pd.to_datetime('16:00:00').time())).astype(float)
            # Evening Peak
            df['tvtp_phase4'] = (local_times >= pd.to_datetime('16:00:00').time()).astype(float)

            # Scaling data to have zero mean and unit variance (z-transformation)
            df[['delta_generation', 'delta_emissions']] = self.scaler.fit_transform(
                df[['delta_generation', 'delta_emissions']]
            )

            # Print inspection
            # self._inspect_data(df)

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

        # In case the last increment does not perfectly align with the length of the dataset (avoids data loss at the tail)
        max_idx = len(self.prep_df) - self.window_length
        window_indices = list(range(0, max_idx + 1, self.step_size))
        if window_indices[-1] != max_idx:
            window_indices.append(max_idx)

        logger.info(f"Fitting model and computing MEF for {len(window_indices)} windows...")
        try:
            # Parallel execution with a progress bar
            results = Parallel(n_jobs=self.n_jobs)(
                delayed(self._process_window)(i=i, prep_data=self.prep_df)
                for i in tqdm(window_indices, desc=f"Analyzing {self.tso_display}")
            )

            valid_results = [item for sublist in results if sublist is not None for item in sublist]

            if valid_results:
                logger.info("Smoothing MEF results to remove block-boundary jumps...")

                # 1. Create a raw, fully populated DataFrame from results
                raw_df = pd.DataFrame([r['data'] for r in valid_results]).set_index('timestamp').sort_index()

                mef_cols = ['intercept_scaled', 'mef_scaled', 'mef_t_MWh', 'mef_g_kWh', 'intercept']
                smoothed_df = raw_df.copy()

                # 2. Iterate through the window boundaries to find and correct jumps
                for i in range(len(window_indices) - 1):
                    # Define the current and the next block
                    current_block_start_idx = window_indices[i]
                    next_block_start_idx = window_indices[i + 1]

                    # Get the timestamps for the end of the current block and start of the next
                    # The jump occurs between the last point of the old model and the first of the new
                    end_of_current_block_ts = self.prep_df.index[next_block_start_idx - 1]
                    start_of_next_block_ts = self.prep_df.index[next_block_start_idx]

                    # Ensure these timestamps exist in the results
                    if end_of_current_block_ts not in smoothed_df.index or start_of_next_block_ts not in smoothed_df.index:
                        continue

                    # 3. Calculate the jump (the error) for each MEF column
                    for col in mef_cols:
                        jump = smoothed_df.loc[start_of_next_block_ts, col] - smoothed_df.loc[
                            end_of_current_block_ts, col]

                        # 4. Define the slice of the DataFrame that belongs to the current model block
                        block_slice_start_ts = self.prep_df.index[current_block_start_idx]
                        block_slice_end_ts = end_of_current_block_ts
                        block_timestamps = smoothed_df.loc[block_slice_start_ts:block_slice_end_ts].index

                        if len(block_timestamps) <= 1:
                            continue

                        # 5. Create a linear correction ramp and apply it
                        # The ramp goes from 0 to `jump` over the length of the block
                        correction_ramp = np.linspace(0, jump, len(block_timestamps))
                        smoothed_df.loc[block_timestamps, col] += correction_ramp

                self.final_df = smoothed_df.round(4)
                self.indicators = [r['indicator'] for r in valid_results]
                all_coeffs_list = [r['coeffs'] for r in valid_results]
                self.coeffs_df = pd.concat(all_coeffs_list, ignore_index=True)
                self.coeffs_df.set_index(['timestamp', 'parameter'], inplace=True)

                self._plot_results()
                self._plot_sawtooth_debug(window_indices)
                self._plot_avg_daily_profile()
                self._diagnose_residuals()
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
        current_window = prep_data.iloc[i: i + self.window_length]
        timestamp = current_window.index[-1]  # timestamp of the last observation of the window:
        # mef and estimated_emissions are computed for this observation

        # Find best lag parameter for each window
        best_lag_for_window = self._find_best_lag(current_window)
        logger.debug(f"Window at {timestamp}: Optimal lag found is {best_lag_for_window} using {self.ic.upper()}.")

        best_converged = False
        best_model = None
        best_aic = np.inf

        # (1) Determine the best model for each window and store it in best_model
        for params in ParameterGrid(self.param_grid):
            result, aic = self._fit_markov_model(window_data=current_window, params=params, order=best_lag_for_window)

            if result is None:
                continue

            is_converged = result.mle_retvals['converged']

            if best_model is None:
                best_model = result
                best_aic = aic
                best_converged = is_converged
            elif is_converged and not best_converged:
                best_model = result
                best_aic = aic
                best_converged = True
            elif is_converged == best_converged:
                if aic < best_aic:
                    best_model = result
                    best_aic = aic

        if best_model is not None:
            window_results = []
            # Timestamps of the last self.step_size intervals (default 32)
            target_timestamps = current_window.index[-self.step_size:]

            for ts in target_timestamps:
                pred_data = self._predict_emissions(model=best_model, timestamp=ts)
                mef_data = self._compute_mef(model=best_model, timestamp=ts)
                indicator_data, coeff_table = self._save_indicators(model=best_model, timestamp=ts)

                pred_data = pred_data if pred_data else {}
                mef_data = mef_data if mef_data else {}

                res_row = {
                    'timestamp': ts,
                    'delta_generation': current_window.loc[ts, 'delta_generation'],
                    'delta_emissions': current_window.loc[ts, 'delta_emissions'],
                    **pred_data,
                    **mef_data
                }
                window_results.append({'data': res_row, 'indicator': indicator_data, 'coeffs': coeff_table})

            return window_results  # Return the list with self.step_size tuples
        else:
            logger.error(f"Failed to fit model for {timestamp}. Model is None.")
            return None

    # ---------- Methods in the loop ----------#
    def _find_best_lag(self, window_data: pd.DataFrame) -> int:
        """
        Finds the optimal number of autoregressive lags for a given window
        using a specified information criterion (AIC or BIC) on a simple OLS model.

        :param window_data: The data for the current window.
        :return: The optimal number of lags (p).
        """
        best_ic = np.inf
        best_lag = 0
        endog = window_data['delta_emissions']

        for p in range(self.max_lags + 1):
            # Prepare exogenous variables
            exog_vars = ['delta_generation']
            exog_df = window_data[exog_vars].copy()

            if p > 0:
                for i in range(1, p + 1):
                    exog_df[f'ar_lag_{i}'] = endog.shift(i)

            # Drop NaNs created by lagging
            full_df = pd.concat([endog, exog_df], axis=1).dropna()
            current_endog = full_df['delta_emissions']
            current_exog = sm.add_constant(full_df.drop('delta_emissions', axis=1))

            # Fit a fast OLS model
            try:
                model = sm.OLS(current_endog, current_exog).fit()
                current_ic = getattr(model, self.ic)  # Gets model.bic or model.aic

                if current_ic < best_ic:
                    best_ic = current_ic
                    best_lag = p
            except Exception:
                # If OLS fails, just skip this lag order
                continue

        return best_lag

    @staticmethod
    def _fit_markov_model(window_data, params, order):
        """
        Fits a single Markov Regression model for a given window and parameters. The fitted model is used for in-sample prediction and error computation.
        :param window_data: DataFrame with 'delta_emissions' and 'delta_generation' columns
        :param params: Dictionary with model parameters
        :param order: The number of AR lags to include
        :returns msdr_results: Determined model parameters
        """
        endog = window_data['delta_emissions']
        exog = window_data[['delta_generation']].copy()

        tvtp_cols = ['tvtp_const', 'tvtp_phase2', 'tvtp_phase3', 'tvtp_phase4']
        has_tvtp = all(col in window_data.columns for col in tvtp_cols)
        tvtp_df = window_data[tvtp_cols].copy() if has_tvtp else None

        if order > 0:
            for i in range(1, order + 1):
                exog[f'ar_lag_{i}'] = endog.shift(i)
            valid_idx = exog.dropna().index
            endog = endog.loc[valid_idx]
            exog = exog.loc[valid_idx]
            if has_tvtp:
                tvtp_df = tvtp_df.loc[valid_idx]

        exog_tvtp_data = tvtp_df.to_numpy(dtype=np.float64) if has_tvtp else None

        try:
            msdr_model = sm.tsa.MarkovRegression(
                endog=endog,
                exog=exog,
                exog_tvtp=exog_tvtp_data,
                k_regimes=params['k_regimes'],
                trend=params['trend'],
                switching_trend=params['switching_trend'],
                switching_exog=params['switching_exog'],
                switching_variance=params['switching_variance']
            )

            # Train model on the window data
            msdr_result = msdr_model.fit(disp=False)

            return msdr_result, msdr_result.aic
        except Exception as e:
            logger.error(f"Model fitting failed for order={order} with error: {e}")
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
            # estimated_val = fitted_values.iloc[-1] # For step size 1
            estimated_val = fitted_values.loc[timestamp]

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
            # smoothed_probs = model.smoothed_marginal_probabilities.iloc[-1].to_dict() # For step size 1
            smoothed_probs = model.smoothed_marginal_probabilities.loc[timestamp].to_dict()

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
        Transforms scaled coefficients back to the original scale.
        """
        logger.debug("Inverse transforming coefficients to get absolute MEF...")

        # Get transforming factors (sd & mean) from the scaler instance
        # [0] = Generation (X), [1] = Emissions (Y)
        std_gen = self.scaler.scale_[0]
        mw_gen = self.scaler.mean_[0]
        std_emi = self.scaler.scale_[1]
        mw_emi = self.scaler.mean_[1]
        slope_factor = std_emi / std_gen  # is slope coefficient, thus: beta_orig = beta_scaled * (std_emi / std_gen)

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
            df_summary_coeffs = df_summary_coeffs.round(4)

            df_summary_coeffs = df_summary_coeffs.reset_index().rename(columns={'index': 'parameter'})
            df_summary_coeffs['timestamp'] = timestamp

            indicator_row = {
                'timestamp': timestamp,
                'k_regimes': int(model._results.k_regimes),
                'smoothed_probs': {k: round(v, 4) for k, v in
                                   model.smoothed_marginal_probabilities.loc[timestamp].to_dict().items()},
                'aic': round(float(model.aic), 4),
                # 2k - 2 ln(L) // k = no. params, L = max llf (no. params vs. model fit)
                'bic': round(float(model.bic), 4),
                'hqic': round(float(model.hqic), 4),
                'llf': round(float(model.llf), 4),
                'mle_converged': bool(model.mle_retvals['converged'])
            }

            return indicator_row, df_summary_coeffs
        else:
            logger.error(f"Failed to store indicators for {timestamp}. Model is None.")
            return None, None

    # ---------- Data & File handling ----------#
    def _get_save_dir(self):
        """Constructs the save directory based on instance attributes."""
        if self.test:
            return RESULTS_DIR / "test" / f"{self.run}" / f"{self.tso}_{self.year}_{self.num_iterations}"
        else:
            return RESULTS_DIR / f"{self.run}" / f"{self.tso}" / f"{self.year}"

    def save_to_file(self, data, filename):
        """
        Saves a dataframe to a file in the appropriate directory.
        :param data: Dataframe to save
        :param filename: Filename
        """
        save_dir = self._get_save_dir()
        os.makedirs(save_dir, exist_ok=True)
        filepath = save_dir / filename
        ext = Path(filename).suffix.lower().lstrip('.')

        match ext:
            case "csv":
                try:
                    data.to_csv(filepath, float_format='%.4f')
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

    def _plot_results(self):
        """
        Plots the estimated vs. original emissions and calculates performance metrics.
        Saves the plot as a PNG file.
        """
        df_plot = self.final_df[['delta_emissions', 'delta_estimated_emissions']].copy()
        df_plot.index = pd.to_datetime(df_plot.index, format="ISO8601")

        # Calculate metrics before interpolation for validity
        df_metrics = df_plot.dropna()
        if df_metrics.empty:
            logger.error("No valid data points for plotting.")
            return

        r2 = r2_score(df_metrics['delta_emissions'], df_metrics['delta_estimated_emissions'])
        mae = mean_absolute_error(df_metrics['delta_emissions'], df_metrics['delta_estimated_emissions'])
        mse = mean_squared_error(df_metrics['delta_emissions'], df_metrics['delta_estimated_emissions'])
        rmse = np.sqrt(mse)

        # Interpolate by time for complete plot
        if df_plot.isnull().values.any():
            logger.info("Interpolating missing values for plot.")
            df_plot = df_plot.interpolate(method='time').dropna()

        if df_plot.empty:
            logger.error("No valid data points for plotting after interpolation.")
            return

        # noinspection PyTypeChecker
        with plt.style.context('default'):
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.plot(df_plot.index, df_plot['delta_estimated_emissions'], label='Estimated Emissions', alpha=0.7,
                    color='tab:blue')
            ax.plot(df_plot.index, df_plot['delta_emissions'], label='Original Emissions', alpha=0.7, linestyle='--',
                    color='tab:orange')
            ax.set_title(
                f"{self.tso_display} ({self.year})\n| R² = {r2:.4f} | MAE = {mae:.4f} | MSE = {mse:.4f} | RMSE = {rmse:.4f} |")
            ax.set_ylabel("Emissions (Scaled)")
            ax.set_xlabel("Time")
            ax.legend()
            ax.grid(True, alpha=0.3)
            fig.autofmt_xdate(rotation=45)

            save_dir = self._get_save_dir()
            os.makedirs(save_dir, exist_ok=True)
            filename = save_dir / "estimated_emissions.png"

            try:
                fig.savefig(filename, bbox_inches='tight')
                logger.info(f"Plot saved to {filename}")
            except Exception as e:
                logger.error(f"Failed to save image to file: {e}. Continuing...")
            finally:
                plt.close(fig)

    def _diagnose_residuals(self):
        """
        Performs a residual analysis and saves diagnostic plots and statistical test results.
        """
        if self.final_df is None or self.final_df.empty:
            logger.warning("No final_df available for residual diagnostics. Skipping.")
            return

        logger.info("Performing residual diagnostics...")

        # 1. Calculate residuals
        residuals = (self.final_df['delta_emissions'] - self.final_df['delta_estimated_emissions']).dropna()

        if residuals.empty:
            logger.warning("Residuals are empty. Skipping diagnostics.")
            return

        # 2. Perform statistical tests
        ljung_box_lags = [10, 20, 40, 80]
        ljung_box_results = sm.stats.acorr_ljungbox(residuals, lags=ljung_box_lags, return_df=True)
        ljung_box_results.index.name = 'lags'
        jb_stat, jb_p, skew, kurt = sm.stats.jarque_bera(residuals)

        # 3. Create diagnostic plots (2x2 grid)
        with plt.style.context('default'):
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            fig.suptitle(f'Residual Diagnostics for {self.tso_display} ({self.year})', fontsize=16)

            # Plot 1: Residuals over Time
            axes[0, 0].plot(residuals.index, residuals, color='tab:blue', linewidth=0.7, alpha=0.8)
            axes[0, 0].axhline(y=0, color='tab:red', linestyle='--', linewidth=1.5)
            axes[0, 0].set_title('Residuals Over Time')
            axes[0, 0].set_xlabel('Time')
            axes[0, 0].set_ylabel('Residual Value')
            axes[0, 0].grid(True, alpha=0.3)
            fig.autofmt_xdate(rotation=45, ha='right')

            # Plot 2: Distribution of Residuals
            axes[0, 1].hist(residuals, bins=50, density=True, color='tab:blue', alpha=0.7, label='Residuals')
            mu, std = stats.norm.fit(residuals)
            x = np.linspace(*axes[0, 1].get_xlim(), 100)
            axes[0, 1].plot(x, stats.norm.pdf(x, mu, std), 'k', linewidth=2, label='Normal Distribution')
            axes[0, 1].set_title('Distribution of Residuals')
            axes[0, 1].set_xlabel('Residual Value')
            axes[0, 1].legend()
            axes[0, 1].grid(True, alpha=0.3)

            # Plot 3: Autocorrelation (ACF)
            sm.graphics.tsa.plot_acf(residuals, lags=40, ax=axes[1, 0], markerfacecolor='tab:orange', markeredgecolor='tab:orange')
            axes[1, 0].set_title('Autocorrelation of Residuals (ACF)')
            axes[1, 0].grid(True, alpha=0.3)

            # Plot 4: Q-Q Plot
            sm.qqplot(residuals, line='s', ax=axes[1, 1], color='tab:blue', marker='o')
            axes[1, 1].set_title('Q-Q Plot vs. Normal Distribution')
            axes[1, 1].grid(True, alpha=0.3)

            plt.tight_layout(rect=[0, 0.03, 1, 0.95])

        # 4. Save plots and data
        save_dir = self._get_save_dir()
        os.makedirs(save_dir, exist_ok=True)

        plot_filename = save_dir / "residual_diagnostics.png"
        try:
            fig.savefig(plot_filename, bbox_inches='tight', facecolor='white')
            logger.info(f"Saved residual diagnostics plot to {plot_filename}")
        except Exception as e:
            logger.error(f"Failed to save residual diagnostics plot: {e}")
        finally:
            plt.close(fig)

        diagnostics_summary = {
            'ljung_box': ljung_box_results.to_dict(orient='index'),
            'jarque_bera': {
                'statistic': jb_stat,
                'p_value': jb_p,
                'skewness': skew,
                'kurtosis': kurt
            }
        }
        self.save_to_file(data=diagnostics_summary, filename='residual_diagnostics.json')
        logger.info(f"Saved diagnostic test results to {save_dir}")

    def _plot_sawtooth_debug(self, window_indices):
        """
        Plots the smoothed MEF timeseries and marks the window boundaries.
        """
        if self.final_df is None or 'mef_t_MWh' not in self.final_df.columns:
            logger.warning("Final DataFrame with MEF not available for sawtooth debug plot.")
            return

        logger.info("Plotting sawtooth debug graph...")

        df_plot = self.final_df.copy()

        # Limit to a reasonable number of days for clarity, e.g., the first 5 days
        start_date = df_plot.index.min()
        end_date = start_date + pd.Timedelta(days=5)
        df_plot = df_plot.loc[start_date:end_date]

        if df_plot.empty:
            return

        # noinspection PyTypeChecker
        with plt.style.context('default'):
            # An _plot_results angepasste Größe
            fig, ax = plt.subplots(figsize=(12, 6))

            # An _plot_results angepasste Farbe und Transparenz (tab:blue, alpha=0.7)
            ax.plot(df_plot.index, df_plot['mef_t_MWh'], label='Smoothed MEF (t/MWh)', color='tab:blue', alpha=0.7,
                    linewidth=1.5)

            # Draw vertical lines at the start of each new window application
            for i in window_indices:
                if i > 0:
                    boundary_ts = self.prep_df.index[i]
                    if start_date <= boundary_ts <= end_date:
                        # Dezentere Trennlinien passend zum Style
                        ax.axvline(boundary_ts, color='tab:red', linestyle='--', linewidth=1.0, alpha=0.5,
                                   label='Model Switch' if i == self.step_size else "")

            ax.set_title(f'Smoothed MEF vs. Model Application Boundaries for {self.tso_display} ({self.year})')
            ax.set_xlabel('Time')
            ax.set_ylabel('MEF (t CO₂ / MWh)')

            # Avoid duplicate labels in legend
            handles, labels = ax.get_legend_handles_labels()
            by_label = dict(zip(labels, handles))
            ax.legend(by_label.values(), by_label.keys())

            ax.grid(True, alpha=0.3)
            fig.autofmt_xdate(rotation=45)

            plt.tight_layout()

            save_dir = self._get_save_dir()
            plot_filename = save_dir / "sawtooth_debug_profile_smoothed.png"
            try:
                # facecolor='white' verhindert Darstellungsfehler im IDE Dark-Mode
                fig.savefig(plot_filename, bbox_inches='tight', facecolor='white')
                logger.info(f"Saved sawtooth debug plot to {plot_filename}")
            except Exception as e:
                logger.error(f"Failed to save sawtooth debug plot: {e}")
            plt.close(fig)

    def _plot_avg_daily_profile(self):
        """
        Calculates and plots the average daily profile of the MEF.
        """
        if self.final_df is None or 'mef_t_MWh' not in self.final_df.columns:
            logger.warning("Final DataFrame with MEF not available for daily profile plot.")
            return

        logger.info("Plotting average daily MEF profile...")

        # 1. Group by time of day and calculate the mean
        daily_avg_15min = self.final_df.groupby(self.final_df.index.time)['mef_t_MWh'].mean()

        # 2. Create a dummy date index for plotting
        dummy_day = pd.date_range(start='2024-01-01', periods=len(daily_avg_15min), freq='15min')
        daily_avg_15min.index = dummy_day

        # noinspection PyTypeChecker
        with plt.style.context('default'):
            # 3. Create the plot (angepasste Größe)
            fig, ax = plt.subplots(figsize=(12, 6))

            # An _plot_results angepasste Farbe und Transparenz
            ax.plot(daily_avg_15min.index, daily_avg_15min.values, color='tab:blue', alpha=0.7, linewidth=1.5,
                    label='Ø MEF (15 min)')

            # 4. Format the x-axis
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax.set_xlim(dummy_day[0], dummy_day[-1])

            # 5. Labels and layout
            ax.set_title(f'Durchschnittliches Tagesprofil des MEF ({self.tso_display}, {self.year})')
            ax.set_xlabel('Uhrzeit')
            ax.set_ylabel('MEF (t CO₂ / MWh)')

            ax.grid(True, alpha=0.3)
            ax.legend()
            fig.autofmt_xdate(rotation=45)

            plt.tight_layout()

            # 6. Save the figure
            save_dir = self._get_save_dir()
            plot_filename = save_dir / "mef_avg_daily_profile.png"
            try:
                # facecolor='white' verhindert Darstellungsfehler im IDE Dark-Mode
                fig.savefig(plot_filename, bbox_inches='tight', facecolor='white')
                logger.info(f"Saved average daily profile plot to {plot_filename}")
            except Exception as e:
                logger.error(f"Failed to save average daily profile plot: {e}")
            plt.close(fig)

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
