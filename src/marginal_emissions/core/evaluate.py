"""
Class for performing post-analysis evaluation of the MEF results.
"""
import json
import os
import time
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.api as sm
from sklearn.model_selection import ParameterGrid
from sklearn.preprocessing import StandardScaler

from marginal_emissions import logger
from marginal_emissions.vars import RESULTS_DIR, DATA_DIR


class MEFEvaluator:
    def __init__(self, tso: str, skip_fitting: bool = False):
        """
        Initialize the evaluator for a specific TSO.
        """
        self.tso = tso
        self.tso_display = "50Hertz" if tso == "50hertz" else tso.capitalize()
        self.results_path = RESULTS_DIR / 'msar'
        self.data_path = DATA_DIR / 'processed'
        self.all_data = {}
        self.skip_fitting = skip_fitting
        self._load_data_for_all_years()

    def _load_data_for_all_years(self):
        """Loads the necessary data files for all available years for the TSO."""
        logger.info(f"Loading all data for {self.tso}...")
        for year_dir in (self.results_path / self.tso).iterdir():
            if year_dir.is_dir():
                year = year_dir.name
                self.all_data[year] = {}
                
                # Load MEF results
                mef_results_file = year_dir / 'mef_final.csv'
                if mef_results_file.exists():
                    self.all_data[year]['mef_results'] = pd.read_csv(mef_results_file, index_col=0, parse_dates=True)
                
                # Load original processed data
                search_pattern = f"final_{self.tso}_{year}_*.csv"
                matching_files = list(self.data_path.glob(search_pattern))
                if matching_files:
                    self.all_data[year]['mef_data'] = pd.read_csv(matching_files[0], index_col=0, parse_dates=True)

    def run_evaluation(self):
        """Runs all evaluation and plotting methods."""
        self.plot_daily_profiles()
        self.plot_seasonal_daily_profiles()
        if not self.skip_fitting:
            for year in self.all_data.keys():
                self.analyze_global_regimes(year)

    def plot_daily_profiles(self):
        """Plots the average daily profiles of MEF and AEF for all available years."""
        logger.info(f"Plotting daily profiles for {self.tso}...")
        with plt.style.context('default'):
            fig, ax1 = plt.subplots(figsize=(12, 6))
            ax2 = ax1.twinx()
            colors = {'2023': 'tab:blue', '2024': 'tab:orange'}

            for year, data in self.all_data.items():
                color = colors.get(year, 'gray')
                if 'mef_results' in data:
                    df_mef = data['mef_results']
                    daily_avg_mef = df_mef.groupby(df_mef.index.time)['mef_g_kWh'].mean()
                    min_mef = daily_avg_mef.min()
                    max_mef = daily_avg_mef.max()
                    dummy_day = pd.date_range(start="2024-01-01", periods=len(daily_avg_mef), freq='15min')
                    daily_avg_mef.index = dummy_day
                    ax1.plot(daily_avg_mef.index, daily_avg_mef.values, label=f'Avg. MEF {year} (Min: {min_mef:.2f}, Max: {max_mef:.2f})', color=color, linestyle='-')
                
                if 'mef_data' in data:
                    df_data_year = data['mef_data'].copy()
                    df_data_year['aef_g_kWh'] = (df_data_year['total_emissions'] / df_data_year['total_generation_all']) * 1000
                    daily_avg_aef = df_data_year.groupby(df_data_year.index.time)['aef_g_kWh'].mean()
                    min_aef = daily_avg_aef.min()
                    max_aef = daily_avg_aef.max()
                    dummy_day_aef = pd.date_range(start="2024-01-01", periods=len(daily_avg_aef), freq='15min')
                    daily_avg_aef.index = dummy_day_aef
                    ax2.plot(daily_avg_aef.index, daily_avg_aef.values, label=f'Avg. AEF {year} (Min: {min_aef:.2f}, Max: {max_aef:.2f})', color=color, linestyle='--')

            ax1.xaxis.set_major_locator(mdates.HourLocator(interval=2))
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax1.set_title(f'Average Daily Profile of MEF and AEF for {self.tso_display}')
            ax1.set_xlabel('Time of Day')
            ax1.set_ylabel('Marginal Emission Factor (gCO₂/kWh)')
            ax2.set_ylabel('Average Emission Factor (gCO₂/kWh)')
            ax1.grid(True, alpha=0.3)
            lines, labels = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax2.legend(lines + lines2, labels + labels2, loc='best')
            fig.autofmt_xdate(rotation=45)
            plt.tight_layout()
            
            save_dir = self.results_path / self.tso
            os.makedirs(save_dir, exist_ok=True)
            plot_path = save_dir / f"average_daily_profile_mef_aef_{self.tso}.pdf"
            fig.savefig(plot_path, bbox_inches='tight')
            logger.info(f"Average daily profile plot saved successfully.")
            plt.close(fig)

    def plot_seasonal_daily_profiles(self):
        """Plots the average daily MEF profiles for summer and winter."""
        logger.info(f"Plotting seasonal daily profiles for {self.tso}...")
        with plt.style.context('default'):
            fig, ax = plt.subplots(figsize=(12, 6))
            colors = {'2023': 'tab:blue', '2024': 'tab:orange'}
            linestyles = {'Summer': '-', 'Winter': '--'}

            for year, data in self.all_data.items():
                color = colors.get(year, 'gray')
                if 'mef_results' in data:
                    df_mef = data['mef_results']
                    summer_mask = (df_mef.index.month >= 6) & (df_mef.index.month <= 8)
                    winter_mask = (df_mef.index.month == 12) | (df_mef.index.month <= 2)
                    seasons = {'Summer': df_mef[summer_mask], 'Winter': df_mef[winter_mask]}
                    
                    for season_name, df_season in seasons.items():
                        ls = linestyles[season_name]
                        if not df_season.empty:
                            daily_avg = df_season.groupby(df_season.index.time)['mef_g_kWh'].mean()
                            min_mef = daily_avg.min()
                            max_mef = daily_avg.max()
                            dummy_day = pd.date_range(start="2024-01-01", periods=len(daily_avg), freq='15min')
                            daily_avg.index = dummy_day
                            ax.plot(daily_avg.index, daily_avg.values, label=f'{season_name} {year} (Min: {min_mef:.2f}, Max: {max_mef:.2f})', color=color, linestyle=ls)

            ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax.set_title(f'Average Seasonal Daily MEF Profiles for {self.tso_display}')
            ax.set_xlabel('Time of Day')
            ax.set_ylabel('Marginal Emission Factor (gCO₂/kWh)')
            ax.grid(True, alpha=0.3)
            ax.legend()
            fig.autofmt_xdate(rotation=45)
            plt.tight_layout()
            
            save_dir = self.results_path / self.tso
            os.makedirs(save_dir, exist_ok=True)
            plot_path = save_dir / f"average_seasonal_daily_profile_{self.tso}.pdf"
            fig.savefig(plot_path, bbox_inches='tight')
            logger.info(f"Seasonal daily profiles saved successfully.")
            plt.close(fig)

    def analyze_global_regimes(self, year: str):
        """Fits a single, non-rolling MSAR model to the dataset for a given year."""
        logger.info(f"Fitting global MSAR model for {self.tso}/{year}...")
        if year not in self.all_data or 'mef_data' not in self.all_data[year]:
            logger.error(f"No data available to analyze global regimes for {self.tso}/{year}")
            return

        df = self.all_data[year]['mef_data'][['delta_generation', 'delta_emissions']].copy()
        df = df.asfreq('15min')

        winsor_limits = {'delta_generation': (-140.25, 147.50), 'delta_emissions': (-99.66, 106.67)}
        for col, (lower, upper) in winsor_limits.items():
            df[col] = df[col].clip(lower=lower, upper=upper)
        df.dropna(inplace=True)

        local_times = df.index.tz_convert('Europe/Berlin').time
        df['tvtp_const'] = 1.0
        df['tvtp_phase2'] = ((local_times >= pd.to_datetime('06:00:00').time()) & (
                    local_times < pd.to_datetime('10:00:00').time())).astype(float)
        df['tvtp_phase3'] = ((local_times >= pd.to_datetime('10:00:00').time()) & (
                    local_times < pd.to_datetime('16:00:00').time())).astype(float)
        df['tvtp_phase4'] = (local_times >= pd.to_datetime('16:00:00').time()).astype(float)

        scaler = StandardScaler()
        df[['delta_generation', 'delta_emissions']] = scaler.fit_transform(df[['delta_generation', 'delta_emissions']])

        endog = df['delta_emissions']
        exog = df[['delta_generation']].copy()
        for i in range(1, 3):
            exog[f'ar_lag_{i}'] = endog.shift(i)

        full_df = pd.concat([endog, exog, df[['tvtp_const', 'tvtp_phase2', 'tvtp_phase3', 'tvtp_phase4']]],
                            axis=1).dropna()
        endog = full_df['delta_emissions']
        exog = full_df.drop(['delta_emissions', 'tvtp_const', 'tvtp_phase2', 'tvtp_phase3', 'tvtp_phase4'], axis=1)
        exog_tvtp = full_df[['tvtp_const', 'tvtp_phase2', 'tvtp_phase3', 'tvtp_phase4']]

        param_grid = {
            'k_regimes': [2, 3],
            'trend': ['c'],
            'switching_trend': [True],
            'switching_exog': [True],
            'switching_variance': [True]
        }

        best_model = None
        best_aic = float('inf')
        best_converged = False

        grid_list = list(ParameterGrid(param_grid))
        total_models = len(grid_list)
        logger.info(f"Starting grid search over {total_models} parameter combinations...")

        for idx, params in enumerate(grid_list, 1):
            logger.info(f"Fitting model {idx}/{total_models} (Regimes: {params['k_regimes']})...")
            try:
                model = sm.tsa.MarkovRegression(endog=endog, exog=exog, exog_tvtp=exog_tvtp, **params)
                result = model.fit(disp=False)
                is_converged = result.mle_retvals['converged']
                aic = result.aic

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
            except Exception as e:
                logger.error(f"Error fitting model {idx}/{total_models} with params {params}: {e}")
                continue

        if best_model is None:
            logger.error("Failed to fit any model.")
            return

        logger.info(
            f"Best model found with {best_model.k_regimes} regimes (AIC: {best_aic:.2f}, Converged: {best_converged}).")
        result = best_model
        summary_text = result.summary().as_text()

        replacements = {
            'x1': 'delta_generation',
            'x2': 'ar_lag_1',
            'x3': 'ar_lag_2',
            'tvtp0': 'baseline_night',
            'tvtp1': 'phase2_morning',
            'tvtp2': 'phase3_midday',
            'tvtp3': 'phase4_evening'
        }

        for old, new in replacements.items():
            summary_text = summary_text.replace(old, new)

        save_dir = self.results_path / self.tso
        os.makedirs(save_dir, exist_ok=True)

        summary_path = save_dir / f"global_regime_summary_{self.tso}_{year}.txt"
        with open(summary_path, 'w') as f:
            f.write(summary_text)
        logger.info(f"Model summary saved successfully.")

        with plt.style.context('default'):
            num_subplots = 2 + result.k_regimes

            fig, axes = plt.subplots(num_subplots, 1, figsize=(15, 3 * num_subplots), sharex=True)
            fig.suptitle(f'Global Regime Analysis for {self.tso_display} ({year})', fontsize=16)
            plot_colors = ['tab:green', 'tab:red', 'tab:blue', 'tab:orange', 'tab:purple']

            axes[0].plot(full_df.index, full_df['delta_generation'], label='Δ Generation (scaled)',
                         color=plot_colors[0])
            axes[0].set_ylabel('Δ Gen (scaled)')
            axes[0].grid(True, alpha=0.3)
            axes[0].legend(loc='upper left')

            axes[1].plot(full_df.index, full_df['delta_emissions'], label='Δ Emissions (scaled)', color=plot_colors[1])
            axes[1].set_ylabel('Δ Emi (scaled)')
            axes[1].grid(True, alpha=0.3)
            axes[1].legend(loc='upper left')

            for i in range(result.k_regimes):
                axes[i + 2].plot(result.smoothed_marginal_probabilities.index,
                                 result.smoothed_marginal_probabilities[i],
                                 label=f'Regime {i} Prob.', color=plot_colors[i + 2])
                axes[i + 2].set_ylabel(f'P(Regime {i})')
                axes[i + 2].grid(True, alpha=0.3)
                axes[i + 2].legend(loc='upper left')

            axes[-1].set_xlabel('Date')
            fig.autofmt_xdate(rotation=45, ha='right')
            plt.tight_layout(rect=[0, 0, 1, 0.96])

            plot_path = save_dir / f"global_regime_plot_{self.tso}_{year}.pdf"
            fig.savefig(plot_path, bbox_inches='tight')
            logger.info(f"Smoothed marginal probabilities plot saved successfully.")
            plt.close(fig)
