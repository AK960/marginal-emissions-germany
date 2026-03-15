"""
Class for validating the MEF time series.
"""
import json
import warnings
import matplotlib.ticker as mticker
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_absolute_percentage_error

from marginal_emissions import logger
from pathlib import Path

class MEFValidator:
    def __init__(self, data, tso, year, save_dir: Path):
        """
        Initialize a MEF Validator Object.
        :param data: DataFrame with the final MEF results.
        :param tso: Name of the Transmission System Operator (TSO) in lowercase.
        :param year: The year of the data.
        :param save_dir: The directory where validation results should be saved.
        """
        # Base
        self.results_summary = {}
        self.df = data
        self.tso = tso  # Expecting lowercase
        self.year = year
        self.save_dir = save_dir
        
        # For display purposes in plots and logs
        self.tso_display = "50Hertz" if tso == "50hertz" else tso.capitalize()
        
        # Validation Input
        logger.info(f"Initialized MEFValidator instance.")

    # ____________________ Entrypoint ____________________#
    def run_validation(self):
        logger.info("Running validation checks...")
        self.results_summary = {}

        # Rubric 1
        self._test_non_negativity()
        self._test_max_carbon_intensity()

        # Rubric 2
        self._test_empirical_annual_mef()
        
        # Rubric 3
        self._test_net_demand_patterns()

        # Output Generation
        self._generate_bounds_plot()
        self._generate_percentile_mef_plot()
        self._save_summary_as_json()
        logger.info("Validation checks completed.")

    # ____________________ Validation Functions ____________________#
    # Rubric 1: Expected Carbon Intensities
    def _test_non_negativity(self):
        """
        1.1: The MEF is not expected to be negative.
        """
        if 'mef_g_kWh' not in self.df.columns:
            return

        total_rows = len(self.df)
        negative_rows = (self.df['mef_g_kWh'] < 0).sum()
        pct_negative = (negative_rows / total_rows) * 100

        self.results_summary['Test 1.1'] = {
            'Description': 'Non-Negativity',
            'Result': {
                'Pct Negative MEF': f"{pct_negative:.2f}%"
            }
        }
        logger.info(f"Test 1.1 (Non-negativity): {pct_negative:.2f}% of rows are negative.")

    def _test_max_carbon_intensity(self):
        """
        1.2: The MEF is not expected to exceed the carbon intensity of the most-carbon-intensive plant.
        """
        if 'max_carbon_intensity' not in self.df.columns or 'mef_g_kWh' not in self.df.columns:
            return

        total_rows = len(self.df)
        exceeding_rows = (self.df['mef_g_kWh'] > self.df['max_carbon_intensity']).sum()
        pct_exceeding = (exceeding_rows / total_rows) * 100

        self.results_summary['Test 1.2'] = {
            'Description': 'Max Carbon Intensity',
            'Result': {
                'Pct Exceeding Max': f"{pct_exceeding:.2f}%"
            }
        }
        logger.info(f"Test 1.2 (Max Carbon Intensity): {pct_exceeding:.2f}% of rows exceed maximum bounds.")

    def _generate_bounds_plot(self):
        """
        Plots the MEF with its upper bound and labels the source of the bound.
        """
        if 'mef_g_kWh' not in self.df.columns or 'max_carbon_intensity' not in self.df.columns:
            logger.warning("MEF data or max carbon intensity not available for plotting.")
            return

        df_plot = self.df.copy()
        
        # For clarity, limit the plot to a specific period, e.g., one week
        if len(df_plot) > 672: # 1 week of 15-min data
            df_plot = df_plot.iloc[:672]

        # noinspection PyTypeChecker
        with plt.style.context('default'):
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Plot the computed MEF
            ax.plot(df_plot.index, df_plot['mef_g_kWh'], label='Computed MEF (15-min)', color='tab:blue', alpha=0.7, linewidth=1.2)
            
            # Plot the upper bound
            ax.plot(df_plot.index, df_plot['max_carbon_intensity'], label='Upper Bound (Max Intensity)', color='tab:orange', linestyle='--', linewidth=1.5)

            # Add text labels for the source of the upper bound
            df_plot['source_change'] = df_plot['max_carbon_source'].ne(df_plot['max_carbon_source'].shift())
            source_blocks = df_plot[df_plot['source_change']].index

            for i, start_block in enumerate(source_blocks):
                end_block = source_blocks[i + 1] if i + 1 < len(source_blocks) else df_plot.index[-1]
                
                # Get the source name and y-position for the label
                source_name = df_plot.loc[start_block, 'max_carbon_source']
                if source_name == 'None':
                    continue
                
                y_pos = df_plot.loc[start_block, 'max_carbon_intensity']
                
                # Find the middle of the block for the x-position
                block_center_idx = int((df_plot.index.get_loc(start_block) + df_plot.index.get_loc(end_block)) / 2)
                x_pos = df_plot.index[block_center_idx]
                
                # Add the text label
                ax.text(x_pos, y_pos + 5, source_name, ha='center', va='bottom', fontsize=8, color='tab:orange', alpha=0.9)

            ax.axhline(0, color='black', linewidth=1, linestyle='--')
            ax.set_title(f"MEF Bounds Validation - {self.tso_display} ({self.year})")
            ax.set_xlabel("Time")
            ax.set_ylabel("Marginal Emission Factor (gCO2/kWh)")
            ax.legend()
            ax.grid(True, alpha=0.3)
            fig.autofmt_xdate(rotation=45)

            plt.tight_layout()

            plot_path = self.save_dir / f"1_mef_bounds_plot_{self.tso}_{self.year}.png"
            try:
                fig.savefig(plot_path, bbox_inches='tight', facecolor='white')
                logger.info(f"Saved MEF bounds plot.")
            except Exception as e:
                logger.error(f"Failed to save MEF bounds plot: {e}")
            finally:
                plt.close(fig)

    # Rubric 2
    def _test_empirical_annual_mef(self):
        """
        2.1: Compares the model's annual average MEF with an empirical annual MEF derived from a simple regression.
        """
        if 'delta_generation' not in self.df.columns or 'delta_emissions' not in self.df.columns:
            logger.warning("Columns 'delta_generation' or 'delta_emissions' not found, skipping Test 2.1.")
            return

        # 1. Resample 15-min deltas to hourly deltas by summing them up
        df_hourly_deltas = self.df[['delta_generation', 'delta_emissions']].resample('h').sum()
        
        # 2. Drop the first row which might be incomplete and cause an outlier
        df_reg = df_hourly_deltas.iloc[1:].dropna()
        
        if len(df_reg) < 2:
            logger.warning("Not enough hourly data points to run regression for Test 2.1. Skipping.")
            return

        # Reshape data for sklearn
        X = df_reg['delta_generation'].values.reshape(-1, 1)
        y = df_reg['delta_emissions'].values

        # Fit linear regression
        model = LinearRegression()
        model.fit(X, y)

        # The slope is the Empirical Annual MEF in tCO2/MWh. Convert to g/kWh.
        empirical_mef_t_mwh = model.coef_[0]
        empirical_mef_g_kwh = empirical_mef_t_mwh * 1000
        r_squared = model.score(X, y)

        # Calculate the model's average MEF for comparison
        model_avg_mef = self.df['mef_g_kWh'].mean()
            
        # Calculate Mean Absolute Percentage Error
        mape = mean_absolute_percentage_error([model_avg_mef], [empirical_mef_g_kwh]) * 100

        self.results_summary['Test 2.1'] = {
            'Description': 'Empirical Annual Averages',
            'Result': {
                'Empirical Annual MEF (g/kWh)': f"{empirical_mef_g_kwh:.2f}",
                'Model Annual Average MEF (g/kWh)': f"{model_avg_mef:.2f}",
                'Mean Absolute Percentage Error (%)': f"{mape:.2f}",
                'R-squared of Empirical MEF': f"{r_squared:.4f}"
            }
        }
        logger.info("Test 2.1 (Empirical Annual MEF): Saved empirical annual MEF results.")
        
        self._plot_empirical_annual_mef(df_reg, model)

    def _plot_empirical_annual_mef(self, data, model):
        """Plots the scatter plot for the Empirical Annual MEF regression."""
        # noinspection PyTypeChecker
        with plt.style.context('default'):
            fig, ax = plt.subplots(figsize=(8, 5))

            # Scatter plot of the data
            ax.scatter(data['delta_generation'], data['delta_emissions'], alpha=0.3, label='Hourly Changes', color='tab:blue')
            
            # Plot the regression line
            x_vals = np.array(ax.get_xlim())
            y_vals = model.intercept_ + model.coef_[0] * x_vals
            ax.plot(x_vals, y_vals, color='tab:orange', linestyle='--', linewidth=2, label='Linear Regression')

            # Formatting
            slope = model.coef_[0] * 1000 # in g/kWh
            r2 = model.score(data['delta_generation'].values.reshape(-1, 1), data['delta_emissions'].values)
            
            ax.set_title(f'Empirical Annual MEF for {self.tso_display} ({self.year})\nSlope = {slope:.2f} g/kWh | R² = {r2:.4f}')
            ax.set_xlabel('Change in Conventional Generation (MWh)')
            ax.set_ylabel('Change in Conventional Emissions (tCO2)')
            ax.grid(True, alpha=0.3)
            ax.legend()
            
            fig.tight_layout()
            plot_path = self.save_dir / f"2.1_empirical_annual_mef_{self.tso}_{self.year}.png"
            try:
                fig.savefig(plot_path, bbox_inches='tight', facecolor='white')
                logger.info(f"Test 2.1 Saved empirical annual MEF plot.")
            except Exception as e:
                logger.error(f"Failed to save empirical annual MEF plot: {e}")
            finally:
                plt.close(fig)

    # Rubric 3: Expected net-demand (residual load) temporal patterns (it is expected that MEFs will differ during periods of low vs. high net-demand)
    def _get_coal_share(self):
        """Hilfsfunktion: Berechnet den prozentualen Kohleanteil am Jahresmix."""
        coal_cols = [col for col in ['lignite_generation', 'hard_coal_generation'] if col in self.df.columns]
        if not coal_cols or 'total_generation_all' not in self.df.columns:
            logger.warning("Could not calculate coal share: 'total_generation_all' or coal columns are missing.")
            return 0
        coal_gen = self.df[coal_cols].sum().sum()
        total_gen = self.df['total_generation_all'].sum()
        return coal_gen / total_gen if total_gen > 0 else 0

    def _test_net_demand_patterns(self):
        """
        Tests and reports on MEF behavior for both coal and non-coal region hypotheses.
        """
        if 'net_demand' not in self.df.columns or 'mef_g_kWh' not in self.df.columns:
            logger.warning("Column 'net_demand' or 'mef_g_kWh' is missing. Skipping Net-Demand tests.")
            return

        # 1. Calculate quantiles and median MEFs for the quantiles
        q20 = self.df['net_demand'].quantile(0.20)
        q80 = self.df['net_demand'].quantile(0.80)

        mef_low_demand = self.df.loc[self.df['net_demand'] <= q20, 'mef_g_kWh'].median()
        mef_high_demand = self.df.loc[self.df['net_demand'] >= q80, 'mef_g_kWh'].median()

        # 2. Test Hypothesis 4.3.1 (Coal Region)
        passed_coal = mef_high_demand < mef_low_demand
        hypothesis_coal = {
            'Result': "Passed" if passed_coal else "Failed",
            'Expectation': "High Demand MEF < Low Demand MEF",
            'Actual': f"{mef_high_demand:.2f} < {mef_low_demand:.2f}"
        }
        logger.info(f"Test 3.1 (Coal Region Hypothesis): {hypothesis_coal['Result']}")

        # 3. Test Hypothesis 4.3.2 (No Coal Region)
        passed_no_coal = mef_high_demand > mef_low_demand
        hypothesis_no_coal = {
            'Result': "Passed" if passed_no_coal else "Failed",
            'Expectation': "High Demand MEF > Low Demand MEF",
            'Actual': f"{mef_high_demand:.2f} > {mef_low_demand:.2f}"
        }
        logger.info(f"Test 3.2 (No-Coal Region Hypothesis): {hypothesis_no_coal['Result']}")

        # 4. Add the entire nested dictionary to the main summary
        self.results_summary['Test 3'] = {
            'Description': 'Net-Demand Patterns',
            'Indicators': {
                'Median MEF (Bottom 20% Net-Demand)': f"{mef_low_demand:.2f}",
                'Median MEF (Top 20% Net-Demand)': f"{mef_high_demand:.2f}",
                'Coal Share': f"{self._get_coal_share() * 100:.2f}%"
            },
            'Hypothesis 3.1 (Coal Region)': hypothesis_coal,
            'Hypothesis 3.2 (No-Coal Region)': hypothesis_no_coal
        }

        # 5. Generate plot
        self._generate_net_demand_plot(q20, q80)


    def _generate_net_demand_plot(self, q20, q80):
        """
        Erstellt einen Boxplot zum Vergleich der MEF-Verteilungen.
        """
        low_df = self.df[self.df['net_demand'] <= q20][['mef_g_kWh']].copy()
        low_df['Period'] = 'Bottom 20%\n(Low Net-Demand)'

        high_df = self.df[self.df['net_demand'] >= q80][['mef_g_kWh']].copy()
        high_df['Period'] = 'Top 20%\n(Peak Net-Demand)'

        plot_data = pd.concat([low_df, high_df])

        plt.figure(figsize=(8, 6))
        
        # Suppress the specific PendingDeprecationWarning from seaborn's internal call
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", PendingDeprecationWarning)
            sns.boxplot(data=plot_data, x='Period', y='mef_g_kWh', hue='Period', palette='Set2', legend=False)

        plt.title(f"MEF Distribution by Net-Demand - {self.tso_display} ({self.year})")
        plt.ylabel("Marginal Emission Factor (gCO2/kWh)")
        plt.xlabel("")
        plt.tight_layout()

        plot_path = self.save_dir / f"3_mef_net_demand_boxplot_{self.tso}_{self.year}.png"
        plt.savefig(plot_path)
        plt.close()
        logger.info(f"Saved Net-Demand Boxplot.")

    def _generate_percentile_mef_plot(self):
        """
        Generates a plot of MEF and Net Demand vs. Net Demand Percentiles.
        """
        if 'net_demand' not in self.df.columns or 'mef_g_kWh' not in self.df.columns:
            logger.warning("Cannot generate percentile plot: 'net_demand' or 'mef_g_kWh' is missing.")
            return

        logger.info("Generating MEF vs. Net Demand percentile plot...")

        df_plot = self.df[['net_demand', 'mef_g_kWh']].dropna().copy()

        # Convert units for plotting
        # Net demand from MWh (per 15min) to average GW
        df_plot['net_demand_gw'] = (df_plot['net_demand'] / 0.25) / 1000
        # MEF from g/kWh to lbs/MWh (1 g/kWh = 2.20462 lbs/MWh)
        df_plot['mef_lbs_mwh'] = df_plot['mef_g_kWh'] * 2.20462

        # Create 40 percentile bins (2.5% each) from net demand
        try:
            df_plot['percentile_bin'] = pd.qcut(df_plot['net_demand_gw'], q=40, labels=False, duplicates='drop')
        except ValueError:
            logger.warning("Could not create percentile bins due to non-unique bin edges. Skipping percentile plot.")
            return
        
        # Calculate the mean of MEF and Net Demand for each bin
        percentile_data = df_plot.groupby('percentile_bin')[['net_demand_gw', 'mef_lbs_mwh']].mean()
        
        # Create an x-axis representing the midpoint of each percentile bin
        x_axis = (percentile_data.index + 0.5) * (100 / 40)

        # Create the plot
        # noinspection PyTypeChecker
        with plt.style.context('default'):
            fig, ax1 = plt.subplots(figsize=(8, 5))

            # Plot Net Demand on the left axis (ax1)
            color1 = 'tab:orange'
            ax1.set_xlabel('Net Demand Percentile')
            ax1.set_ylabel('Net Demand (GW)', color=color1)
            ax1.plot(x_axis, percentile_data['net_demand_gw'], color=color1, label='Net Demand (GW)')
            ax1.tick_params(axis='y', labelcolor=color1)
            ax1.xaxis.set_major_formatter(mticker.PercentFormatter())

            # Create a second y-axis for the MEF
            ax2 = ax1.twinx()
            color2 = 'tab:blue'
            ax2.set_ylabel('Percentile MEF (lbs / MWh)', color=color2)
            ax2.plot(x_axis, percentile_data['mef_lbs_mwh'], color=color2, label='Percentile MEF (lbs / MWh)')
            ax2.tick_params(axis='y', labelcolor=color2)

            # Title and layout
            ax1.set_title(f'Empirical Percentile MEF vs. Net Demand\n{self.tso_display} ({self.year})')
            ax1.grid(True, alpha=0.3)
            fig.tight_layout()

            # Save the figure
            plot_path = self.save_dir / f"3_percentile_mef_vs_netdemand_{self.tso}_{self.year}.png"
            try:
                fig.savefig(plot_path, bbox_inches='tight', facecolor='white')
                logger.info(f"Saved percentile MEF plot.")
            except Exception as e:
                logger.error(f"Failed to save percentile MEF plot: {e}")
            finally:
                plt.close(fig)

    def _save_summary_as_json(self):
        """
        Saves the validation test results to a JSON file.
        """
        file_path = self.save_dir / f"validation_summary_{self.tso}_{self.year}.json"
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.results_summary, f, default=self._json_converter, ensure_ascii=False, indent=4)
            logger.info(f"Validation summary saved successfully.")
        except Exception as e:
            logger.error(f"Failed to save summary to JSON: {e}")

    @staticmethod
    def _json_converter(obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64)):
            return int(obj)
        if isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        raise TypeError(f"Type {type(obj)} is not JSON serializable")
