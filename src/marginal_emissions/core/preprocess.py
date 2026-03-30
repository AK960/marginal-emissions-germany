"""
Class for preprocessing the MEF time series.
"""
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytz
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

from marginal_emissions import logger
from marginal_emissions.vars import *

class MEFPreprocessor:
    def __init__(self):
        self.areas_gen_dict = {}
        self.emissions = {}
        self.out_dir_interim = f'{DATA_DIR}/interim'
        self.out_dir_processed = f'{DATA_DIR}/processed'
        self.out_dir_figures = f'{RESULTS_DIR}/figures'
        os.makedirs(self.out_dir_interim, exist_ok=True)
        os.makedirs(self.out_dir_processed, exist_ok=True)
        os.makedirs(self.out_dir_figures, exist_ok=True)

    def prep_emissions(self):
        emissions = pd.concat([
            EMI_DICT['emi_2022'],
            EMI_DICT['emi_2023'],
            EMI_DICT['emi_2024'],
            EMI_DICT['emi_2025']
        ])

        emissions = emissions.rename(columns=EMI_COLS)

        emissions['Datetime'] = pd.to_datetime(emissions['Datetime'], format='%Y-%m-%dT%H:%M:%S')
        emissions = emissions.set_index('Datetime')
        try:
            emissions.index = emissions.index.tz_localize('Europe/Berlin', ambiguous='infer')
        except pytz.exceptions.AmbiguousTimeError:
            emissions.index = emissions.index.tz_localize('Europe/Berlin', ambiguous=True)
        emissions.index = emissions.index.tz_convert('UTC')

        # Check for and remove duplicate entries
        if emissions.index.duplicated().any():
            emissions = emissions[~emissions.index.duplicated(keep='first')]

        """
        Agora provides the data without considering the time shift from summer to winter time (no 2nd entry for 02:00), yet, it considers the shift
        from winter to summer (01:00 -> 03:00). When transforming to UTC, the ambiguous=True forces pandas to interpret the entry 2023-10-29 02:00:00
        as belonging to the summer time and thus sets it to 00:00 UTC. The entry 2023-10-29 03:00:00 is set to 02:00 UTC, thus leaving the 01:00 UTC
        blank. To complete the time series, the gap is closed by performing a forward fill.
        """
        ## Closing the gap and removing duplicates
        full_idx = pd.date_range(start=emissions.index.min(), end=emissions.index.max(), freq='h', tz='UTC')
        emissions = emissions.reindex(full_idx)

        # The interpolation must happen before converting to numeric, as it might operate on string representations
        emissions = emissions.interpolate(method='time', limit_direction='both')
        
        # Convert columns to numeric, assuming standard decimal format for Agora data.
        # The .str.replace for German format is NOT needed here.
        for col in emissions.columns:
            emissions[col] = pd.to_numeric(emissions[col], errors='coerce')

        """
        The generation timeseries consists of intervals such that the interval of time t contains [t, t+15)
        The emissions timeseries consists of single points in time where at time t contains [t-60, t)
        To align both, move emissions time series -1h such that t contains [t, t+60)
        By forward filling, both time series can now be aligned, such that for emissions also t contains [t, t+15)
        => In the following, it is possible to:
            --> Compute the hourly emissions per control area, weighed by their share of conventional generation
            --> Perform a generation weighed downsampling of emissions to quarter hourly resolution
        """
        # Align timeseries to period-start
        emissions.index = emissions.index - pd.Timedelta(hours=1)

        # Save emissions to file
        min_date = emissions.index.min().strftime('%Y%m%d%H%M')
        max_date = emissions.index.max().strftime('%Y%m%d%H%M')

        # Compute new total_emissions to exclude nuclear and pump storage (total emissions in original data are more than the considered conventionals)
        fuels = ['lignite', 'hard_coal', 'fossile_gas', 'other_conventionals']
        emissions['total_emissions'] = emissions[fuels].sum(axis=1)

        self.emissions = emissions

        try:
            self.emissions.to_csv(f'{self.out_dir_interim}/emissions_germany_utc_{min_date}_{max_date}.csv', index=True)
            logger.info(f"Saved file to {self.out_dir_interim}/emissions_germany_utc_{min_date}_{max_date}.csv")
        except Exception as e:
            logger.error(f"Failed to save file: {e}")

    def prep_generation(self):
        for area, df_raw in GEN_DICT.items():
            gen = df_raw.copy()

            gen = gen.rename(columns=GEN_COLS)

            # Transform date columns to datetime and set index
            gen['datetime'] = pd.to_datetime(gen['datetime'], format="%d.%m.%Y %H:%M")
            gen = gen.set_index('datetime')

            # Set timezone to UTC
            gen.index = gen.index.tz_localize('Europe/Berlin', ambiguous='infer')
            gen.index = gen.index.tz_convert('UTC')

            # Transform columns to numeric
            for col in gen.columns:
                if not pd.api.types.is_float_dtype(gen[col]):
                    gen[col] = (
                        gen[col]
                        .astype(str)
                        .str.replace(".", "", regex=False)
                        .str.replace(",", ".", regex=False)
                    )
                    gen[col] = pd.to_numeric(gen[col], errors='coerce')

            # Aggregate all generation before removing everything but conventional sources
            gen['total_generation_all'] = gen.sum(axis=1, numeric_only=True)

            # 2. Define which columns to keep: conventional fuels + the new total_generation_all
            conventional_cols = [c for c in GEN_COLS.values() if c in gen.columns]
            cols_to_keep = conventional_cols + ['total_generation_all']
            gen = gen[cols_to_keep]

            # 3. Calculate total_generation (conventional only) from the remaining fuel columns
            gen['total_generation'] = gen[conventional_cols].sum(axis=1, numeric_only=True)

            # Fill instance df
            self.areas_gen_dict[area] = gen

        for area in self.areas_gen_dict:
            min_date = self.areas_gen_dict[area].index.min().strftime('%Y%m%d%H%M')
            max_date = self.areas_gen_dict[area].index.max().strftime('%Y%m%d%H%M')
            try:
                self.areas_gen_dict[area].to_csv(f'{self.out_dir_interim}/generation_{area}_utc_{min_date}_{max_date}.csv')
                logger.info(f"Saved file to {self.out_dir_interim}/generation_{area}_utc_{min_date}_{max_date}.csv")
            except Exception as e:
                logger.error(f"Failed to save file: {e}")

        return self.areas_gen_dict # In case one wants to continue working with the dataframe

    def alloc_emissions(self):
        if self.emissions is None or self.emissions.empty:
            raise ValueError("Emissions not prepared yet. Call prep_emissions() first.")
        if not self.areas_gen_dict:
            raise ValueError("Generation not prepared yet. Call prep_generation() first.")

        fuels = ['lignite', 'hard_coal', 'fossile_gas', 'other_conventionals']

        # Germany-wide 15-min generation per fuel, then aggregate it to hourly values
        total_gen_15min = (
            pd.concat(
                [df[[f for f in fuels if f in df.columns]] for df in self.areas_gen_dict.values()]
            )
            .groupby(level=0)
            .sum()
            .sort_index()
        )
        total_gen_hourly = total_gen_15min.resample('1h').sum()

        regional_emissions_final = {}

        for name, df_reg in self.areas_gen_dict.items():
            regional_emissions_15min = pd.DataFrame(index=df_reg.index)

            for fuel in fuels:
                if fuel not in df_reg.columns or fuel not in total_gen_hourly.columns or fuel not in self.emissions.columns:
                    continue

                # (1) Regional hourly generation and share
                regional_gen_15min = df_reg[fuel].copy()
                regional_gen_hourly = regional_gen_15min.resample('1h').sum()
                regional_share_h = (regional_gen_hourly / total_gen_hourly[fuel]).fillna(0)
                regional_emissions_hourly = self.emissions[fuel] * regional_share_h

                # (2) Temporal downscaling to 15 min via IEF interpolation
                ief_hourly = (regional_emissions_hourly / regional_gen_hourly).replace([np.inf, -np.inf], 0).fillna(
                    0)

                # pchip to interpolate hard edges
                ief_15min = ief_hourly.resample('15min').interpolate(method='pchip')
                ief_15min = ief_15min.ffill().bfill()

                # Multiply with 15-minute generation
                raw_emissions_15min = regional_gen_15min * ief_15min

                regional_emissions_15min[fuel] = raw_emissions_15min

            # Total emissions per control area and quarter-hour
            regional_emissions_15min['total_emissions'] = regional_emissions_15min.sum(axis=1)
            regional_emissions_final[name] = regional_emissions_15min

        # Join data frames for the final processed output dataframe and split them by year (avoid ram overflow for too long datasets)
        splits = [
            ("2022-12-23 23:00:00+00:00", "2024-01-01 00:00:00+00:00", "2023"),
            ("2023-12-23 23:00:00+00:00", "2025-01-01 00:00:00+00:00", "2024")
        ]

        for reg in self.areas_gen_dict:
            # Get regional frames
            df_emi = regional_emissions_final[reg]
            df_gen = self.areas_gen_dict[reg]

            # Merge on index
            final_df_all = pd.merge(
                df_gen,
                df_emi,
                left_index=True,
                right_index=True,
                how='inner',
                suffixes=('_generation', '_emissions')
            )

            # Compute shift (difference between t-1 and t, value at t says how much generation / emissions occured during t-1 and t)
            final_df_all['delta_generation'] = final_df_all['total_generation'].diff()
            final_df_all['delta_emissions'] = final_df_all['total_emissions'].diff()
            final_df_all = final_df_all[1:] # drop first row, will be none

            # Rearrange df
            other_cols = [c for c in final_df_all.columns if c not in
                          ['total_generation', 'total_generation_all', 'delta_generation', 'total_emissions', 'delta_emissions']]
            new_order = other_cols + ['total_generation', 'total_generation_all', 'delta_generation', 'total_emissions', 'delta_emissions']
            final_df_all = final_df_all[new_order]

            # Split by year - window length
            for start_date, end_date, year_label in splits:
                split_df_all = final_df_all.loc[start_date:end_date]

                if split_df_all.empty:
                    print(f"No data for {year_label}")
                    continue

                min_date = split_df_all.index.min().strftime('%Y%m%d%H%M')
                max_date = split_df_all.index.max().strftime('%Y%m%d%H%M')
                filename_all = f"final_{reg}_{year_label}_15min_utc_{min_date}_{max_date}.csv"
                try:
                    split_df_all.to_csv(f'{self.out_dir_processed}/{filename_all}', index=True)
                    logger.info(f"Saved file to {filename_all}")
                except Exception as e:
                    logger.error(f"Failed to save file: {e}")

        return regional_emissions_final

    def validate_allocation(self, regional_emissions_final):
        if self.emissions is None or self.emissions.empty:
            raise ValueError("Emissions not prepared yet. Call prep_emissions() first.")
        if not regional_emissions_final:
            raise ValueError("Emissions not allocated yet. Call alloc_emissions() first.")
        if not self.areas_gen_dict:
            raise ValueError("Generation not prepared yet. Call prep_generation() first.")

        logger.info("Starting validation of emission allocation...")
        fuels = ['lignite', 'hard_coal', 'fossile_gas', 'other_conventionals']

        # Summiere alle 15-Min-Werte über die Regionen auf
        total_regional_15min = pd.concat(regional_emissions_final.values()).groupby(level=0).sum()

        # Zähle, wie viele Viertelstunden jede Stunde hat (sollten 4 sein)
        count_15min = total_regional_15min.groupby(pd.Grouper(freq='1h')).count()

        # Bilde die Stunden-Summen
        total_regional_hourly = total_regional_15min.resample('1h').sum()

        # Filtere unvollständige Stunden (wie z.B. Ränder des Datensatzes oder Zeitumstellung) heraus
        valid_hours_mask = count_15min.iloc[:, 0] == 4
        total_regional_hourly = total_regional_hourly.loc[valid_hours_mask]

        common_idx = total_regional_hourly.index.intersection(self.emissions.index)

        # Randeffekte (erster und letzter Zeitstempel) abschneiden
        if len(common_idx) > 2:
            common_idx = common_idx[1:-1]

        original_hourly = self.emissions.loc[common_idx]
        resampled_hourly = total_regional_hourly.loc[common_idx]

        # Validate fuel-by-fuel preservation of hourly totals
        for fuel in fuels + ['total_emissions']:
            if fuel not in original_hourly.columns or fuel not in resampled_hourly.columns:
                continue

            diff = (resampled_hourly[fuel] - original_hourly[fuel]).abs()

            # Durchschnittlichen relativen Fehler (MAPE) berechnen
            valid_mask = original_hourly[fuel] > 0
            if valid_mask.any():
                # Relative Differenz für jede gültige Stunde berechnen (*100 für Prozent)
                rel_diff_series = (diff[valid_mask] / original_hourly[fuel][valid_mask]) * 100
                mean_rel_diff = rel_diff_series.mean()
            else:
                mean_rel_diff = 0.0

            # Find the maximum absolute deviation
            max_abs_diff = diff.max()

            # Relative error am Punkt der maximalen Abweichung
            if max_abs_diff > 0:
                time_of_max_diff = diff.idxmax()  # Speichere den exakten Zeitpunkt der höchsten Abweichung
                original_value_at_max_diff = original_hourly.loc[time_of_max_diff, fuel]

                if original_value_at_max_diff > 0:
                    max_rel_diff = (max_abs_diff / original_value_at_max_diff) * 100
                else:
                    max_rel_diff = np.inf  # Handle Division by Zero
            else:
                max_rel_diff = 0
                time_of_max_diff = "N/A"

            # Use a relative threshold (e.g., 20%) for the warning
            if max_rel_diff > 20.0:
                logger.warning(
                    f"[{fuel}] WARNING: Max rel. dev. is {max_rel_diff:.2f}% "
                    f"(abs: {max_abs_diff:.2f} tCO2 at {time_of_max_diff}). "
                    f"AVG rel. deviation: {mean_rel_diff:.4f}%"
                )
            else:
                logger.info(
                    f"[{fuel}] PASSED: Max rel. dev. is {max_rel_diff:.2f}% "
                    f"(abs: {max_abs_diff:.2f} tCO2 at {time_of_max_diff}). "
                    f"AVG rel. deviation: {mean_rel_diff:.4f}%"
                )

        self.plot_delta_profile(regional_emissions_final)
        self._plot_validation_comparison(original_hourly, resampled_hourly)

    def plot_delta_profile(self, regional_emissions_final, days_to_plot=3):
        """
        Plots the delta_emissions profile for each region to check for sawtooth patterns.
        """
        logger.info("Plotting delta profiles for validation...")

        for region, df_emi in regional_emissions_final.items():
            df_plot = df_emi.copy()
            df_plot['delta_emissions'] = df_plot['total_emissions'].diff()

            # Limit to the first few days for clarity
            start_date = df_plot.index.min()
            end_date = start_date + pd.Timedelta(days=days_to_plot)
            df_plot = df_plot.loc[start_date:end_date]

            if df_plot.empty:
                logger.warning(f"No data to plot for delta profile of region {region}")
                continue

            # noinspection PyTypeChecker
            with plt.style.context('default'):
                fig, ax = plt.subplots(figsize=(12, 6))
                ax.plot(df_plot.index, df_plot['delta_emissions'], label='Delta Emissions', alpha=0.7,
                        color='tab:blue')

                ax.set_title(f'Delta Emissions Profile for {region} (First {days_to_plot} Days)')
                ax.set_xlabel('Time')
                ax.set_ylabel('Delta Emissions (tCO2 per 15 min)')
                ax.legend()
                ax.grid(True, alpha=0.3)
                fig.autofmt_xdate(rotation=45)

                plot_filename = f'{self.out_dir_figures}/delta_profile_{region}_smoothed.png'
                try:
                    fig.savefig(plot_filename, bbox_inches='tight')
                    logger.info(f"Saved delta profile plot to {plot_filename}")
                except Exception as e:
                    logger.error(f"Failed to save plot: {e}")
                finally:
                    plt.close(fig)

    def _plot_validation_comparison(self, original_hourly, resampled_hourly):
        """
        Plots the original vs. resampled hourly emissions to visually inspect the allocation balance.
        """
        logger.info("Plotting allocation validation comparison...")
        
        # We only plot the 'total_emissions' for clarity
        df_plot = pd.DataFrame({
            'Original': original_hourly['total_emissions'],
            'Resampled': resampled_hourly['total_emissions']
        }).dropna()

        df_plot['Difference'] = (df_plot['Resampled'] - df_plot['Original']).abs()

        # Calculate metrics
        r2 = r2_score(df_plot['Original'], df_plot['Resampled'])
        mae = mean_absolute_error(df_plot['Original'], df_plot['Resampled'])
        mse = mean_squared_error(df_plot['Original'], df_plot['Resampled'])
        rmse = np.sqrt(mse)

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True, gridspec_kw={'height_ratios': [3, 1]})

        # Main plot: Original vs. Resampled
        ax1.plot(df_plot.index, df_plot['Original'], label='Original Hourly Emissions', color='blue', linewidth=1.5)
        ax1.plot(df_plot.index, df_plot['Resampled'], label='Resampled from 15-min', color='red', linestyle='--', linewidth=1)
        title = (
            f'Validation: Original vs. Resampled Hourly Total Emissions\n'
            f'R² = {r2:.4f} | MAE = {mae:.2f} | MSE = {mse:.2f} | RMSE = {rmse:.2f}'
        )
        ax1.set_title(title)
        ax1.set_ylabel('Emissions (tCO2)')
        ax1.legend()
        ax1.grid(True, which='major', linestyle='--', linewidth='0.5')

        # Difference plot
        ax2.plot(df_plot.index, df_plot['Difference'], label='Absolute Difference', color='green')
        ax2.set_title('Absolute Difference')
        ax2.set_xlabel('Timestamp (UTC)')
        ax2.set_ylabel('Emissions (tCO2)')
        ax2.legend()
        ax2.grid(True, which='major', linestyle='--', linewidth='0.5')

        plt.tight_layout()
        plot_filename = f'{self.out_dir_figures}/allocation_validation_comparison.png'
        try:
            fig.savefig(plot_filename)
            logger.info(f"Saved validation comparison plot to {plot_filename}")
        except Exception as e:
            logger.error(f"Failed to save validation plot: {e}")
        plt.close(fig)
