"""
Class for preprocessing the MEF time series.
"""
import os

import pandas as pd
import pytz

from marginal_emissions import logger
from marginal_emissions.conf.vars_preprocess import *

class MEFPreprocessor:
    def __init__(self):
        self.areas_df_dict = {}
        self.emissions = {}
        self.out_dir_interim = f'{root}/data/interim'
        self.out_dir_processed = f'{root}/data/processed'
        os.makedirs(self.out_dir_interim, exist_ok=True)
        os.makedirs(self.out_dir_processed, exist_ok=True)

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

        emissions = emissions.interpolate(method='time', limit_direction='both')
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

            # Only keep columns from GEN_COLS
            valid_cols = [c for c in gen.columns if c in GEN_COLS.values()]
            gen = gen.loc[:, valid_cols]

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

            # Aggregate conventional generation
            gen['total_generation'] = gen.sum(axis=1, numeric_only=True)

            # Fill instance df
            self.areas_df_dict[area] = gen

        for area in self.areas_df_dict:
            min_date = self.areas_df_dict[area].index.min().strftime('%Y%m%d%H%M')
            max_date = self.areas_df_dict[area].index.max().strftime('%Y%m%d%H%M')
            try:
                self.areas_df_dict[area].to_csv(f'{self.out_dir_interim}/generation_{area}_utc_{min_date}_{max_date}.csv')
                logger.info(f"Saved file to {self.out_dir_interim}/generation_{area}_utc_{min_date}_{max_date}.csv")
            except Exception as e:
                logger.error(f"Failed to save file: {e}")

        return self.areas_df_dict # In case one wants to continue working with the dataframe

    def alloc_emissions(self):
        if self.emissions is None or self.emissions.empty:
            raise ValueError("Emissions not prepared yet. Call prep_emissions() first.")
        if not self.areas_df_dict:
            raise ValueError("Generation not prepared yet. Call prep_generation() first.")

        # Regional allocation of emissions based on share of regional generation from total generation
        ## Aggregate total generation per production type and one hour
        total_gen_15min = pd.concat(self.areas_df_dict.values()).groupby(level=0).sum()
        total_gen_hourly = total_gen_15min.resample('1h').sum()

        ## Allocate emissions to regional_generation based on share of regional generation
        regional_emissions_final = {}

        for name, df_reg in self.areas_df_dict.items():
            fuels = ['lignite', 'hard_coal', 'fossile_gas', 'other_conventionals']
            regional_emissions_15min = pd.DataFrame(index=df_reg.index)

            for fuel in fuels:
                if fuel in df_reg.columns:
                    ## (1) Regional hourly generation per production type (share of German generation that come from each area)
                    regional_gen_hourly = df_reg[fuel].resample('h').sum()

                    ## Share of regional generation per production type on total generation per production type
                    regional_share_h = (regional_gen_hourly / total_gen_hourly[fuel]).fillna(0)  # In case of no generation in a region, set share to 0

                    ## Regional emissions per hour and production type
                    regional_emissions_hourly = self.emissions[fuel] * regional_share_h

                    ## (2) Temporal downscaling to 15 min via IEF interpolation --> implicit emissions factor
                    ### How much tCO2 where emitted per MWh within the hour?
                    ief_hourly = (regional_emissions_hourly / regional_gen_hourly).fillna(0)
                    ### Linear interpolation of emissions per quarter-hour instead of step function from hour to hour with hard breaks
                    ief_15min = ief_hourly.resample('15min').interpolate(method='time')
                    ### Fill edged if NaNs where generated
                    ief_15min = ief_15min.ffill().bfill()
                    ### Multiply with 15-minute generation
                    raw_emissions_15min = df_reg[fuel] * ief_15min

                    ## (3) Correct to initial hourly value (the sum of the four points may be larger than the original value, thus correction applied)
                    #raw_hourly_sum = raw_emissions_15min.resample('1h').transform('sum')
                    #target_hourly = regional_emissions_hourly.resample('15min').ffill()
                    #correction_factor = (target_hourly / raw_hourly_sum).fillna(1)

                    ## (4) Final assignment
                    regional_emissions_15min[fuel] = raw_emissions_15min #* correction_factor

            # Total emissions per control area per quarter-hour (final df: production type and total emissions per quarter-hour, weighed by generation)
            regional_emissions_15min['total_emissions'] = regional_emissions_15min.sum(axis=1)
            regional_emissions_final[name] = regional_emissions_15min
            regional_emissions_final[name] = regional_emissions_final[name].rename(columns=EMI_COLS)

        # Join data frames for the final processed output dataframe and split them by year (avoid ram overflow for too long datasets)
        splits = [
            ("2022-12-23 23:00:00+00:00", "2024-01-01 00:00:00+00:00", "2023"),
            ("2023-12-23 23:00:00+00:00", "2025-01-01 00:00:00+00:00", "2024")
        ]

        for reg in self.areas_df_dict:
            # Get regional frames
            df_emi = regional_emissions_final[reg]
            df_gen = self.areas_df_dict[reg]

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
                          ['total_generation', 'delta_generation', 'total_emissions', 'delta_emissions']]
            new_order = other_cols + ['total_generation', 'delta_generation', 'total_emissions', 'delta_emissions']
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
        if not self.areas_df_dict:
            raise ValueError("Generation not prepared yet. Call prep_generation() first.")

        logger.info("Starting validation of emission allocation...")
        # Sum all 15 min values
        total_regional_15min = pd.concat(regional_emissions_final.values()).groupby(level=0).sum()

        # Resample to one hour
        total_regional_hourly = total_regional_15min.resample('1h').sum()

        # Compare to original emissions
        common_idx = total_regional_hourly.index.intersection(self.emissions.index)

        diff = total_regional_hourly.loc[common_idx] - self.emissions.loc[common_idx]
        for col in diff.columns:
            if col in self.emissions.columns:
                max_diff = diff[col].abs().max()
                if max_diff > 0.1:
                    logger.warning(f"Validation Warning [{col}]: Max deviation of {max_diff:.4f} tCO2 detected.")
                else:
                    logger.info(f"Validation Passed [{col}]: Max deviation is {max_diff:.4e}).")