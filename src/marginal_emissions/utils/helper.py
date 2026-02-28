"""
Helper functions for the CLI commands
"""
import os
from typing import List

import numpy as np
from matplotlib import pyplot as plt
from statsmodels.tsa.stattools import adfuller
from pyprojroot import here
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

from . import logger
import chardet
from pathlib import Path
import pandas as pd
import pytz

def check_encoding(path) -> str | None:
    """
    :description: When a file is not encoded as utf-8, this function checks and returns the encoding. Excel files are
    binary and do not have text encoded.
    :param path: Input file path.
    :return: Encoding of an input file.
    """
    binary_extensions = {'.xlsx', '.xls', '.xlsm', '.xlsb'}

    logger.info(f'Checking encoding of file "{path}"...')
    try:
        file_path = Path(path)
        file_extension = file_path.suffix.lower()

        if file_extension in binary_extensions:
            logger.info(f'File "{path}" is binary, no encoding check needed')
            return 'binary'

        with open(path, 'rb') as f:
            raw = f.read(10000) # first 10 KB for encoding detection
            encoding = chardet.detect(raw)['encoding']
        logger.info(f'File is encoded as "{encoding}"')
        return encoding

    except FileNotFoundError:
        logger.error(f'File {path} not found')
        return None
    except PermissionError:
        logger.error('Permission denied')
        return None
    except IsADirectoryError:
        logger.error(f'{path} is a directory, not a file')
        return None
    except Exception as e:
        logger.error(e)
        return None

def search_df(df, search_pattern, use_regex=False, case_sensitive=False):
    """
    Search the dataframe for a string and return all matching rows.

    :param df: Dataframe to search.
    :param search_pattern: String to search for.
    :param use_regex: If true, uses regex for searching.
    :param case_sensitive: If true, searches for the exact match.
    :return: Dataframe with matching rows.
    """
    mask = df.astype(str).apply(
        lambda col: col.str.contains(
            search_pattern,
            case=case_sensitive,
            regex=use_regex,
            na = False
        )
    )

    result = df[mask.any(axis=1)]
    logger.info(f"Found {len(result)} rows matching '{search_pattern}' pattern.")
    return result

def get_all_subdirs(base_path: str = "./data") -> List[Path]:
    """Find subdirectories in a given path."""
    path = Path(base_path)
    if not path.exists():
        return []

    subdirs = sorted([p for p in path.rglob('*') if p.is_dir()])
    return subdirs

def plot_estimated_emissions(data, tso, year, run):
    """
    Plots the estimated vs. original emissions and calculates performance metrics.
    Saves the plot as a PNG file.
    """
    # Filter data to remove NaNs (e.g., the first window)
    df_plot = data.dropna(subset=['estimated_emissions', 'total_emissions'])

    if df_plot.empty:
        print("Warning: No valid data points for plotting (maybe window length > data length?)")
        return

    # Calculate metrics
    r2 = r2_score(df_plot['total_emissions'], df_plot['estimated_emissions'])
    mae = mean_absolute_error(df_plot['total_emissions'], df_plot['estimated_emissions'])
    mse = mean_squared_error(df_plot['total_emissions'], df_plot['estimated_emissions'])
    rmse = np.sqrt(mse)

    # Create Plot
    with plt.style.context('default'):
        plt.figure(figsize=(12, 6))
        plt.plot(df_plot.index, df_plot['total_emissions'], label='Original Emissions (Scaled)', alpha=0.7)
        plt.plot(df_plot.index, df_plot['estimated_emissions'], label='Model Estimation (Scaled)', alpha=0.7,
                 linestyle='--')

        plt.title(
            f"MSDR Model Validation - {tso}\nR² = {r2:.4f} | MAE = {mae:.4f} | MSE = {mse:.4f} | RMSE = {rmse:.4f}")
        plt.ylabel("Scaled Emissions")
        plt.xlabel("Time")
        plt.legend()
        plt.grid(True, alpha=0.3)

        # Save plot
        try:
            save_dir = here() / "results" / f"{tso}_run_{run}_{year}" / "figures"
            os.makedirs(save_dir, exist_ok=True)

            filename = save_dir / f"{tso}_{year}_prediction.png"
            plt.savefig(filename)
            plt.close()  # Close figure to free memory
            logger.info(f"Estimated plot saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save image to file: {e}. Continuing...")
        finally:
            plt.close()
plt.close()  # Ensure the figure is closed even on error

def test_stationarity(timeseries, column_name):
    """
    Performs augmented Dickey-Fuller test for stationarity of a time series.
    """
    logger.info(f"Performing stationarity test for {column_name}")

    timeseries_clean = timeseries.dropna()

    result = adfuller(timeseries_clean, autolag='AIC')

    # Extract results
    adf_statistic = result[0]
    p_value = result[1]
    used_lags = result[2]
    n_observations = result[3]
    critical_values = result[4]

    # Ergebnisse übersichtlich ausgeben
    print(f"ADF-Statistik:  {adf_statistic:.4f}")
    print(f"p-Wert:         {p_value:.4f}")
    print(f"Verwendete Lags: {used_lags}")
    print(f"Anzahl Beobachtungen: {n_observations}")
    print("Kritische Werte:")
    for key, value in critical_values.items():
        print(f"   {key}: {value:.4f}")

    # Print interpretation
    print("-" * 40)
    if p_value < 0.05:
        print("Ergebnis: p-Wert < 0.05. Die Nullhypothese wird abgelehnt.")
        print("-> Die Zeitreihe ist STATIONÄR (ohne stochastischen Trend).")
    else:
        print("Ergebnis: p-Wert >= 0.05. Die Nullhypothese kann NICHT abgelehnt werden.")
        print("-> Die Zeitreihe ist NICHT STATIONÄR (stochastischer Trend vorhanden).")
    print("\n")

    return result

def say_hello(self):
    print("Hello from marginal_emissions!")

