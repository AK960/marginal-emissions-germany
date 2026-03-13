"""
Helper functions for the CLI commands
"""
import os
from pathlib import Path
from typing import List

import chardet
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.plotting import autocorrelation_plot
from pyprojroot import here
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from statsmodels.tsa.stattools import adfuller
import matplotlib.pyplot as plt
from scipy import stats
from scipy.stats import probplot
import numpy as np
from marginal_emissions import logger


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

def diagnose_model_fit(data, orig_col, esti_col):
    """
    Print diagnostic plots for model fit.
    """
    if data is None or data.empty:
        logger.error("No data available for model fit diagnostics.")
        return

    data.index = pd.to_datetime(data.index, format="ISO8601")

    # Analyse residuals
    residuals = data[orig_col] - data[esti_col]

    # noinspection PyTypeChecker
    with plt.style.context('default'):
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))

        for ax in axes.flat:
            ax.set_facecolor('white')

        # Residuals over time
        axes[0, 0].plot(data.index, residuals, color='blue', linewidth=0.8)
        axes[0, 0].set_title('Residuals over Time')
        axes[0, 0].axhline(y=0, color='orange', linestyle='--', linewidth=1.5)
        axes[0, 0].grid(True, alpha=0.3)

        # Histogram of residuals
        axes[0, 1].hist(residuals, bins=50, color='blue', alpha=0.7, edgecolor='orange')
        axes[0, 1].set_title('Distribution of Residuals')
        axes[0, 1].grid(True, alpha=0.3)

        # Q-Q Plot
        stats.probplot(residuals.dropna(), dist="norm", plot=axes[1, 0])
        axes[1, 0].get_lines()[0].set_color('blue')
        axes[1, 0].get_lines()[1].set_color('orange')
        axes[1, 0].set_title('Q-Q Plot')
        axes[1, 0].grid(True, alpha=0.3)

        # Autocorrelation of Residuals
        autocorrelation_plot(residuals.dropna(), ax=axes[1, 1])
        axes[1, 1].lines[0].set_color('blue')
        if len(axes[1, 1].lines) > 1:
            axes[1, 1].lines[1].set_color('orange')
        axes[1, 1].set_title('Autocorrelation of Residuals')
        axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.show()

def test_stationarity(timeseries, column_name):
    """
    Performs augmented Dickey-Fuller test_msdr for stationarity of a time series.
    """
    logger.info(f"Performing stationarity test_msdr for {column_name}")

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
