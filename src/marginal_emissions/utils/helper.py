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

def plot_estimated_emissions(data, tso, year, run='msar'):
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

def plot_over_time(
        data, tso, col1, col2,
        y_label=None,
        out_filename = None,
        col1_label='Delta Emissions',
        col2_label='Delta Estimated Emissions',
        plot=False,
        root = here(),
        run = 'msar',
        year = None
):
    """
    Plots the estimated vs. original emissions and calculates performance metrics.
    Saves the plot as a PNG file.
    :param year: Year of the data
    :param data: Dataframe that contains col1 and col2
    :param tso: Transmission System Operator (TSO) name
    :param col1: Column with baseline data
    :param col1_label: Name of the column with baseline data for legend
    :param col2: Column with data to be plotted against the baseline
    :param col2_label: Name of the column to be plotted against the baseline
    :param y_label: Description of the y-axis
    :param out_filename: Filename to save the plot
    :param plot: Choose whether to plot data or save as png-file (Default: False -> save as png-file)
    :param root: Project root directory
    :param run: Description of the run for filename
    """
    tso = tso.capitalize() if tso else None
    # Filter data to remove NaNs (e.g., the first window)
    df_plot = data[[col1, col2]].copy()
    df_plot.index = pd.to_datetime(df_plot.index, format="ISO8601")

    # Calculate metrics before interpolation for validity
    df_metrics = df_plot.dropna()
    if df_metrics.empty:
        logger.error(f"No valid data points for plotting {out_filename}")
        return

    r2 = r2_score(df_metrics[col2], df_metrics[col1])
    mae = mean_absolute_error(df_metrics[col2], df_metrics[col1])
    mse = mean_squared_error(df_metrics[col2], df_metrics[col1])
    rmse = np.sqrt(mse)

    # Interpolate by time for complete plot
    if df_plot.isnull().values.any():
        logger.info(f"Interpolating missing values for plot: {out_filename}")
        df_plot = df_plot.interpolate(method='time')
        df_plot = df_plot.dropna()

    if df_plot.empty:
        logger.error("No valid data points for plotting (maybe window length > data length?)")
        return

    # noinspection PyTypeChecker
    with plt.style.context('default'):
        # Create Plot
        fig, ax = plt.subplots(figsize=(12, 6))

        ax.plot(df_plot.index, df_plot[col2], label=col2_label, alpha=0.7, color='tab:blue')
        ax.plot(df_plot.index, df_plot[col1], label=col1_label, alpha=0.7, linestyle='--', color='tab:orange')

        ax.set_title(f"{tso}\n| R² = {r2:.4f} | MAE = {mae:.4f} | MSE = {mse:.4f} | RMSE = {rmse:.4f} |")
        ax.set_ylabel(y_label)
        ax.set_xlabel("Time")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.autofmt_xdate(rotation=45)

        if not plot:
            # Save plot
            try:
                if out_filename is None:
                    save_dir = root / "results" / f"test_{run}"
                    filename = save_dir / f"estimated_emissions.png"
                else:
                    save_dir = root / "results" / f"run_{run}" / f"{tso}_{year}" / "figures"
                    filename = save_dir / f"{out_filename}"
                os.makedirs(save_dir, exist_ok=True)

                fig.savefig(filename, bbox_inches='tight')
                logger.info(f"Plot saved to {filename}")
            except Exception as e:
                logger.error(f"Failed to save image to file: {e}. Continuing...")
                plt.close()
            finally:
                plt.close(fig)

        else:
            plt.show()


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

