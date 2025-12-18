"""
This script contains helper functions.
"""
from typing import List

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


def say_hello(self):
    print("Hello from marginal_emissions!")

