"""
This script contains helper functions.
"""
from . import logger
import chardet
from pathlib import Path

def check_encoding(path) -> str | None:
    """
    :description: When a file is not encoded as utf-8, this function checks and returns the encoding. Excel files are
    binary and do not have text encoded.
    :param path: Input file path.
    :return: Encoding of input file.
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


def say_hello(self):
    print("Hello from marginal_emissions!")

