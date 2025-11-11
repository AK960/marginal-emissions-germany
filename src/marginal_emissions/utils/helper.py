"""
This script contains helper functions.
"""

from . import logger
import chardet

def check_encoding(path) -> str | None:
    """
    :description: When a file is not encoded as utf-8, this function checks and returns the encoding
    :param path: Input file path.
    :return: Encoding of input file.
    """
    try:
        with open(path, 'rb') as f:
            raw = f.read(10000) # first 10 KB for encoding detection
            encoding = chardet.detect(raw)['encoding']
        logger.info(f'File is encoded as "{encoding}"')
        return encoding

    except FileNotFoundError:
        logger.error('File not found.')
        return None
    except UnicodeDecodeError:
        logger.error('Encoding is not correct.')
        return None
    except PermissionError:
        logger.error('Permission denied.')
        return None
    except IsADirectoryError:
        logger.error('Path is a directory, not a file.')
        return None
    except Exception as e:
        logger.error(e)
        return None


def say_hello(self):
    print("Hello from marginal_emissions!")

