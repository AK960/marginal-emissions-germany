import logging
import sys

logger = logging.getLogger('marginal_emissions')
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt='[%(levelname)s][%(asctime)s][%(filename)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

logger.propagate = False  # Verhindert doppelte Ausgaben

__all__ = ['logger']

