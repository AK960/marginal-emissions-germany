import logging

logger = logging.getLogger('marginal_emissions')

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(levelname)s][%(asctime)s][%(filename)s]_%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

__all__ = ['logger']

