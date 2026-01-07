"""
This file contains the Child-Class for making requests to the Entsoe API. It inherits parameters
and methods from the Base Client. The data retrieved using the class is considered raw data and saved in the respective directory.
"""

from pathlib import Path
from typing import Optional, Dict

import pytz
import requests
from entsoe.exceptions import NoMatchingDataError
from entsoe.parsers import parse_generation

from marginal_emissions.vars import *
from . import base_client, logger

