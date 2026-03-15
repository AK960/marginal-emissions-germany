"""
This file contains the Client for making requests to the SMARD API. The data retrieved is considered raw data and is saved in the respective directory. API does not require authentication.
"""

from pathlib import Path
from typing import Optional, Dict

import pandas as pd
import json
import requests
from marginal_emissions.vars import SMARD_BASE_URL, SMARD_FILTER, SMARD_REGION

class SmardClient:
    def __init__(self):
        self.base_url = SMARD_BASE_URL
        self.prod_type = SMARD_FILTER
        self.area = SMARD_REGION
        self.resolution = 'quarterhour'

    def get_actual_generation(self):
        """
        This function retrieves the requested data from the SMARD API.
        :param area: Area as specified in /marginal_emissions/conf/vars_fetch.py
        :param prod_type: Production Type as specified in /marginal_emissions/conf/vars_fetch.py
        :return:
        """
        pass

    def _get_indices(self):
        """

        :return: None, save response as a file
        """


    def _convert_indices(self):
        """
        The function is applied to the returned JSON file from _get_indices and converts the index series from ms into a dictionary with indices and corresponding UTC timestamp.
        :return: Dictionary
        """
        pass
