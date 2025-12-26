"""
This file contains the Child-Class for making requests to the Entsoe API. It inherits parameters
and methods from the Base Client. The data retrieved using the class is considered raw data and saved in the respective directory. Engineering of an analysis dataframe will be conducted using the respective class.
""" # TODO: Write class to create final df - not necessarily CLI
import os.path
from pathlib import Path
from typing import Optional, Dict

import pytz
import requests
from entsoe.exceptions import NoMatchingDataError
import pandas as pd
from marginal_emissions.vars import *
from . import base_client, logger


class EntsoeClient(base_client.EnergyDataClient):
    """
    Client for scraping of electricity market data from ENTSO-E API (see README/Appendix/Important Links).

    The class encapsulates API communication and implements the necessary logic to request data from the ENTSO-E API.
    """
    def __init__(self, api_key: str, base_url: Optional[str] = None):
        super().__init__(api_key, base_url) # Include params and methods from Base Client (Parent)
        self.iec_codes = EIC_CONTROL_AREA_CODES

    def _base_request(self, params: Dict, start: pd.Timestamp, end: pd.Timestamp) -> requests.Response:
        """
        Basic logic to perform an api request. Adds base parameters and performs the request. The response is returned to the calling function.

        :param params: Dictionary with endpoint-specific parameters
        :param start: Start date of requested time frame
        :param end: End date of requested time frame
        :return: Http response from ENTSOE API
        """
        start_str = self._datetime_to_str(start)
        end_str = self._datetime_to_str(end)

        base_params = {
            'securityToken': self.api_key,
            'periodStart': start_str,
            'periodEnd': end_str
        }
        params.update(base_params) # Adds base parameters to params

        logger.info(f"Requesting data from {self.base_url}")

        try:
            response = requests.get(
                self.base_url,
                params=params,
                timeout=(10, 60)
            )

            response.raise_for_status()
            logger.info(f"Request returned with status code {response.status_code}")

        except requests.exceptions.Timeout as e:
            raise TimeoutError(
                f"Timeout while calling ENTSO-E API at {self.base_url}"
            ) from e

        except requests.exceptions.RequestException as e: # TODO: Test again for Amprion (http-error)
            # umfasst ConnectionError, HTTPError nach raise_for_status, etc.
            msg = f"Request to ENTSO-E API failed at {self.base_url} with error: {e}"
            if e.response is not None:
                msg += f"\nResponse body: {e.response.text}"
            raise RuntimeError(msg) from e

        # sometimes 200 with error -> check content
        if response.headers.get('content-type', '') in ['application/xml', 'text/xml']:
            if 'No matching data found' in response.text:
                raise NoMatchingDataError

        return response

    def get_actual_generation_per_production_type(self):
        pass

    def get_actual_generation_per_generation_unit(self, area: str, start_date: pd.Timestamp, end_date: pd.Timestamp):
        # Specify output params
        area_name = next((key for key, val in self.iec_codes.items() if val == area), area)
        out_dir = Path("data/raw/entsoe")
        out_dir.mkdir(parents=True, exist_ok=True)

        # Loop dict with area codes to retrieve data for all
        logger.info(f"Requesting data from ENTSO-E API for area {area_name}")
        params = {
            'documentType': 'A73',
            'processType': 'A16',
            'in_Domain': area
        }
        response = self._base_request(params=params, start=start_date, end=end_date)

        # Write content to a file
        try:
            out_file = out_dir / f"aggu_{area_name}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xml"
            out_file.write_text(response.text, encoding="utf-8")
            logger.info(f"Data saved to {out_file}")
        except Exception as e:
            logger.error(f"Error saving data to file: {e}")

        return None

    @staticmethod
    def _datetime_to_str(dtm: pd.Timestamp) -> str:
        """
        Convert a datetime object to a string in UTC of the form YYYYMMDDhh00

        :param dtm: Datetime object to convert
        :return: str from initial datetime
        """
        if dtm.tzinfo is not None and dtm.tzinfo != pytz.UTC:
            dtm = dtm.tz_convert("UTC")
        fmt = '%Y%m%d%H00'
        ret_str = dtm.round(freq='h').strftime(fmt)
        return ret_str