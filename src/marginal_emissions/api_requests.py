import logging
from typing import Optional
import pandas as pd
import requests
from pandas import DataFrame
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

class EnergyChartsClient:
    BASE_URL = "https://api.energy-charts.info"

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()

    def get_public_power(
            self,
            start_date: str,
            end_date: str,
            country: str = "de"
    ) -> Optional[DataFrame]:
        """
        Fetch public net electricity production for a given country for each production type from EnergyCharts.

        Args:
            start_date: Start date of the data in YYYY-MM-DD format
            end_date: End date of the data in YYYY-MM-DD format
            country: Country code of the country to fetch data for (default: "de")

        Returns:
            Pandas Dataframe with the fetched data

        Raises:
            ValueError: Invalid parameters
            RequestException: API request failed
        """
        # Validate parameters
        if not all([start_date, end_date, country]):
            raise ValueError("Missing required parameter(s)")

        params = {
            "start": start_date,
            "end": end_date,
            "country": country,
        }

        try:
            response = self.session.get(
                f"{self.BASE_URL}/public_power",
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            logger.info(f"[Info] Successfully fetched data. Processing ...")

            return self._parse_response(data, key = "production_types")

        except RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        
        except (KeyError, ValueError) as e:
            logger.error(f"Data parsing failed: {e}")
            raise ValueError(f"Invalid API response structure: {e}")

    def get_cbpf(
            self,
            start_date,
            end_date,
            country="de"
    ) -> Optional[DataFrame]:
        """
        Fetch cross-border physical flow data (cbpf) of electricity in GW between a specified country and its neighbors
        from EnergyCharts.

        Args:
            start_date: Start date of the data in YYYY-MM-DD format
            end_date: End date of the data in YYYY-MM-DD format
            country: Country code of the country to fetch data for (default: "de")

        Returns:
            Pandas Dataframe with the fetched data

        Raises:
            ValueError: Invalid parameters
            RequestException: API request failed
        """
        # Validate parameters
        if not all([start_date, end_date, country]):
            raise ValueError("Missing required parameter(s)")

        params = {
            "start": start_date,
            "end": end_date,
            "country": country,
        }

        try:
            response = self.session.get(
                f"{self.BASE_URL}/cbpf",
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            logger.info(f"[Info] Successfully fetched data. Processing ...")

            return self._parse_response(data, key = "countries")

        except RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

        except (KeyError, ValueError) as e:
            logger.error(f"Data parsing failed: {e}")
            raise ValueError(f"Invalid API response structure: {e}")

    @staticmethod
    def _parse_response(data: dict, key: str) -> DataFrame:
        """
        Universal parser for EnergyCharts API responses.

        Args:
            data: API response JSON as dictionary
            key: The data key to process ('production_types' or 'countries')

        Returns:
            DataFrame with timestamp and data columns
        """
        # Validate parameters
        if not all(k in data for k in ['unix_seconds', key]):
            raise ValueError("Missing required parameter(s)")

        df = pd.DataFrame({
            'timestamp': pd.to_datetime(data['unix_seconds'], unit='s', utc=True),
        })

        for item in data[key]:
            if 'name' in item and 'data' in item:
                df[item['name']] = item['data']
            else:
                logger.warning("Skipping invalid entry")

        if 'deprecated' in data:
            df['deprecated'] = data['deprecated']

        return df

    