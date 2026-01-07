import os
from datetime import timedelta, datetime
from typing import Optional

import click
import pandas as pd
from dotenv import load_dotenv

from marginal_emissions.clients.entsoe_client import EntsoeClient
from marginal_emissions.vars import ENTSOE_BASE_URL, QUERY_START, QUERY_END, EIC_CONTROL_AREA_CODES

@click.group(name='fetch')
def fetch_group():
    """Fetch data from an API."""
    pass

@fetch_group.command(name='entsoe')
@click.option(
    '--req-type', '-rt',
    type=click.Choice([
        'actual_generation_per_generation_unit', 'aggu',
        'actual_generation_per_production_type', 'agpt'
        ],
        case_sensitive=False
    ),
    required=True,
    help='Specifies the specific endpoint. To check available endpoints run "mef-tool entsoe -e"'
)
@click.option(
    '--is-test', '-t',
    is_flag=True,
    help='If set, fetch will be performed for one day according to default or passed timestamp +24h.'
)
@click.option(
    '--area', '-a',
    type=click.Choice(['50hertz', 'amprion', 'tennet', 'transnetbw'], case_sensitive=False),
    required=True,
    help='Specify the desired control area.'
)
@click.option(
    '--start-date', '-sd',
    type=click.DateTime(),
    default=QUERY_START,
    help=f'Start date of dataset to be fetched. Default is {QUERY_START}. Format must be "yyyy-mm-dd"'
)
@click.option(
    '--end-date', '-ed',
    type=click.DateTime(),
    default=QUERY_END,
    help=f'End date of dataset to be fetched. Default is {QUERY_END}. Format must be "yyyy-mm-dd"'
)
def fetch_entsoe(
        req_type,
        is_test,
        area,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
):
    """Fetch data from the ENTSO-E API."""
    if is_test:
        end_date = start_date + timedelta(days=1)
        click.echo("[TEST] Performing test run")

    # Convert to pandas datetime
    try:
        start_date = pd.to_datetime(start_date, format='%Y-%m-%d')
        end_date = pd.to_datetime(end_date, format='%Y-%m-%d')
    except ValueError:
        raise ValueError("Start and end date must be datetime objects.")

    # Load base vars
    try:
        load_dotenv()
        api_key = os.getenv("ENTSOE_API_KEY")
        base_url = ENTSOE_BASE_URL
        area = EIC_CONTROL_AREA_CODES[area.upper()]
    except ValueError:
        raise ValueError("API key, endpoint, or area cannot be None.")

    client = EntsoeClient(
        api_key=api_key,
        base_url=base_url
    )

    match req_type:
        case 'actual_generation_per_generation_unit' | 'aggu':
            click.echo(f"Fetching generation data per generation unit from {start_date} to {end_date}")
            client.get_actual_generation_per_generation_unit(
                area=area,
                start_date=start_date,
                end_date=end_date
            )
        case 'actual_generation_per_production_type' | 'agpt':
            click.echo(f"Fetching generation data per production type from {start_date.date()} to {end_date.date()}")
            client.get_actual_generation_per_production_type(
                area=area,
                start_date=start_date,
                end_date=end_date
            )
            pass