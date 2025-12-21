from datetime import timedelta, datetime
from typing import Optional

from DateTime import DateTime
import click

from marginal_emissions import logger
from marginal_emissions.clients.entsoe_client import EntsoeClient
from marginal_emissions.vars import ENTSOE_BASE_URL, QUERY_START, QUERY_END
from dotenv import load_dotenv
import os

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
    help='Specifies the specific endpoint. To check available endpoints run "mef-tool entsoe -e". If nothing else is set, request will be performed for default timeframe.'
)
@click.option(
    '--is-test', '-t',
    is_flag=True,
    default=True,
    help='If set to true, fetch will be performed for one day according to default or passed value +24h.'
)
@click.option(
    '--start-date', '-sd',
    type=datetime,
    default=QUERY_START,
    help=f'Start date of dataset to be fetched. Default is {QUERY_START}. Format: "yyyy-mm-dd"'
)
@click.option(
    '--end-date', '-ed',
    type=datetime,
    default=QUERY_START + timedelta(hours=24),
    help=f'End date of dataset to be fetched. Default is {QUERY_END}. Format: "yyyy-mm-dd"'
)
def fetch_entsoe(
        req_type,
        is_test,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
):
    if is_test:

        if not start_date:
            start_date = QUERY_START
        if not end_date:
            end_date = start_date + timedelta(hours=24)
        click.echo(f'[TEST_RUN] from {start_date.Date()} until {end_date.Date()}')

    # Load vars
    load_dotenv()
    api_key = os.getenv("ENTSOE_API_KEY")
    base_url = ENTSOE_BASE_URL

    if not api_key:
        click.echo("API key cannot be None.")
        return
    if not base_url:
        click.echo("Endpoint cannot be None.")
        return

    client = EntsoeClient(
        api_key=api_key,
        base_url=ENTSOE_BASE_URL
    )

    match req_type:
        case 'actual_generation_per_generation_unit' | 'aggu':
            click.echo("Fetching generation data per generation unit...")
            client.get_actual_generation_per_generation_unit(
                start_date=start_date,
                end_date=end_date
            )
        case 'actual_generation_per_production_type' | 'agpt':
            click.echo("Fetching generation data per production type...")
            pass

# TODO: Implement date parsing function