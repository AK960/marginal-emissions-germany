import click

from marginal_emissions import logger
from marginal_emissions.clients.entsoe_client import EntsoeClient
from marginal_emissions.vars import ENTSOE_BASE_URL
from dotenv import load_dotenv
import os

@click.group(name='fetch')
def fetch_group():
    """Fetch data from an API."""
    pass

fetch_group.add_command(name='entsoe')
click.option(
    '--actual-generation-per-generation-unit', '-agpu',
    help='Fetch actual generation per generation unit'
)
def fetch_entsoe():
    # Get API key from .env
    load_dotenv()
    api_key = os.getenv("ENTSOE_API_KEY")
    if not api_key:
        logger.error("No API key found in .env file.")
        return

    client = EntsoeClient(
        api_key=api_key,
        base_url=ENTSOE_BASE_URL
    )
    client.get_actual_generation_per_generation_unit()

# TODO: Implement date parsing function