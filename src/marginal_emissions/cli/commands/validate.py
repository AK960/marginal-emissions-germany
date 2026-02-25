"""
CLI command for validating the MEF time series.
"""

import click

from marginal_emissions import logger
from marginal_emissions.core.validate import MEFValidator

@click.group(name='validation')
def validation_group():
    """
    Run MEF Validation for a single area and year.
    """
    pass

@validation_group.command(name='run')
@click.option(
    '--operator', '-tso',
    type=click.Choice([
        '50Hertz', 'Amprion', 'TenneT', 'TransnetBW', 'All'
    ],
    case_sensitive=False
    ),
    required=False,
    help='Select TSO for whose area to run analysis (not case sensitive).'
)
@click.option(
    '--year', '-y',
    type=click.Choice([
        '2023', '2024'
    ],
    case_sensitive=False
    ),
    required=False,
    help='Select TSO for whose area to run analysis (not case sensitive).'
)
def validate(
        operator,
        year
):
    print("Works")
