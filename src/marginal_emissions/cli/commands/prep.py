"""
CLI command to run data preprocessing and create analysis dataset.
"""

import click

@click.group(name='prep')
def prep_group():
    """Functions to perform data preprocessing."""
    pass

@prep_group.command(name='emissions')
def prep_emissions():
    pass

@prep_group.command(name='generation')
def prep_conv():
    pass

