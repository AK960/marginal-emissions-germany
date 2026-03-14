"""
This file contains logic to locally synch the results dir with the latex assets dir.
"""

import click

@click.command(name='synchtex')
def synchtex_group():
    """Synchs the analysis output folder with the LaTeX files to provide plots and figures."""

