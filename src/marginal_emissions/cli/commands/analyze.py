"""
CLI command for fetching data from an API.
"""

import click
import re
from pyprojroot import here

from marginal_emissions import logger
from marginal_emissions.conf.vars_analyze import *
from marginal_emissions.core.msdr import MSDRAnalyzer


@click.group(name='analysis')
def analysis_group():
    """
    Run MSDR analysis for single or all dataframes
    """
    pass

@analysis_group.command(name='run')
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
    '--is-test', '-t',
    is_flag=True,
    help='If set, analysis will be performed on shorter test dataset.'
)
def set_data(operator, is_test):
    if is_test:
        logger.info("PERFORMING TEST RUN")
        _run_analysis(operator='test', data=TEST_DF, is_test=True)
    elif operator:
        tso = operator.lower()

        # Select data to run analysis on based on the dataframe input param
        match tso:
            case '50hertz':
                _run_analysis(operator=tso, data=F_HERTZ)
            case 'amprion':
                _run_analysis(operator=tso, data=AMPRION)
            case 'tennet':
                _run_analysis(operator=tso, data=TENNET)
            case 'transnetbw':
                _run_analysis(operator=tso, data=TRANSNET_BW)
            case 'all':
                for area, df in ANALYSIS_DFS.items():
                    _run_analysis(operator=area, data=df)
            case _:
                print(f"InputError: Unknown argument '{tso}'. Run '--help' for more information.")

    else:
        logger.error("Must provide either -t or -tso flag.")

def _run_analysis(operator, data, is_test=False):
    # Check last run for class initialization
    if not is_test:
        new_run = _check_last_run(name=operator) + 1
        for year, df in data.items():
            # Run analysis
            try:
                logger.info(f"Starting analysis for {operator} in {year}")
                analyzer = MSDRAnalyzer(tso=operator, data=df, run=new_run, year=year)
                analyzer.prepare()
                analyzer.fit()
                analyzer.predict()
                analyzer.compute()
                analyzer.merge_mef()
                logger.info(f"Finished analysis for {operator} in {year}")
            except Exception as e:
                logger.error(f"Analysis failed with error: {e}")

    else:
        new_run = 0
        analyzer = MSDRAnalyzer(tso=operator, data=data, run=new_run, year=0)
        analyzer.prepare()
        analyzer.fit()
        analyzer.predict()
        analyzer.compute()
        analyzer.merge_mef()
        logger.info(f"Finished test run!")

def _check_last_run(name) -> int:
    # Check out dir
    rt = here()
    out_dir = rt / "results"
    out_dir.mkdir(parents=True, exist_ok=True)

    max_run = 0
    pattern = re.compile(fr'^{re.escape(name)}_run_(\d+)', re.IGNORECASE)

    for item in out_dir.iterdir():
        if item.is_dir():
            match = pattern.search(item.name)
            if match:
                current_run = int(match.group(1))
                if current_run > max_run:
                    max_run = current_run

    return max_run