"""
CLI command for fetching data from an API.
"""

import click
import re
from pyprojroot import here

from marginal_emissions import logger
from marginal_emissions.conf.vars_analyze import *
from marginal_emissions.core.msar import MSARAnalyzer
from marginal_emissions.utils.helper import *

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
    '--year', '-y',
    type=click.Choice([
        '2023', '2024', 'all'
    ],
    case_sensitive=False
    ),
    required=False,
    help='Select TSO for whose area to run analysis (not case sensitive).'
)
@click.option(
    '--is-test', '-t',
    is_flag=True,
    help='If set, analysis will be performed on shorter test_msdr dataset.'
)
def set_data(
        operator,
        year,
        is_test
):
    if is_test:
        logger.info("PERFORMING TEST RUN")
        _run_analysis(data=TEST_DF, is_test=True)
    elif operator:
        tso = operator.lower()
        yr = year.lower() if year else 'all'

        # Select data to run analysis on based on the dataframe input param
        match tso:
            case '50hertz':
                if yr == 'all':
                    _run_analysis(operator=tso, data=F_HERTZ)
                else:
                    _run_analysis(operator=tso, data={yr: F_HERTZ[yr]})
            case 'amprion':
                if yr == 'all':
                    _run_analysis(operator=tso, data=AMPRION)
                else:
                    _run_analysis(operator=tso, data={yr: AMPRION[yr]})
            case 'tennet':
                if yr == 'all':
                    _run_analysis(operator=tso, data=TENNET)
                else:
                    _run_analysis(operator=tso, data={yr: TENNET[yr]})
            case 'transnetbw':
                if yr == 'all':
                    _run_analysis(operator=tso, data=TRANSNET_BW)
                else:
                    _run_analysis(operator=tso, data={yr: TRANSNET_BW[yr]})
            case 'all':
                if yr == 'all':
                    for area, years_dict in ANALYSIS_DFS.items():
                        _run_analysis(operator=area, data=years_dict)
                else:
                    for area, years_dict in ANALYSIS_DFS.items():
                        _run_analysis(operator=area, data={yr: years_dict[yr]})
            case _:
                logger.error(f"Unknown argument '{tso}'. Run '--help' for more information.")

    else:
        logger.error("Must provide either -t or -tso and -y flag.")

def _run_analysis(data, operator=None, is_test=False):
    # Check last run for class initialization
    if not is_test:
        logger.info(f"Starting analysis for {operator}...")
        if operator is None:
            raise ValueError("Operator must be provided for non-test_msdr runs.")

        for year, df in data.items():
            # Run analysis
            try:
                logger.info(f"Starting analysis for {operator} in {year}")
                analyzer = MSARAnalyzer(tso=operator, data=df, year=year, test=is_test)
                analyzer.prepare()
                analyzer.fit_compute()
                analyzer.save_to_file(data=analyzer.final_df, filename='mef_final.csv')
                analyzer.save_to_file(data=analyzer.coeffs_df, filename='coefficients.csv')
                analyzer.save_to_file(data=analyzer.indicators, filename='indicators.json')
                plot_over_time(
                    data=analyzer.final_df,
                    tso=operator,
                    col1='delta_emissions',
                    col2='delta_estimated_emissions',
                    col1_label='Emissions',
                    col2_label='Estimated Emissions',
                    y_label='Emissions (Scaled)',
                    out_filename='estimated_emissions.png'
                )
                logger.info(f"Finished test_msdr run!")
                logger.info(f"Finished analysis for {operator} in {year}")
            except Exception as e:
                logger.error(f"Analysis failed with error: {e}")

    else:
        logger.info("Starting test_msdr analysis...")
        analyzer = MSARAnalyzer(data=data)
        analyzer.prepare()
        analyzer.fit_compute()
        analyzer.save_to_file(data=analyzer.final_df, filename='mef_final.csv')
        analyzer.save_to_file(data=analyzer.coeffs_df, filename='coefficients.csv')
        analyzer.save_to_file(data=analyzer.indicators, filename='indicators.json')
        plot_over_time(
            data=analyzer.final_df,
            tso='TenneT',
            col1='delta_emissions',
            col2='delta_estimated_emissions',
            col1_label='Emissions',
            col2_label='Estimated Emissions',
            y_label='Emissions (Scaled)'
        )
        logger.info(f"Finished test_msdr run!")