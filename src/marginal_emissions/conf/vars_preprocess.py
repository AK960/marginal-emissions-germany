"""
Variables necessary for preprocessing of SMARD and Agora raw data.
"""
import pandas as pd
from pyprojroot import here

root = here()

EMI_DICT = {
    'emi_2022': pd.read_csv(f'{root}/data/raw/emissions_ger_2022_hourly_lastweek.csv', sep=','),
    'emi_2023': pd.read_csv(f'{root}/data/raw/emissions_ger_2023_hourly.csv', sep=','),
    'emi_2024': pd.read_csv(f'{root}/data/raw/emissions_ger_2024_hourly.csv', sep=','),
    'emi_2025': pd.read_csv(f'{root}/data/raw/emissions_ger_2025_hourly_firstday.csv', sep=','),
}

EMI_COLS = {
    'Braunkohle': 'lignite',
    'Erdgas': 'fossile_gas',
    'Andere': 'other_conventionals',
    'Steinkohle': 'hard_coal',
    'Absolute Emissionen': 'total_emissions',
    'CO₂-Emissionsfaktor des Strommix': 'aef'
}

GEN_DICT = {
    '50hertz': pd.read_csv(
        f'{root}/data/raw/realisierte_erzeugung_50hertz_202212240000_202501020000_viertelstunde.csv',
        sep=';'
    ),
    'amprion': pd.read_csv(
        f'{root}/data/raw/realisierte_erzeugung_amprion_202212240000_202501020000_viertelstunde.csv',
        sep=';'
    ),
    'tennet': pd.read_csv(
        f'{root}/data/raw/realisierte_erzeugung_tennet_202212240000_202501020000_viertelstunde.csv',
        sep=';'
    ),
    'transnetbw': pd.read_csv(
        f'{root}/data/raw/realisierte_erzeugung_transnetbw_202212240000_202501020000_viertelstunde.csv',
        sep=';'
    )
}


GEN_COLS = {
    'Datum von': 'datetime',
    'Erdgas [MWh] Originalauflösungen': 'fossile_gas',
    'Braunkohle [MWh] Originalauflösungen': 'lignite',
    'Steinkohle [MWh] Originalauflösungen': 'hard_coal',
    'Sonstige Konventionelle [MWh] Originalauflösungen': 'other_conventionals'
}
