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
    'f_hertz': pd.read_csv(
        f'{root}/data/raw/Realisierte_Erzeugung_50Hertz_202212240000_202501020000_Viertelstunde.csv',
        sep=';'
    ),
    'amprion': pd.read_csv(
        f'{root}/data/raw/Realisierte_Erzeugung_Amprion_202212240000_202501020000_Viertelstunde.csv',
        sep=';'
    ),
    'tennet': pd.read_csv(
        f'{root}/data/raw/Realisierte_Erzeugung_TenneT_202212240000_202501020000_Viertelstunde.csv',
        sep=';'
    ),
    'transnet_bw': pd.read_csv(
        f'{root}/data/raw/Realisierte_Erzeugung_TransnetBW_202212240000_202501020000_Viertelstunde.csv',
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