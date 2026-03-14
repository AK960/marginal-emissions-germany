"""
This file contains environment and filter variables.
"""

import pandas as pd
from pyprojroot import here

# ---------------------- Global Vars ----------------------
ROOT = here()
RESULTS_DIR = ROOT / "results"
DATA_DIR = ROOT / "data"

# ---------------------- Preprocess ----------------------
EMI_DICT = {
    'emi_2022': pd.read_csv(f'{DATA_DIR}/raw/emissions_ger_2022_hourly_lastweek.csv', sep=','),
    'emi_2023': pd.read_csv(f'{DATA_DIR}/raw/emissions_ger_2023_hourly.csv', sep=','),
    'emi_2024': pd.read_csv(f'{DATA_DIR}/raw/emissions_ger_2024_hourly.csv', sep=','),
    'emi_2025': pd.read_csv(f'{DATA_DIR}/raw/emissions_ger_2025_hourly_firstday.csv', sep=','),
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
        f'{DATA_DIR}/raw/realisierte_erzeugung_50hertz_202212240000_202501020000_viertelstunde.csv',
        sep=';'
    ),
    'amprion': pd.read_csv(
        f'{DATA_DIR}/raw/realisierte_erzeugung_amprion_202212240000_202501020000_viertelstunde.csv',
        sep=';'
    ),
    'tennet': pd.read_csv(
        f'{DATA_DIR}/raw/realisierte_erzeugung_tennet_202212240000_202501020000_viertelstunde.csv',
        sep=';'
    ),
    'transnetbw': pd.read_csv(
        f'{DATA_DIR}/raw/realisierte_erzeugung_transnetbw_202212240000_202501020000_viertelstunde.csv',
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

# CLI options
AVAILABLE_APIS = ['entsoe']

# Datetime
QUERY_START = pd.to_datetime('2022-12-24 00:00:00', format='%Y-%m-%d %H:%M:%S')
QUERY_END = pd.to_datetime('2025-01-01 23:59:59', format='%Y-%m-%d %H:%M:%S')

# ENTSOe
ENTSOE_BASE_URL = 'https://web-api.tp.entsoe.eu/api'
EIC_CONTROL_AREA_CODES = { # Control Area Codes, others check README/Important Links/ENTSOe/EIC Manual & Codes
    '50HERTZ': '10YDE-VE-------2',
    'AMPRION': '10YDE-RWENET---I',
    'TENNET': '10YDE-EON------1',
    'TRANSNETBW': '10YDE-ENBW-----N'
}

# SMARD
SMARD_BASE_URL = 'https://www.smard.de/app'
SMARD_FILTER = {
    'Stromerzeugung Braunkohle': 1223,
    'Stromerzeugung Kernenergie': 1224,
    'Stromerzeugung Wind Offshore': 1225,
    'Stromerzeugung Wasserkraft': 1226,
    'Stromerzeugung Sonstige Konventionelle': 1227,
    'Stromerzeugung Sonstige Erneuerbare': 1228,
    'Stromerzeugung Biomasse': 4066,
    'Stromerzeugung Wind Onshore': 4067,
    'Stromerzeugung Photovoltaik': 4068,
    'Stromerzeugung Steinkohle': 4069,
    'Stromerzeugung Pumpspeicher': 4070,
    'Stromerzeugung Erdgas': 4071
}
SMARD_REGION = {
    '50HERTZ': '50Hertz',
    'AMPRION': 'Amprion',
    'TENNET': 'TenneT',
    'TRANSNETBW': 'TransnetBW'
}

# EnergyCharts
ENERGYCHARTS_BASE_URL = 'https://api.energy-charts.info'

# ElectricityMaps
ELECTRICITYMAPS_BASE_URL = ''