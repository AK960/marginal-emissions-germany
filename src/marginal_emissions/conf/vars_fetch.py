"""
This file contains environment and filter variables that are needed for the API request to the data platforms.
"""
import pandas as pd

# CLI options
AVAILABLE_APIS = ['entsoe', 'energycharts']

# Datetime
QUERY_START = pd.to_datetime('2023-01-01 00:00:00', format='%Y-%m-%d %H:%M:%S')
QUERY_END = pd.to_datetime('2024-12-31 23:59:59', format='%Y-%m-%d %H:%M:%S')

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