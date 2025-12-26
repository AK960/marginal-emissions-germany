""" This file contains recurring variables """
import pandas as pd

# CLI options
AVAILABLE_APIS = ['entsoe', 'smard', 'energycharts']

# Datetime
QUERY_START = pd.to_datetime('2023-01-01 00:00:00', format='%Y-%m-%d %H:%M:%S')
QUERY_END = pd.to_datetime('2024-12-31 23:59:59', format='%Y-%m-%d %H:%M:%S')

# ENTSOe
ENTSOE_BASE_URL = "https://web-api.tp.entsoe.eu/api"
EIC_CONTROL_AREA_CODES = { # Control Area Codes, others check README/Important Links/ENTSOe/EIC Manual & Codes
    "50HERTZ": "10YDE-VE-------2",
    "AMPRION": "10YDE-RWENET---I",
    "TENNET": "10YDE-EON------1",
    "TRANSNETBW": "10YDE-ENBW-----N"
}

# SMARD
SMARD_BASE_URL="https://api.smard.com/api/v1"

# EnergyCharts
ENERGYCHARTS_BASE_URL="https://api.energy-charts.info"

# ElectricityMaps
ELECTRICITYMAPS_BASE_URL=""