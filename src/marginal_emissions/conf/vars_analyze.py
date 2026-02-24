"""
Environment variables for the msdr analysis of the final analysis dataframes.
"""
import pandas as pd
from pyprojroot import here

root = here()
data_dir = root / "data" / "processed"

# Data dictionary for executing everything in the cli
ANALYSIS_DFS = {
    '50Hertz': {
        '2023': pd.read_csv(f'{data_dir}/analysis_f_hertz_2023_15min_utc_202212232300_202401010000.csv'),
        '2024': pd.read_csv(f'{data_dir}/analysis_f_hertz_2024_15min_utc_202312232300_202501010000.csv')
    },
    'Amprion': {
        '2023': pd.read_csv(f'{data_dir}/analysis_amprion_2023_15min_utc_202212232300_202401010000.csv'),
        '2024': pd.read_csv(f'{data_dir}/analysis_amprion_2024_15min_utc_202312232300_202501010000.csv')
    },
    'TenneT': {
        '2023': pd.read_csv(f'{data_dir}/analysis_tennet_2023_15min_utc_202212232300_202401010000.csv'),
        '2024': pd.read_csv(f'{data_dir}/analysis_tennet_2024_15min_utc_202312232300_202501010000.csv')
    },
    'TransnetBW': {
        '2023': pd.read_csv(f'{data_dir}/analysis_transnet_bw_2023_15min_utc_202212232300_202401010000.csv'),
        '2024': pd.read_csv(f'{data_dir}/analysis_transnet_bw_2024_15min_utc_202312232300_202501010000.csv')
    }
}

# Datasets for the single execution in cli
F_HERTZ = ANALYSIS_DFS['50Hertz']
AMPRION = ANALYSIS_DFS['Amprion']
TENNET = ANALYSIS_DFS['TenneT']
TRANSNET_BW = ANALYSIS_DFS['TransnetBW']

TEST_DF = pd.read_csv(f'{data_dir}/test_final_tennet.csv')