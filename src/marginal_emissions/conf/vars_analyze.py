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
        '2023': pd.read_csv(f'{data_dir}/final_f_hertz_2023_15min_utc_202212232315_202401010000.csv',
                            index_col=0),
        '2024': pd.read_csv(f'{data_dir}/final_f_hertz_2024_15min_utc_202312232300_202501010000.csv',
                            index_col=0)
    },
    'Amprion': {
        '2023': pd.read_csv(f'{data_dir}/final_amprion_2023_15min_utc_202212232315_202401010000.csv',
                            index_col=0),
        '2024': pd.read_csv(f'{data_dir}/final_amprion_2024_15min_utc_202312232300_202501010000.csv',
                            index_col=0)
    },
    'TenneT': {
        '2023': pd.read_csv(f'{data_dir}/final_tennet_2023_15min_utc_202212232315_202401010000.csv',
                            index_col=0),
        '2024': pd.read_csv(f'{data_dir}/final_tennet_2024_15min_utc_202312232300_202501010000.csv',
                            index_col=0)
    },
    'TransnetBW': {
        '2023': pd.read_csv(f'{data_dir}/final_transnet_bw_2023_15min_utc_202212232315_202401010000.csv',
                            index_col=0),
        '2024': pd.read_csv(f'{data_dir}/final_transnet_bw_2024_15min_utc_202312232300_202501010000.csv',
                            index_col=0)
    }
}

# Datasets for the single execution in cli
F_HERTZ = ANALYSIS_DFS['50Hertz']
AMPRION = ANALYSIS_DFS['Amprion']
TENNET = ANALYSIS_DFS['TenneT']
TRANSNET_BW = ANALYSIS_DFS['TransnetBW']

TEST_DF = pd.read_csv(f'{data_dir}/test_tennet.csv')

