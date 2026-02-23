"""
Environment variables for the msdr analysis of the final analysis dataframes.
"""
import pandas as pd
from pyprojroot import here

root = here()
data_dir = root / "data" / "processed"

# Data dictionary for executing everything in the cli
ANALYSIS_DFS = {
    '50Hertz': pd.read_csv(f'{data_dir}/analysis_final_f_hertz_15min_utc_202212312300_202412312245'),
    'Amprion': pd.read_csv(f'{data_dir}/analysis_final_amprion_15min_utc_202212312300_202412312245'),
    'TenneT': pd.read_csv(f'{data_dir}/analysis_final_tennet_15min_utc_202212312300_202412312245'),
    'TransnetBW': pd.read_csv(f'{data_dir}/analysis_final_transnet_bw_15min_utc_202212312300_202412312245'),
}

# Datasets for the single execution in cli
F_HERTZ = ANALYSIS_DFS['50Hertz']
AMPRION = ANALYSIS_DFS['Amprion']
TENNET = ANALYSIS_DFS['TenneT']
TRANSNET_BW = ANALYSIS_DFS['TransnetBW']

TEST_DF = pd.read_csv(f'{data_dir}/test_final_tennet.csv')