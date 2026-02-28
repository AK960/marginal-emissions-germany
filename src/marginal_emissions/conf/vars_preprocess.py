"""
Variables necessary for preprocessing of SMARD and Agora raw data.
"""

GEN_COLS = {
    'Datum von': 'datetime',
    'Erdgas [MWh] Originalauflösungen': 'fossile_gas',
    'Braunkohle [MWh] Originalauflösungen': 'lignite',
    'Steinkohle [MWh] Originalauflösungen': 'hard_coal',
    'Sonstige Konventionelle [MWh] Originalauflösungen': 'other_conventionals'
}

EMI_COLS = {
    'Braunkohle': 'lignite',
    'Erdgas': 'fossile_gas',
    'Andere': 'other_conventionals',
    'Steinkohle': 'hard_coal',
    'Absolute Emissionen': 'total_emissions',
    'CO₂-Emissionsfaktor des Strommix': 'aef'
}