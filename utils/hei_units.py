"""HEI in the paper's unit, standard deviations (2026-11-09).

The paper standardizes HEI to mean 0 and standard deviation 1 across household-years, the way analyze/labor_supply/12_ does for
the main income table (projection-weighted, geographic non-movers, all years), as Allcott et al. (2019) standardize their Health
Index. Estimates on HEI in points become estimates in standard deviations once coefficients and standard errors are divided by
that standard deviation; tests and stars do not change. Levels become (level - mean) / standard deviation.
"""
import sys
from functools import lru_cache
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import numpy as np
import pandas as pd
import pyreadstat

COEF_COLS = ['b', 'se', 'jump', 'jump_se', 'jump12', 'jump12_se', 'step01', 'step01_se', 'lead2', 'lead2_se', 'pre', 'pre_se', 'jump_late', 'jump_late_se',
             'r0', 'r0_se', 'post', 'post_se', 'late', 'late_se', 'diff', 'diff_se', 'iv_implied', 'gap']
LEVEL_COLS = ['mean_pre', 'mean_m1']


@lru_cache(maxsize=1)
def hei_mean_sd():
    d, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'), usecols=['hei_usda', 'projection_factor', 'movers_f'])
    d = d[(d['movers_f'] == 0) & d['hei_usda'].notna() & d['projection_factor'].notna()]
    m = np.average(d['hei_usda'], weights=d['projection_factor'])
    return float(m), float(np.sqrt(np.average((d['hei_usda'] - m) ** 2, weights=d['projection_factor'])))


def hei_sd():
    return hei_mean_sd()[1]


def to_sd(df, outcome_col='outcome', name='hei_usda', mask=None):
    """A copy of df with the coefficient and level columns of the HEI rows in standard deviations."""
    mean, sd = hei_mean_sd()
    d = df.copy()
    rows = (d[outcome_col] == name) if mask is None else mask
    for c in COEF_COLS:
        if c in d.columns:
            d.loc[rows, c] = d.loc[rows, c].astype(float) / sd
    for c in LEVEL_COLS:
        if c in d.columns:
            d.loc[rows, c] = (d.loc[rows, c].astype(float) - mean) / sd
    return d
