"""Income-nutrition gradient binscatters EXCLUDING RETIREES.

Same sample, controls, weights, and binscatter machinery as
replicate_figure1_usda.py, dropping the household-years the labor-supply
note's Sample section excludes (2026-09-16): retirees, i.e. not-employed
occupation cell with average head age 67 or older (exact age from birth years),
and households whose assigned occupation is the armed forces (male head unless
absent or armed forces, then female; code 7).
Cross-sectional figure, so the flags apply to the current year only.

Outputs (moved from the figs root on 2026-11-05):
  figs/labor_supply/fig1d_health_index_no_retirees.png       (Syndigo hi_allcott)
  figs/labor_supply/fig1d_health_index_usda_no_retirees.png  (USDA hi_usda_allcott)
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def log(msg):
    print(msg, flush=True)

import sys
_r = Path(__file__).resolve().parent
while not (_r / 'paths.py').exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

DATASET = idir / 'panel_dataset' / 'panel_hh_year.parquet'
USDAP   = idir / 'panel_dataset' / 'hi_usda_panel.parquet'
PANELIST = idir / 'panelists' / 'panelists_all_years.parquet'
FIG_DIR = fdir / 'labor_supply'

YEARS       = range(2004, 2021)
N_QUANTILES = 15

log("Loading datasets...")
hhy = pd.read_parquet(DATASET, columns=[
    'household_code', 'panel_year', 'hh_real_income_avg', 'avg_age_hh_head',
    'household_size', 'projection_factor', 'hi_allcott'])
usd = pd.read_parquet(USDAP, columns=['household_code', 'panel_year', 'hi_usda_allcott'])
occ = pd.read_parquet(PANELIST, columns=[
    'household_code', 'panel_year', 'male_head_occupation', 'female_head_occupation'])
hhy = hhy[hhy['panel_year'].isin(YEARS)] \
    .merge(usd, on=['household_code', 'panel_year'], how='inner') \
    .merge(occ, on=['household_code', 'panel_year'], how='left')

# exclusions (labor_supply.tex "Sample"): retirees = not-employed occupation cell
# (codes 10/12) AND average head age >= 67 (a working 67-year-old stays in);
# armed forces = assigned occupation code 7 (male head unless absent or 7, then female)
m = pd.to_numeric(hhy['male_head_occupation'], errors='coerce')
f = pd.to_numeric(hhy['female_head_occupation'], errors='coerce')
occ_src = m.where(m.notna() & ~m.isin([0, 7]), f)
army = occ_src.eq(7)
retiree = occ_src.isin([10, 12]) & (hhy['avg_age_hh_head'] >= 67)
log(f"  dropping {retiree.sum():,} retiree (age 67+) and {army.sum():,} armed-forces HH-years "
    f"of {len(hhy):,} ({(retiree | army).mean():.1%})")
hhy = hhy[~(retiree | army)].copy()

p1, p99 = hhy['hh_real_income_avg'].quantile(0.01), hhy['hh_real_income_avg'].quantile(0.99)
hhy = hhy[(hhy['hh_real_income_avg'] >= p1) & (hhy['hh_real_income_avg'] <= p99)]

hhy['age_bin'] = pd.cut(hhy['avg_age_hh_head'], bins=[0, 35, 45, 55, 65, 100], labels=False)
age_dum = pd.get_dummies(hhy['age_bin'], prefix='a', drop_first=True, dtype=float)
yr_dum  = pd.get_dummies(hhy['panel_year'], prefix='y', drop_first=True, dtype=float)
ctl_cols = list(age_dum.columns) + list(yr_dum.columns) + ['household_size']
hhy = pd.concat([hhy.reset_index(drop=True), age_dum.reset_index(drop=True),
                 yr_dum.reset_index(drop=True)], axis=1)

def _resid_wls(y, X, w):
    sw  = np.sqrt(w)
    Xc  = np.column_stack([np.ones(len(y)), X])
    beta, _, _, _ = np.linalg.lstsq(Xc * sw[:, None], y * sw, rcond=None)
    return y - Xc @ beta

def binscatter(df, yvar, xvar='hh_real_income_avg', controls=None, wvar='projection_factor', nq=15):
    cols = [yvar, xvar, wvar] + (controls or [])
    d = df[cols].dropna().copy()
    y = d[yvar].values.astype(float)
    x = d[xvar].values.astype(float)
    w = d[wvar].values.astype(float)
    if controls:
        C = d[controls].values.astype(float)
        y = _resid_wls(y, C, w) + np.average(d[yvar].values, weights=w)
        x = _resid_wls(x, C, w) + np.average(d[xvar].values, weights=w)
    edges = np.percentile(x, np.linspace(0, 100, nq + 1))
    edges[-1] += 1
    b = np.digitize(x, edges)
    xm, ym = [], []
    for i in range(1, nq + 1):
        m = b == i
        if m.sum() > 0:
            xm.append(np.average(x[m], weights=w[m]))
            ym.append(np.average(y[m], weights=w[m]))
    return np.array(xm), np.array(ym)

panels = [
    ('hi_allcott',      'fig1d_health_index_no_retirees'),
    ('hi_usda_allcott', 'fig1d_health_index_usda_no_retirees'),
]
for var, fname in panels:
    log(f"  Plotting {var}...")
    xb, yb = binscatter(hhy, var, 'hh_real_income_avg', ctl_cols, 'projection_factor', N_QUANTILES)
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.scatter(xb, yb, color='#a63d40', s=50, zorder=5, edgecolors='white', linewidth=0.5)
    ax.set_xlabel('Income ($000s)', fontsize=17)
    ax.set_ylabel('Nutrition (std. dev.)', fontsize=19)
    ax.grid(True, alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(FIG_DIR / f'{fname}.png', bbox_inches='tight', dpi=150)
    plt.close()
    log(f"    Saved: {FIG_DIR / fname}.png")
log("Done!")
