"""
USDA-data version of replicate_figure1.py (Allcott et al. 2019 Figure 1).

Identical binscatter machinery and controls; the four outcomes come from the
USDA-based HH-year panel (build_hi_usda_panel.py) instead of the Syndigo panel.
Outputs are suffixed _usda -- the Syndigo originals are NOT overwritten.

  A) fig1a_sugars_usda        (sugar_per_1000cal_usda)
  B) fig1b_whole_grains_usda  (whole_usda)
  C) fig1c_produce_usda       (produce_usda)
  D) fig1d_health_index_usda  (hi_usda_allcott)

Income, demographics, weights, and controls all come from panel_hh_year.parquet
(source-independent), so any difference vs. the originals reflects the
nutrition source alone.
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
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
DATASET = idir / 'panel_dataset' / 'panel_hh_year.parquet'
USDAP   = idir / 'panel_dataset' / 'hi_usda_panel.parquet'
FIG_DIR = fdir / 'gradients'

YEARS       = range(2004, 2021)  # stop at 2020 to avoid 2021 definition change
N_QUANTILES = 15

# ============================================================
# LOAD & FILTER
# ============================================================
log("Loading datasets...")
hhy = pd.read_parquet(DATASET, columns=[
    'household_code', 'panel_year', 'hh_real_income_avg', 'avg_age_hh_head',
    'household_size', 'projection_factor'])
usd = pd.read_parquet(USDAP, columns=[
    'household_code', 'panel_year', 'hi_usda_allcott',
    'sugar_per_1000cal_usda', 'produce_usda', 'whole_usda'])
hhy = hhy[hhy['panel_year'].isin(YEARS)].merge(
    usd, on=['household_code', 'panel_year'], how='inner')
log(f"  {len(hhy):,} HH-year obs, {hhy['household_code'].nunique():,} HHs")

p1, p99 = hhy['hh_real_income_avg'].quantile(0.01), hhy['hh_real_income_avg'].quantile(0.99)
hhy = hhy[(hhy['hh_real_income_avg'] >= p1) & (hhy['hh_real_income_avg'] <= p99)]

# ============================================================
# CONTROLS FOR BINSCATTER (identical to replicate_figure1.py)
# ============================================================
hhy['age_bin'] = pd.cut(hhy['avg_age_hh_head'], bins=[0, 35, 45, 55, 65, 100], labels=False)
age_dum = pd.get_dummies(hhy['age_bin'], prefix='a', drop_first=True, dtype=float)
yr_dum  = pd.get_dummies(hhy['panel_year'], prefix='y', drop_first=True, dtype=float)
ctl_cols = list(age_dum.columns) + list(yr_dum.columns) + ['household_size']
hhy = pd.concat([hhy.reset_index(drop=True), age_dum.reset_index(drop=True),
                 yr_dum.reset_index(drop=True)], axis=1)

# ============================================================
# BINSCATTER (identical machinery)
# ============================================================
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

# ============================================================
# FIGURE
# ============================================================
log("Creating figures...")
panels = [
    ('sugar_per_1000cal_usda', 'Sugars (g per 1,000 Cal)',                 ' ', 'fig1a_sugars_usda'),
    ('whole_usda',             'Share bread calories from whole grains',   ' ', 'fig1b_whole_grains_usda'),
    ('produce_usda',           'Calorie share from fruit+veg',             ' ', 'fig1c_produce_usda'),
    ('hi_usda_allcott',        'Nutrition (std. dev.)',                    ' ', 'fig1d_health_index_usda'),
]
for var, ylabel, title, fname in panels:
    log(f"  Plotting {var}...")
    xb, yb = binscatter(hhy, var, 'hh_real_income_avg', ctl_cols, 'projection_factor', N_QUANTILES)
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.scatter(xb, yb, color='#a63d40', s=50, zorder=5, edgecolors='white', linewidth=0.5)
    ax.set_xlabel('Income ($000s)', fontsize=17)
    ax.set_ylabel(ylabel, fontsize=19)
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(FIG_DIR / f'{fname}.png', bbox_inches='tight', dpi=150)
    plt.close()
    log(f"    Saved: {FIG_DIR / fname}.png")

log("\n=== Summary Statistics ===")
for var, label, _, __ in panels:
    v = hhy[var].dropna()
    log(f"  {label}: mean={v.mean():.3f}, sd={v.std():.3f}, N={len(v):,}")
log("Done!")
