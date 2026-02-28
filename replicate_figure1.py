"""
Replication of Allcott, Diamond, Dubé, Handbury, Rahkovsky, Schnell (2019)
Figure 1: Healthfulness of Grocery Purchases by Household Income

4-panel binscatter:
  A) Sugar per 1000 Cal
  B) Whole grain share (share of bread/baked goods calories from whole grain products)
  C) Produce (fruit + veg calorie share)
  D) Health Index per 1000 Cal (Allcott-style: year-demeaned normalization)

Loads the pre-built panel dataset from build_panel_dataset.py.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def log(msg):
    print(msg, flush=True)

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
DATASET = BASE / 'interim' / 'panel_dataset' / 'panel_hh_year.parquet'
OUTPUT  = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs')
OUTPUT.mkdir(exist_ok=True)

YEARS       = range(2004, 2017)
N_QUANTILES = 15

# ============================================================
# LOAD & FILTER
# ============================================================
log("Loading dataset...")
hhy = pd.read_parquet(DATASET)
hhy = hhy[hhy['panel_year'].isin(YEARS)].copy()
log(f"  {len(hhy):,} HH-year obs, {hhy['household_code'].nunique():,} HHs")

p1, p99 = hhy['HHAvIncome'].quantile(0.01), hhy['HHAvIncome'].quantile(0.99)
hhy = hhy[(hhy['HHAvIncome'] >= p1) & (hhy['HHAvIncome'] <= p99)]

# ============================================================
# CONTROLS FOR BINSCATTER
# ============================================================
hhy['age_bin'] = pd.cut(hhy['AgeInt'], bins=[0, 35, 45, 55, 65, 100], labels=False)
age_dum = pd.get_dummies(hhy['age_bin'], prefix='a', drop_first=True, dtype=float)
yr_dum  = pd.get_dummies(hhy['panel_year'], prefix='y', drop_first=True, dtype=float)
ctl_cols = list(age_dum.columns) + list(yr_dum.columns) + ['household_size']
hhy = pd.concat([hhy.reset_index(drop=True), age_dum.reset_index(drop=True), yr_dum.reset_index(drop=True)], axis=1)

# ============================================================
# BINSCATTER
# ============================================================
def _resid_wls(y, X, w):
    sw  = np.sqrt(w)
    Xc  = np.column_stack([np.ones(len(y)), X])
    beta, _, _, _ = np.linalg.lstsq(Xc * sw[:, None], y * sw, rcond=None)
    return y - Xc @ beta

def binscatter(df, yvar, xvar='HHAvIncome', controls=None, wvar='projection_factor', nq=15):
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
    ('sugar_per_1000cal', 'Sugars (g per 1,000 Cal)', 'Panel A: Sugars',       'fig1a_sugars'),
    ('Whole',             'Share whole grain',          'Panel B: Whole Grains', 'fig1b_whole_grains'),
    ('Produce',           'Calorie share from produce', 'Panel C: Produce',      'fig1c_produce'),
    ('HI_allcott',        'Health Index (std. dev.)',    'Panel D: Health Index', 'fig1d_health_index'),
]
for var, ylabel, title, fname in panels:
    log(f"  Plotting {var}...")
    xb, yb = binscatter(hhy, var, 'HHAvIncome', ctl_cols, 'projection_factor', N_QUANTILES)
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.scatter(xb, yb, color='#2c5f8a', s=50, zorder=5, edgecolors='white', linewidth=0.5)
    ax.set_xlabel('Household income ($000s)', fontsize=11)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_title(title, fontsize=13, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(OUTPUT / f'{fname}.png', bbox_inches='tight', dpi=150)
    plt.close()
    log(f"    Saved: {OUTPUT / fname}.png")

log("\n=== Summary Statistics ===")
for var, label, _, __ in panels:
    v = hhy[var].dropna()
    log(f"  {label}: mean={v.mean():.3f}, sd={v.std():.3f}, N={len(v):,}")
log(f"  HHAvIncome: mean={hhy['HHAvIncome'].mean():.1f}, "
    f"p25={hhy['HHAvIncome'].quantile(0.25):.1f}, p75={hhy['HHAvIncome'].quantile(0.75):.1f}")
log("Done!")
