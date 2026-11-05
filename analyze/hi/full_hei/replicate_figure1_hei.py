"""
Full-HEI version of the Allcott et al. (2019) Figure 1, health-index panel:
income vs. nutrition gradient, using the full HEI (hei_usda, 0-100) as the
nutrition measure. Standardized so the y-axis reads in standard deviations,
matching fig1d_health_index(_usda).png.

Same binscatter machinery / controls as replicate_figure1.py (age-bin, year,
household-size controls; projection-factor weights). Output suffixed _hei;
the Syndigo and simple-USDA panels are not touched.

Output: figs/fig1d_health_index_hei.png
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

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
DATASET = BASE / 'interim' / 'panel_dataset' / 'panel_hh_year.parquet'
HEIP    = BASE / 'interim' / 'panel_dataset' / 'hei_usda_panel.parquet'
FIG_DIR = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs')

YEARS       = range(2004, 2021)  # stop at 2020 to avoid 2021 definition change
N_QUANTILES = 15

# ============================================================
# LOAD & FILTER
# ============================================================
log("Loading datasets...")
hhy = pd.read_parquet(DATASET, columns=[
    'household_code', 'panel_year', 'hh_real_income_avg', 'avg_age_hh_head',
    'household_size', 'projection_factor'])
hei = pd.read_parquet(HEIP, columns=['household_code', 'panel_year', 'hei_usda'])
hhy = hhy[hhy['panel_year'].isin(YEARS)].merge(
    hei, on=['household_code', 'panel_year'], how='inner')
log(f"  {len(hhy):,} HH-year obs, {hhy['household_code'].nunique():,} HHs")

p1, p99 = hhy['hh_real_income_avg'].quantile(0.01), hhy['hh_real_income_avg'].quantile(0.99)
hhy = hhy[(hhy['hh_real_income_avg'] >= p1) & (hhy['hh_real_income_avg'] <= p99)]

# standardize the 0-100 HEI to std-dev units (projection-weighted), so the
# y-axis matches fig1d_health_index(_usda)'s "Nutrition (std. dev.)"
w = hhy['projection_factor'].values
m = np.average(hhy['hei_usda'], weights=w)
sd = np.sqrt(np.average((hhy['hei_usda'] - m) ** 2, weights=w))
hhy['hei_z'] = (hhy['hei_usda'] - m) / sd

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
# FIGURE (single fig1d panel, HEI)
# ============================================================
log("Creating figure...")
xb, yb = binscatter(hhy, 'hei_z', 'hh_real_income_avg', ctl_cols, 'projection_factor', N_QUANTILES)
fig, ax = plt.subplots(figsize=(6, 5))
ax.scatter(xb, yb, color='#a63d40', s=50, zorder=5, edgecolors='white', linewidth=0.5)
ax.set_xlabel('Income ($000s)', fontsize=17)
ax.set_ylabel('Nutrition (std. dev.)', fontsize=19)
ax.set_title(' ', fontsize=14, fontweight='bold')
ax.grid(True, alpha=0.3)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.tight_layout()
plt.savefig(FIG_DIR / 'fig1d_health_index_hei.png', bbox_inches='tight', dpi=150)
plt.close()
log(f"  Saved: {FIG_DIR / 'fig1d_health_index_hei.png'}")
log(f"  HEI: mean={hhy['hei_usda'].mean():.1f}, sd={sd:.1f}, N={len(hhy):,}")
log("Done!")
