"""Binscatter of daily purchased calories per adult equivalent vs. income.

Companion to replicate_figure1_usda.py (same sample, controls, weights, and
binscatter machinery). The outcome is total_cals_usda scaled to a daily rate
and divided by the household's calorie-needs adult equivalents, so household
size/composition doesn't mechanically drive the income gradient.

Adult equivalents: each person's USDA Dietary Guidelines estimated calorie
need (moderately active, by age x sex) divided by 2,000 kcal. A HH of two
adult men = 2.6 AE.

The y-axis is an INDEX of the weighted sample mean (mean = 100): the levels
(~600-700 kcal/day per AE) are recorded at-home BARCODED purchases -- roughly
a third of daily energy intake, since Nielsen misses food away from home and
unscanned items -- so raw levels invite misreading as total intake.

Output: figs/calories_by_income_usda.png
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
USDAP   = BASE / 'interim' / 'panel_dataset' / 'hi_usda_panel.parquet'
PANELIST = BASE / 'interim' / 'panelists' / 'panelists_all_years.parquet'
FIG_DIR = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs')

YEARS       = range(2004, 2021)
N_QUANTILES = 15

# ============================================================
# ADULT EQUIVALENTS FROM HOUSEHOLD COMPOSITION
# ============================================================
def calorie_need(age, male):
    """USDA DGA estimated daily calorie need, moderately active (midpoints)."""
    bins  = [4, 9, 14, 19, 31, 51, np.inf]
    male_need   = [1200, 1500, 2000, 2600, 2700, 2500, 2300]
    female_need = [1200, 1400, 1800, 2000, 2100, 2000, 1800]
    idx = np.searchsorted(bins, age, side='right')
    return np.where(male, np.take(male_need, idx), np.take(female_need, idx))

log("Building adult equivalents from panelist composition...")
cols = (['household_code', 'panel_year', 'household_size',
         'male_head_birth', 'female_head_birth'] +
        [f'member_{i}_{v}' for i in range(1, 8) for v in ('birth', 'relationship_sex')])
pan = pd.read_parquet(PANELIST, columns=cols)
pan = pan[pan['panel_year'].isin(YEARS)]

people = []
for role, sexmale in [('male_head_birth', True), ('female_head_birth', False)]:
    p = pan[['household_code', 'panel_year', role]].rename(columns={role: 'birth'}).dropna()
    p['male'] = sexmale
    people.append(p)
for i in range(1, 8):
    p = pan[['household_code', 'panel_year', f'member_{i}_birth', f'member_{i}_relationship_sex']] \
        .rename(columns={f'member_{i}_birth': 'birth', f'member_{i}_relationship_sex': 'rel'}).dropna()
    p['male'] = p['rel'].isin([1, 3, 5])  # 1 son, 3 other male rel, 5 male unrelated
    people.append(p[['household_code', 'panel_year', 'birth', 'male']])
people = pd.concat(people, ignore_index=True)
people['age'] = (people['panel_year'] - people['birth']).clip(0, 100)
people['ae'] = calorie_need(people['age'].values, people['male'].values) / 2000.0

ae = people.groupby(['household_code', 'panel_year']).agg(
    ae_total=('ae', 'sum'), n_counted=('ae', 'size')).reset_index()
ae = ae.merge(pan[['household_code', 'panel_year', 'household_size']],
              on=['household_code', 'panel_year'])
log(f"  people counted vs household_size: "
    f"{(ae['n_counted'] == ae['household_size']).mean():.1%} exact match")
ae = ae[ae['n_counted'] == ae['household_size']]  # drop HHs with incomplete rosters

# ============================================================
# LOAD PANEL & OUTCOME (same sample as replicate_figure1_usda.py)
# ============================================================
log("Loading datasets...")
hhy = pd.read_parquet(DATASET, columns=[
    'household_code', 'panel_year', 'hh_real_income_avg', 'avg_age_hh_head',
    'household_size', 'projection_factor'])
usd = pd.read_parquet(USDAP, columns=['household_code', 'panel_year', 'total_cals_usda'])
hhy = hhy[hhy['panel_year'].isin(YEARS)].merge(
    usd, on=['household_code', 'panel_year'], how='inner')
hhy = hhy.merge(ae[['household_code', 'panel_year', 'ae_total']],
                on=['household_code', 'panel_year'], how='inner')
log(f"  {len(hhy):,} HH-year obs, {hhy['household_code'].nunique():,} HHs")

p1, p99 = hhy['hh_real_income_avg'].quantile(0.01), hhy['hh_real_income_avg'].quantile(0.99)
hhy = hhy[(hhy['hh_real_income_avg'] >= p1) & (hhy['hh_real_income_avg'] <= p99)]

hhy['cals_per_ae_day'] = hhy['total_cals_usda'] / 365 / hhy['ae_total']
c1, c99 = hhy['cals_per_ae_day'].quantile(0.01), hhy['cals_per_ae_day'].quantile(0.99)
hhy = hhy[(hhy['cals_per_ae_day'] >= c1) & (hhy['cals_per_ae_day'] <= c99)]

# ============================================================
# CONTROLS & BINSCATTER (identical machinery)
# ============================================================
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

# ============================================================
# FIGURE
# ============================================================
log("Plotting calories per adult equivalent vs income...")
xb, yb = binscatter(hhy, 'cals_per_ae_day', 'hh_real_income_avg', ctl_cols,
                    'projection_factor', N_QUANTILES)
mu = np.average(hhy['cals_per_ae_day'], weights=hhy['projection_factor'])
yb_idx = 100 * yb / mu
fig, ax = plt.subplots(figsize=(6, 5))
ax.axhline(100, color='gray', linewidth=1, alpha=0.6, zorder=1)
ax.scatter(xb, yb_idx, color='#a63d40', s=50, zorder=5, edgecolors='white', linewidth=0.5)
ax.set_xlabel('Income ($000s)', fontsize=17)
ax.set_ylabel('Grocery calories per adult equiv.\n(index, sample mean = 100)', fontsize=15)
ax.grid(True, alpha=0.3)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.tight_layout()
plt.savefig(FIG_DIR / 'calories_by_income_usda.png', bbox_inches='tight', dpi=150)
plt.close()
log(f"  Saved: {FIG_DIR / 'calories_by_income_usda.png'}")

v = hhy['cals_per_ae_day']
log(f"\nDaily cals per AE (recorded at-home purchases): "
    f"weighted mean={mu:.0f}, sd={v.std():.0f}, N={len(v):,}")
log(f"Binscatter range: {yb.min():.0f} to {yb.max():.0f} kcal "
    f"= index {yb_idx.min():.1f} to {yb_idx.max():.1f}")
