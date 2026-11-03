"""
figure_prep_time_trend.py -- the preparation time of groceries bought, over time and by
income, for notes/obesity.tex.

Left: interim/rms_variety/prep_time_scores.parquet (a hand-assigned preparation score per
Nielsen product module, 1 = open and eat to 9 = long cooking; build/prep_time/) merged to
interim/rms_variety/rms_variety_module_fips_year.parquet (retail spending by module, county
and year): the spending-weighted mean score across modules by year, 2007-2020.
Right: interim/panel_dataset/panel_prep_time.parquet (prep_time_raw = the household's
spending-weighted mean module score in the year, build/prep_time/build_prep_time_panel.py)
merged to interim/panel_dataset/panel_hh_year.parquet for income and projection factors:
the projection-weighted mean household score by within-year quartile of real income,
2004-2020 (replaces the share-of-spending panel, Anya 2026-11-03).
Output: figs/health/prep_time/prep_time_trend.png
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

RMS = idir / 'rms_variety'
PD_ = idir / 'panel_dataset'
FIG = fdir / 'health' / 'prep_time'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#7a7a7a'
Q_COL = {1: RED, 2: ORANGE, 3: '#999999', 4: BLUE}
Q_LAB = {1: 'Bottom income quartile', 2: 'Second quartile', 3: 'Third quartile', 4: 'Top income quartile'}
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 10.5,
                     'axes.spines.top': False, 'axes.spines.right': False})

# ---- left: retail scanner, spending-weighted mean score across modules
pt = pd.read_parquet(RMS / 'prep_time_scores.parquet')
print('prep_time values:', pt['prep_time'].value_counts(dropna=False).sort_index().to_dict())
sp = pd.read_parquet(RMS / 'rms_variety_module_fips_year.parquet', columns=['product_module_code', 'year', 'total_spending'])
sp = sp[(sp['year'] >= 2007) & (sp['year'] <= 2020)].groupby(['product_module_code', 'year'], as_index=False)['total_spending'].sum()
d = sp.merge(pt[['product_module_code', 'prep_time']], on='product_module_code').dropna(subset=['prep_time'])
g = d.groupby('year').apply(lambda x: pd.Series({
    'mean_prep': np.average(x['prep_time'], weights=x['total_spending']),
    'modules': x['product_module_code'].nunique()}))
print(g.round(3).to_string())

# ---- right: household panel, mean score by within-year income quartile
hp = pd.read_parquet(PD_ / 'panel_prep_time.parquet', columns=['household_code', 'panel_year', 'prep_time_raw'])
hh = pd.read_parquet(PD_ / 'panel_hh_year.parquet', columns=['household_code', 'panel_year', 'real_income', 'projection_factor'])
hp = hp.merge(hh, on=['household_code', 'panel_year'], how='inner').dropna(subset=['prep_time_raw', 'real_income', 'projection_factor'])
hp = hp[hp['panel_year'].between(2004, 2020)]
hp['inc_q'] = hp.groupby('panel_year')['real_income'].transform(lambda s: pd.qcut(s.rank(method='first'), 4, labels=[1, 2, 3, 4])).astype(int)
byq = hp.groupby(['panel_year', 'inc_q']).apply(lambda x: np.average(x['prep_time_raw'], weights=x['projection_factor'])).unstack()
print(f'household-years: {len(hp):,}; prep_time_raw range {hp.prep_time_raw.min():.2f}-{hp.prep_time_raw.max():.2f}')
print(byq.round(3).to_string())

fig, axes = plt.subplots(1, 2, figsize=(16, 5.5))
ax = axes[0]
ax.plot(g.index, g['mean_prep'], color=BLUE, lw=2.5, marker='o', ms=5)
ax.set_ylabel('Mean preparation score\n(1 = open and eat, 9 = long cooking)'); ax.set_xlabel('Year')
ax.set_ylim(1.75, 2.0)
ax.set_title('Retail scanner: preparation score of groceries sold, spending-weighted')
ax = axes[1]
for q in (1, 2, 3, 4):
    ax.plot(byq.index, byq[q], color=Q_COL[q], lw=2.5 if q in (1, 4) else 1.8, marker='o', ms=5 if q in (1, 4) else 4, label=Q_LAB[q])
ax.set_ylabel('Mean household preparation score'); ax.set_xlabel('Year')
ax.set_title('Consumer panel: household preparation score by income quartile')
ax.legend(frameon=False)
fig.savefig(FIG / 'prep_time_trend.png', bbox_inches='tight', dpi=150)
plt.close(fig)
print(f'saved {FIG / "prep_time_trend.png"}')
