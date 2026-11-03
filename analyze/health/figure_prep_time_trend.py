"""
figure_prep_time_trend.py -- the preparation time of groceries bought, over time, for
notes/obesity.tex. Reads interim/rms_variety/prep_time_scores.parquet (a hand-scored
minutes-of-preparation value per Nielsen product module, build/prep_time/) and
interim/rms_variety/rms_variety_module_fips_year.parquet (retail spending by module, county
and year). Output: figs/health/prep_time_trend.png: spending-weighted mean preparation
score across modules by year (left), and the spending share of modules scored at the
lowest preparation level (right).
"""
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/rms_variety')
FIG = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs') / 'health'
BLUE, RED, GREY = '#2c5f8a', '#a63d40', '#7a7a7a'
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 10.5,
                     'axes.spines.top': False, 'axes.spines.right': False})

pt = pd.read_parquet(BASE / 'prep_time_scores.parquet')
print('prep_time values:', pt['prep_time'].value_counts(dropna=False).sort_index().to_dict())
sp = pd.read_parquet(BASE / 'rms_variety_module_fips_year.parquet', columns=['product_module_code', 'year', 'total_spending'])
sp = sp[(sp['year'] >= 2007) & (sp['year'] <= 2020)].groupby(['product_module_code', 'year'], as_index=False)['total_spending'].sum()
d = sp.merge(pt[['product_module_code', 'prep_time']], on='product_module_code').dropna(subset=['prep_time'])
lowest = d['prep_time'].min()
g = d.groupby('year').apply(lambda x: pd.Series({
    'mean_prep': np.average(x['prep_time'], weights=x['total_spending']),
    'share_lowest': x.loc[x['prep_time'] == lowest, 'total_spending'].sum() / x['total_spending'].sum(),
    'modules': x['product_module_code'].nunique()}))
print(g.round(3).to_string())
fig, axes = plt.subplots(1, 2, figsize=(15, 5.5))
ax = axes[0]
ax.plot(g.index, g['mean_prep'], color=BLUE, lw=2.5, marker='o', ms=5)
ax.set_ylabel('Mean preparation score (1 = open and eat, 9 = long cooking)'); ax.set_xlabel('Year')
ax.set_ylim(1.5, 2.2)
ax.set_title('Preparation score of groceries bought, spending-weighted')
ax = axes[1]
ax.plot(g.index, 100 * g['share_lowest'], color=RED, lw=2.5, marker='o', ms=5)
ax.set_ylabel('Percent of spending'); ax.set_xlabel('Year')
ax.set_ylim(50, 70)
ax.set_title('Share of spending on modules scored 1 (open and eat)')
fig.savefig(FIG / 'prep_time_trend.png', bbox_inches='tight', dpi=150)
plt.close(fig)
print(f'saved {FIG / "prep_time_trend.png"}')
