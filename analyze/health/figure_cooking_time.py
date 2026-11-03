"""
figure_cooking_time.py -- time spent cooking, from the American Time Use Survey (ATUS), for the
Time section of notes/obesity.tex.

cooking_time_trend.png (always): from the published BLS Table A-1 (interim/atus/table_a1.parquet,
  clean/atus/clean_atus_tables.py): civilian population 15+, 2003-2024 (no 2020). Left: average
  minutes per day in food preparation and cleanup, total, men and women; dashed grey: eating and
  drinking (excluding travel). Right: percent of people who did any food preparation or cleanup
  on the diary day, and the average minutes of those who did.
cooking_time_by_group.png (when interim/atus/atus_respondents.parquet exists, clean/atus/
  clean_atus.py on the BLS microdata): adults 18+, weighted mean minutes per day in food
  preparation and cleanup by within-year family-income quartile (CPS family income bracket,
  respondents ranked within year) and by education (12 years or less / 13-15 / 16+), three-year
  bins of survey years. Also printed: the same by employment status.
Outputs: figs/health/time/
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

FIG = fdir / 'health' / 'time'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#7a7a7a'
Q_COL = {1: RED, 2: ORANGE, 3: '#999999', 4: BLUE}
Q_LAB = {1: 'Bottom income quartile', 2: 'Second quartile', 3: 'Third quartile', 4: 'Top income quartile'}
EDU_COL = {'12 years or less': RED, '13-15 years': ORANGE, '16+ years': BLUE}
plt.rcParams.update({'font.size': 12, 'axes.labelsize': 13, 'axes.titlesize': 12.5, 'legend.fontsize': 10,
                     'axes.spines.top': False, 'axes.spines.right': False})


def wmean(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok]) if ok.any() else np.nan


def trend():
    t = pd.read_parquet(idir / 'atus' / 'table_a1.parquet')
    fp = t[t['activity'] == 'food_prep_cleanup'].set_index('year').sort_index()
    eat = t[t['activity'] == 'eating_drinking'].set_index('year').sort_index()
    print('food preparation and cleanup, minutes per day (total / men / women) and percent engaged:')
    print((fp[['hours_total', 'hours_men', 'hours_women']] * 60).round(1).join(fp[['pct_total', 'pct_men', 'pct_women']]).to_string())
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    ax = axes[0]
    ax.plot(fp.index, 60 * fp['hours_total'], color='black', lw=2.5, marker='o', ms=5, label='Food preparation and cleanup, all')
    ax.plot(fp.index, 60 * fp['hours_women'], color=RED, lw=2, marker='o', ms=4, label='Women')
    ax.plot(fp.index, 60 * fp['hours_men'], color=BLUE, lw=2, marker='o', ms=4, label='Men')
    ax.plot(eat.index, 60 * eat['hours_total'], color=GREY, lw=1.8, ls='--', marker='s', ms=3, label='Eating and drinking, all (excl. travel)')
    ax.set_ylabel('Minutes per day, civilian population 15+'); ax.set_xlabel('Survey year'); ax.set_ylim(0, 90)
    ax.set_title('Time spent cooking and cleaning up, ATUS'); ax.legend(frameon=False, loc='lower right')
    ax = axes[1]
    ax.plot(fp.index, fp['pct_total'], color='black', lw=2.5, marker='o', ms=5, label='Percent who cooked or cleaned up on the diary day, all')
    ax.plot(fp.index, fp['pct_women'], color=RED, lw=2, marker='o', ms=4, label='Women')
    ax.plot(fp.index, fp['pct_men'], color=BLUE, lw=2, marker='o', ms=4, label='Men')
    ax.set_ylabel('Percent of people'); ax.set_xlabel('Survey year'); ax.set_ylim(0, 100)
    ax2 = ax.twinx()
    ax2.plot(fp.index, 60 * fp['hours_part_total'], color=GREY, lw=1.8, ls='--', marker='s', ms=3, label='Minutes per day among those who did, all (right axis)')
    ax2.set_ylabel('Minutes per day among participants', color=GREY); ax2.tick_params(axis='y', colors=GREY); ax2.spines['top'].set_visible(False); ax2.set_ylim(0, 100)
    h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, frameon=False, loc='lower right', fontsize=9)
    ax.set_title('Participation on the diary day, and time among participants')
    fig.savefig(FIG / 'cooking_time_trend.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "cooking_time_trend.png"}')


def by_group():
    p = idir / 'atus' / 'atus_respondents.parquet'
    if not p.exists():
        print(f'{p} not found: the ATUS microdata (atussum, atuscps zips in raw/atus/) have not been cleaned; skipping cooking_time_by_group.png')
        return
    d = pd.read_parquet(p)
    d = d[d['weight'] > 0].copy()
    d['bin'] = (d['year'] // 3) * 3
    d['bin_label'] = d['bin'].astype(str) + '-' + (d['bin'] + 2).astype(str)
    inc = d.dropna(subset=['income_bracket']).copy()
    inc['inc_q'] = inc.groupby('year')['income_bracket'].transform(lambda s: pd.qcut(s.rank(method='first'), 4, labels=[1, 2, 3, 4])).astype(int)
    byq = inc.groupby(['bin', 'inc_q']).apply(lambda g: wmean(g['cook'], g['weight'])).unstack()
    bye = d.dropna(subset=['educ3']).groupby(['bin', 'educ3'], observed=True).apply(lambda g: wmean(g['cook'], g['weight'])).unstack()
    print('minutes per day cooking by income quartile (3-year bins):'); print(byq.round(1).to_string())
    print('by education:'); print(bye.round(1).to_string())
    print('by employment:'); print(d.groupby(['bin', 'employed']).apply(lambda g: wmean(g['cook'], g['weight'])).unstack().round(1).to_string())
    fig, axes = plt.subplots(1, 2, figsize=(16, 6), sharey=True)
    ax = axes[0]
    for q in (1, 2, 3, 4):
        ax.plot(byq.index + 1, byq[q], color=Q_COL[q], lw=2.5 if q in (1, 4) else 1.8, marker='o', ms=5, label=Q_LAB[q])
    ax.set_ylabel('Minutes per day, adults 18+'); ax.set_xlabel('Survey years (3-year bins, plotted at the middle year)')
    ax.set_title('Food preparation and cleanup by family income quartile'); ax.legend(frameon=False)
    ax = axes[1]
    for e, c in EDU_COL.items():
        if e in bye.columns:
            ax.plot(bye.index + 1, bye[e], color=c, lw=2.2, marker='o', ms=5, label=e)
    ax.set_xlabel('Survey years (3-year bins, plotted at the middle year)')
    ax.set_title('Food preparation and cleanup by education'); ax.legend(frameon=False)
    fig.savefig(FIG / 'cooking_time_by_group.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "cooking_time_by_group.png"}')


if __name__ == '__main__':
    trend()
    by_group()
