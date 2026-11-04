"""
figure_price_per_calorie_upf.py -- real price per calorie of ultra-processed against other
groceries over time, for the Prices section of notes/obesity.tex (the price-per-calorie
counterpart of figs/variety/price/variety_price_level_by_upf.png, asked for 2026-11-04).

Data: interim/health/nova_calorie_panel.parquet (build/health/build_nova_calorie_panel.py):
household-year spending and calories of purchase rows whose UPC matches a USDA label with an
ingredient list, by NOVA group (spend_nova1..4, cals_nova1..4). "Ultra-processed" = NOVA group 4
of clean/usda/build_usda_nova.py; "other" = NOVA groups 1-3. Sample: figure_nutrition_over_time.load()
(household-years 2004-2020 with a complete roster, income and labelled calories per adult
equivalent inside the 1st-99th percentiles), projection-weighted.
Price per 1,000 kcal in year t = (projection-weighted sum of spending on the group's labelled
rows, deflated to 2013 dollars with the CPI-U) / (projection-weighted sum of their calories) x 1,000.
Unlike the retail-scanner price index, which chains unit-value changes of continuing products,
this is a unit value and moves with the mix of products bought as well as with their prices.
Left: real price per 1,000 kcal by group. Right: the same series indexed to 2004 = 1, each line
labelled with its percent change from 2004 to 2020.
Output: figs/health/prices/price_per_calorie_upf.png
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
from matplotlib.ticker import MaxNLocator
from figure_nutrition_over_time import load

FIG = fdir / 'health' / 'prices'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, GREY = '#2c5f8a', '#a63d40', '#7a7a7a'
CPI = {2004: 188.9, 2005: 195.3, 2006: 201.6, 2007: 207.3, 2008: 215.3, 2009: 214.5, 2010: 218.1, 2011: 224.9,
       2012: 229.6, 2013: 233.0, 2014: 236.7, 2015: 237.0, 2016: 240.0, 2017: 245.1, 2018: 251.1, 2019: 255.7, 2020: 258.8}
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 11,
                     'axes.spines.top': False, 'axes.spines.right': False})


def main():
    df = load()
    w = df['projection_factor']
    defl = df['panel_year'].map(lambda y: CPI[2013] / CPI[y])
    d = pd.DataFrame({'year': df['panel_year'],
                      'sp_upf': df['spend_nova4'] * w * defl, 'kc_upf': df['cals_nova4'] * w,
                      'sp_oth': (df['spend_nova1'] + df['spend_nova2'] + df['spend_nova3']) * w * defl,
                      'kc_oth': (df['cals_nova1'] + df['cals_nova2'] + df['cals_nova3']) * w})
    g = d.groupby('year').sum()
    out = pd.DataFrame({'Ultra-processed (NOVA 4)': 1000 * g['sp_upf'] / g['kc_upf'],
                        'Other groceries (NOVA 1-3)': 1000 * g['sp_oth'] / g['kc_oth'],
                        'All labelled groceries': 1000 * (g['sp_upf'] + g['sp_oth']) / (g['kc_upf'] + g['kc_oth'])})
    idx = out / out.iloc[0]
    print('real $ (2013) per 1,000 kcal by group and year:'); print(out.round(3).to_string())
    print('\nindexed to 2004 = 1:'); print(idx.round(3).to_string())
    cols = {'Ultra-processed (NOVA 4)': RED, 'Other groceries (NOVA 1-3)': BLUE, 'All labelled groceries': GREY}
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    ax = axes[0]
    for c, col in cols.items():
        ax.plot(out.index, out[c], color=col, lw=2.5 if col != GREY else 1.5, ls='-' if col != GREY else '--', marker='o', ms=4, label=c)
    ax.set_ylabel('Real price per 1,000 kcal (2013\\$)'); ax.set_xlabel('Year'); ax.set_ylim(0, out.max().max() * 1.2)
    ax.set_title('Price per calorie of grocery purchases with a USDA label'); ax.legend(frameon=False, loc='lower right')
    ax = axes[1]
    for c, col in cols.items():
        ax.plot(idx.index, idx[c], color=col, lw=2.5 if col != GREY else 1.5, ls='-' if col != GREY else '--', marker='o', ms=4, label=c)
        ax.annotate(f'{100 * (idx[c].iloc[-1] - 1):+.0f}%', (idx.index[-1], idx[c].iloc[-1]), xytext=(6, 0), textcoords='offset points',
                    color=col, fontsize=12, va='center')
    ax.axhline(1, color=GREY, lw=0.8)
    ax.set_ylabel('Real price per 1,000 kcal, 2004 = 1'); ax.set_xlabel('Year')
    ax.set_xlim(2003.5, 2022); ax.set_title('Indexed to 2004 (label: percent change 2004-2020)'); ax.legend(frameon=False, loc='upper left')
    for ax in axes:
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fig.savefig(FIG / 'price_per_calorie_upf.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "price_per_calorie_upf.png"}')


if __name__ == '__main__':
    main()
