"""Exhibits for build_policy_shocks.do: the 2009 WIC food package and COVID (2026-11-05).

Reads interim/what_moves/policy_shock_coefs.csv.
Outputs: figs/what_moves/prices/wic_package.png, figs/what_moves/time/covid_white_collar.png, tabs/what_moves/policy_shocks.tex
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.hei_units import to_sd, hei_sd, hei_mean_sd

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BLUE, GREY = '#2c5f8a', '#7a7a7a'
plt.rcParams.update({'font.size': 14, 'axes.labelsize': 15, 'axes.titlesize': 13, 'xtick.labelsize': 12, 'ytick.labelsize': 12,
                     'axes.spines.top': False, 'axes.spines.right': False})
NAMES = {'hei_usda': 'HEI (SD)', 'produce_share': 'Produce share of calories (pct. points)', 'produce_spend_share': 'Produce share of spending (pct. points)',
         'whole_share': 'Whole-grain share of bread (pct. points)', 'sugar_1000': 'Sugar, g per 1,000 kcal', 'upf_share': 'Ultra-processed share of calories (pct. points)',
         'cals_ae_day': 'Calories per adult equivalent per day', 'real_income': 'Household income ($10k)', 'sodium_1000': 'Sodium, mg per 1,000 kcal'}


def grid(d, design, outs, base, line, title, path):
    fig, axes = plt.subplots(2, 3, figsize=(20, 10.5))
    fig.subplots_adjust(hspace=0.35, wspace=0.25)
    for ax, o in zip(axes.flat, outs):
        x = d[(d.design == design) & (d.outcome == o) & (d.term != 'post')].copy()
        x['y'] = x.term.astype(int)
        x = x.sort_values('y')
        ax.plot(x.y, x.b, color=BLUE, lw=2.5, marker='o', ms=6)
        ax.fill_between(x.y, x.b - 1.96 * x.se, x.b + 1.96 * x.se, color=BLUE, alpha=0.15, lw=0)
        ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axvline(line, color=GREY, lw=1, ls=':')
        ax.set_title(NAMES[o])
        ax.set_xticks(x.y)
    n = d[d.design == design].iloc[0]
    fig.suptitle(f'{title}: {int(n.n_treated):,} households against {int(n.n_control):,}; {base} = 0', fontsize=15)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, bbox_inches='tight', dpi=150)
    print(f'saved {path}')


def main():
    d = to_sd(pd.read_csv(idir / 'what_moves' / 'policy_shock_coefs.csv'))     # HEI in standard deviations, the paper's unit
    grid(d, 'wic', ['hei_usda', 'produce_share', 'produce_spend_share', 'whole_share', 'sugar_1000', 'upf_share'], '2008', 2009.5,
         'Households on WIC before the October 2009 food-package change vs low-income households with young children', fdir / 'what_moves' / 'prices' / 'wic_package.png')
    grid(d, 'covid_white', ['hei_usda', 'upf_share', 'cals_ae_day', 'produce_share', 'sugar_1000', 'real_income'], '2019', 2019.5,
         'White-collar households in 2019 vs blue-collar and service households, through COVID', fdir / 'what_moves' / 'time' / 'covid_white_collar.png')
    print(d[d.term.isin(['post', '2020'])].round(3).to_string(index=False))


if __name__ == '__main__':
    main()
