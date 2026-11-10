"""Price of food by module healthiness, in money and with preparation time valued (Anya 2026-11-10, 0_main.tex first section).

Reads interim/what_moves/module_time_price.parquet (build_module_time_price.py). Bins: claude_hi 0-2, 3, 4-5, 6-10.
A bin's price = 1,000 x real spending on rows with a size / their grams (per kg) or their calories (per 1,000 kcal);
its minutes per 1,000 kcal = the calorie-weighted mean of the modules' minutes per 1,000 kcal, that is the minutes
needed to prepare all the calories the bin sold, per 1,000 kcal.
Outputs:
  figs/what_moves/prices/price_by_healthiness.png       real price per kg and per 1,000 kcal by bin
  figs/what_moves/prices/full_price_by_healthiness.png  minutes of preparation per 1,000 kcal by bin; price per
      1,000 kcal plus the minutes valued at WAGES dollars an hour
Prints the bin table, the wage at which two bins cost the same, and the same with half and twice the minutes.
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

FIG = fdir / 'what_moves' / 'prices'
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#999999'
plt.rcParams.update({'font.size': 24, 'axes.labelsize': 24, 'axes.titlesize': 26, 'xtick.labelsize': 22, 'ytick.labelsize': 22,
                     'axes.spines.top': False, 'axes.spines.right': False})
BINS = ['0-2', '3', '4-5', '6-10']
TICKS = ['0-2\nleast\nhealthy', '3', '4-5', '6-10\nmost\nhealthy']
WAGES = [(0, GREY, 'money only'), (7.25, ORANGE, '$7.25 an hour'), (15, RED, '$15 an hour'), (30, BLUE, '$30 an hour')]


def bins(m, time_scale=1.0):
    m = m.assign(minutes=time_scale * m['min_1000kcal'] * m['cals'] / 1000)
    g = m.groupby('hi_bin').agg(n=('module', 'size'), spend=('spend', 'sum'), spend_g=('spend_g', 'sum'), grams=('grams', 'sum'),
                                cals=('cals', 'sum'), minutes=('minutes', 'sum')).reindex(BINS)
    g['usd_kg'] = 1000 * g['spend_g'] / g['grams']
    g['usd_1000kcal'] = 1000 * g['spend_g'] / g['cals']
    g['min_1000kcal'] = 1000 * g['minutes'] / g['cals']
    g['spend_share'] = g['spend'] / g['spend'].sum()
    g['cal_share'] = g['cals'] / g['cals'].sum()
    return g


def bars(ax, x, v, fmt, title, ylabel):
    ax.bar(x, v, color=BLUE, width=0.6)
    for xi, vi in zip(x, v):
        ax.text(xi, vi, fmt.format(vi), ha='center', va='bottom', fontsize=22)
    ax.set_xticks(x); ax.set_xticklabels(TICKS)
    ax.set_title(title, pad=14); ax.set_ylabel(ylabel)
    ax.set_xlabel('Healthiness score of the product module')
    ax.set_ylim(0, 1.15 * max(v))


def fig_money(g):
    x = np.arange(len(BINS))
    fig, axes = plt.subplots(1, 2, figsize=(26, 10))
    fig.subplots_adjust(wspace=0.28)
    bars(axes[0], x, g['usd_kg'].values, '${:.2f}', 'Per kilogram', 'Real price (2013 dollars)')
    bars(axes[1], x, g['usd_1000kcal'].values, '${:.2f}', 'Per 1,000 calories', 'Real price (2013 dollars)')
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / 'price_by_healthiness.png', bbox_inches='tight', dpi=110); plt.close(fig)
    print(f'saved {FIG / "price_by_healthiness.png"}')


def fig_full(g):
    x = np.arange(len(BINS))
    fig, axes = plt.subplots(1, 2, figsize=(26, 10))
    fig.subplots_adjust(wspace=0.28)
    bars(axes[0], x, g['min_1000kcal'].values, '{:.1f}', 'Preparation time per 1,000 calories', 'Minutes')
    ax = axes[1]
    for w, col, lab in WAGES:
        y = g['usd_1000kcal'].values + w / 60 * g['min_1000kcal'].values
        ax.plot(x, y, color=col, lw=4, marker='o', ms=12, label=lab)
        ax.text(x[-1] + 0.12, y[-1], f'${y[-1]:.2f}', va='center', fontsize=21, color=col)
    ax.set_xticks(x); ax.set_xticklabels(TICKS); ax.set_xlim(-0.3, len(BINS) - 0.3)
    ax.set_ylim(0, None)
    ax.set_title('Price per 1,000 calories, time valued at', pad=14)
    ax.set_ylabel('2013 dollars'); ax.set_xlabel('Healthiness score of the product module')
    ax.legend(frameon=False, loc='upper left', fontsize=22)
    fig.savefig(FIG / 'full_price_by_healthiness.png', bbox_inches='tight', dpi=110); plt.close(fig)
    print(f'saved {FIG / "full_price_by_healthiness.png"}')


def break_even(g, a, b):
    """Hourly wage at which bins a and b cost the same per 1,000 kcal (money + time); nan if they never cross at w >= 0."""
    dp = g.loc[b, 'usd_1000kcal'] - g.loc[a, 'usd_1000kcal']
    dt = g.loc[a, 'min_1000kcal'] - g.loc[b, 'min_1000kcal']
    w = 60 * dp / dt if dt != 0 else np.nan
    return w if w >= 0 else np.nan


def main():
    m = pd.read_parquet(idir / 'what_moves' / 'module_time_price.parquet')
    g = bins(m)
    print(g[['n', 'spend_share', 'cal_share', 'usd_kg', 'usd_1000kcal', 'min_1000kcal']].round(3).to_string())
    for scale in (0.5, 1.0, 2.0):
        gs = bins(m, scale)
        full = {w: (gs['usd_1000kcal'] + w / 60 * gs['min_1000kcal']).round(2).tolist() for w, _, _ in WAGES}
        print(f'minutes x {scale}: full price by bin {full}; wage at which 6-10 costs the same as 0-2: '
              f'{break_even(gs, "6-10", "0-2"):.2f}; 3 vs 0-2: {break_even(gs, "3", "0-2"):.2f}')
    fig_money(g)
    fig_full(g)


if __name__ == '__main__':
    main()
