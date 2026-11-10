"""Price of food by module healthiness, in money and with preparation time valued (Anya 2026-11-10, 0_main.tex first section).
The NOVA rows added the same day were deleted at Anya's request ("delete the nova stuff").

Reads interim/what_moves/module_time_price.parquet (build_module_time_price.py) and, for the variety-adjusted change,
interim/rms_variety/price_index_module_year.parquet (build/variety/build_price_index.py, RMS). Groups: healthiness bins of the
module (claude_hi 0-2, 3, 4-5, 6-10). A group's price = 1,000 x real spending on purchases with a size / their grams (per kg) or
their calories (per 1,000 kcal); its minutes per 1,000 kcal = the minutes needed to prepare all the calories the group sold,
per 1,000 kcal.
Variety-adjusted change (Anya chose option (a) on 2026-11-10): real price level in 2020 relative to 2007, continuing
products (level_ces) and with the Feenstra correction for products entering and leaving (level_va, sigma = 5),
spending-weighted with 2020 RMS spending over modules passing the 1% coverage gate of
analyze/variety/analyze_variety_price_index.py, deflated with the CPI-U of that script.
Outputs:
  figs/what_moves/prices/price_by_healthiness.png       per kg, per 1,000 kcal, variety-adjusted change 2007-2020
  figs/what_moves/prices/full_price_by_healthiness.png  minutes of preparation per 1,000 kcal; price per 1,000 kcal
      plus the minutes valued at WAGES dollars an hour
Prints the group tables, the wage at which two groups cost the same, and the same with half and twice the minutes.
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
HI_BINS = ['0-2', '3', '4-5', '6-10']
HI_TICKS = ['0-2\nleast\nhealthy', '3', '4-5', '6-10\nmost\nhealthy']
HI_X = 'Healthiness score of the product module'
WAGES = [(0, GREY, 'money only'), (7.25, ORANGE, '$7.25 an hour'), (15, RED, '$15 an hour'), (30, BLUE, '$30 an hour')]
BASE_YEAR, END_YEAR = 2007, 2020
CPI = {2007: 207.3, 2020: 258.8}          # CPI-U annual averages, as in analyze/variety/analyze_variety_price_index.py


def hi_groups(m, time_scale=1.0):
    m = m.assign(minutes=time_scale * m['min_1000kcal'] * m['cals'] / 1000)
    g = m.groupby('hi_bin').agg(n=('module', 'size'), spend=('spend', 'sum'), spend_g=('spend_g', 'sum'), grams=('grams', 'sum'),
                                cals=('cals', 'sum'), minutes=('minutes', 'sum')).reindex(HI_BINS)
    g['usd_kg'] = 1000 * g['spend_g'] / g['grams']
    g['usd_1000kcal'] = 1000 * g['spend_g'] / g['cals']
    g['min_1000kcal'] = 1000 * g['minutes'] / g['cals']
    g['spend_share'] = g['spend'] / g['spend'].sum()
    g['cal_share'] = g['cals'] / g['cals'].sum()
    return g


def variety_change(m):
    """Real % change of the price level 2007-2020 by healthiness bin."""
    pi = pd.read_parquet(idir / 'rms_variety' / 'price_index_module_year.parquet')
    cov = pd.read_parquet(idir / 'rms_variety' / 'module_healthiness_usda.parquet', columns=['product_module_code', 'pct_coverage'])
    pi = pi[pi['year'] == END_YEAR].merge(cov, on='product_module_code')
    pi = pi[pi['pct_coverage'] >= 0.01].merge(m[['product_module_code', 'hi_bin']], on='product_module_code', how='inner')
    defl = CPI[END_YEAR] / CPI[BASE_YEAR]
    rows = []
    for k in HI_BINS:
        g = pi[pi['hi_bin'] == k]
        rows.append({'group': k, 'n': len(g),
                     'ces': 100 * (np.average(g['level_ces'], weights=g['total_spending']) / defl - 1),
                     'va': 100 * (np.average(g['level_va'], weights=g['total_spending']) / defl - 1)})
    return pd.DataFrame(rows).set_index('group')


def bars(ax, v, fmt, title, ylabel, ticks, xlabel):
    x = np.arange(len(v))
    ax.bar(x, v, color=BLUE, width=0.6)
    for xi, vi in zip(x, v):
        ax.text(xi, vi, fmt.format(vi), ha='center', va='bottom', fontsize=22)
    ax.set_xticks(x); ax.set_xticklabels(ticks)
    ax.set_title(title, pad=14); ax.set_ylabel(ylabel); ax.set_xlabel(xlabel)
    ax.set_ylim(0, 1.15 * max(v))


def change_bars(ax, ch, ticks, xlabel):
    x = np.arange(len(ch))
    for off, col, color, lab in [(-0.2, 'ces', GREY, 'continuing products'), (0.2, 'va', BLUE, 'variety-adjusted')]:
        ax.bar(x + off, ch[col].values, width=0.38, color=color, label=lab)
        for xi, vi in zip(x + off, ch[col].values):
            ax.text(xi, vi + (0.6 if vi >= 0 else -0.6), f'{vi:+.0f}%', ha='center', va='bottom' if vi >= 0 else 'top', fontsize=20)
    ax.axhline(0, color='black', lw=1)
    lo, hi = min(0, ch[['ces', 'va']].values.min()), max(0, ch[['ces', 'va']].values.max())
    ax.set_ylim(lo - 0.25 * (hi - lo) - 1, hi + 0.25 * (hi - lo) + 1)
    ax.set_xticks(x); ax.set_xticklabels(ticks)
    ax.set_title(f'Real price change, {BASE_YEAR} to {END_YEAR}', pad=14); ax.set_ylabel('Percent'); ax.set_xlabel(xlabel)
    ax.legend(frameon=False, fontsize=20, loc='best')


def fig_money(hi, ch):
    fig, axes = plt.subplots(1, 3, figsize=(36, 11))
    fig.subplots_adjust(wspace=0.3)
    bars(axes[0], hi['usd_kg'].values, '${:.2f}', 'Per kilogram', 'Real price (2013 dollars)', HI_TICKS, HI_X)
    bars(axes[1], hi['usd_1000kcal'].values, '${:.2f}', 'Per 1,000 calories', 'Real price (2013 dollars)', HI_TICKS, HI_X)
    change_bars(axes[2], ch, HI_TICKS, HI_X)
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / 'price_by_healthiness.png', bbox_inches='tight', dpi=100); plt.close(fig)
    print(f'saved {FIG / "price_by_healthiness.png"}')


def end_labels(ax, x, ends, ha, gap):
    """Dollar labels at one end of the lines, pushed apart so they do not overlap."""
    ends = sorted(ends)
    ys = [e[0] for e in ends]
    for i in range(1, len(ys)):
        ys[i] = max(ys[i], ys[i - 1] + gap)
    for (y, col), yl in zip(ends, ys):
        ax.text(x, yl, f'${y:.2f}', va='center', ha=ha, fontsize=21, color=col)


def full_panel(ax, g, ticks, xl, legend_loc):
    x = np.arange(len(g))
    left, right = [], []
    for w, col, lab in WAGES:
        y = g['usd_1000kcal'].values + w / 60 * g['min_1000kcal'].values
        ax.plot(x, y, color=col, lw=4, marker='o', ms=12, label=lab)
        left.append((y[0], col)); right.append((y[-1], col))
    top = max(max(e[0] for e in left), max(e[0] for e in right))
    end_labels(ax, x[0] - 0.12, left, 'right', 0.06 * top)
    end_labels(ax, x[-1] + 0.12, right, 'left', 0.06 * top)
    ax.set_xticks(x); ax.set_xticklabels(ticks); ax.set_xlim(-0.75, len(g) - 0.25)
    ax.set_ylim(0, 1.08 * top)
    ax.set_title('Price per 1,000 calories, time valued at', pad=14)
    ax.set_ylabel('2013 dollars'); ax.set_xlabel(xl)
    ax.legend(frameon=False, loc=legend_loc, fontsize=22)


def fig_full(hi):
    fig, axes = plt.subplots(1, 2, figsize=(26, 11))
    fig.subplots_adjust(wspace=0.3)
    bars(axes[0], hi['min_1000kcal'].values, '{:.1f}', 'Preparation time per 1,000 calories', 'Minutes', HI_TICKS, HI_X)
    full_panel(axes[1], hi, HI_TICKS, HI_X, 'upper center')
    fig.savefig(FIG / 'full_price_by_healthiness.png', bbox_inches='tight', dpi=100); plt.close(fig)
    print(f'saved {FIG / "full_price_by_healthiness.png"}')


def break_even(g, a, b):
    """Hourly wage at which groups a and b cost the same per 1,000 kcal (money + time); nan if they never cross at w >= 0."""
    dp = g.loc[b, 'usd_1000kcal'] - g.loc[a, 'usd_1000kcal']
    dt = g.loc[a, 'min_1000kcal'] - g.loc[b, 'min_1000kcal']
    w = 60 * dp / dt if dt != 0 else np.nan
    return w if w >= 0 else np.nan


def main():
    m = pd.read_parquet(idir / 'what_moves' / 'module_time_price.parquet')
    hi = hi_groups(m)
    print(hi[['n', 'spend_share', 'cal_share', 'usd_kg', 'usd_1000kcal', 'min_1000kcal']].round(3).to_string())
    for scale in (0.5, 1.0, 2.0):
        g = hi_groups(m, scale)
        full = {w: (g['usd_1000kcal'] + w / 60 * g['min_1000kcal']).round(2).tolist() for w, _, _ in WAGES}
        print(f'minutes x {scale}: full price by group {full}; wage at which 6-10 costs the same as 0-2: {break_even(g, "6-10", "0-2"):.2f}')
    ch = variety_change(m)
    print(ch.round(1).to_string())
    fig_money(hi, ch)
    fig_full(hi)


if __name__ == '__main__':
    main()
