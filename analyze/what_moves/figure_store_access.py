"""What stores offer and what they charge, by the income of the ZIP codes their shoppers live in (Anya 2026-11-10): access, not
consumption. Figure 1 reproduces Allcott, Diamond and Dube (2019, Figure II); figures 2 and 3 add prices in the same layout.

Reads interim/what_moves/store_choice_sets.parquet (build_store_choice_sets_rms.py on Oscar: one row per RMS store and year,
measures over the food UPCs the store sold, each UPC counted once and weighted by the calories in one package) and
store_shopper_income.parquet (build_store_shopper_income.py: trip-weighted ACS 2011-2015 median household income of the panel
shoppers' home ZIP codes). Stores with MIN_TRIPS or more panel trips from MIN_HH or more households. Points: means across
store-years 2006-2020 in the bins of their figure (PrepZipCodeData.do: $10,000 bins, under $30,000 pooled and plotted at 25,
$80,000 or more pooled and plotted at 100).
Outputs (figs/what_moves/prices/):
  store_access_by_income.png        added sugar per 1,000 kcal, whole-grain share of bread, fruit and vegetable share, Health Index
  store_shelf_prices_by_income.png  real price per 1,000 kcal of the UPCs offered (each once; stocking and prices both enter), for
                                    the healthy and unhealthy side of each panel: added sugar at most / above 25 g per 1,000 kcal,
                                    whole-grain / other bread, fruit and vegetables / other food, Health Index above / at or below 0
                                    (fruit and vegetables included, all above 0, since 2026-11-12)
  store_same_prices_by_income.png   the same sides, price of the same UPCs relative to their national price (%)
  tabs/what_moves/store_prices_by_income.tex  stores in the top vs bottom quartile of shopper ZIP income (quartiles of stores), in
                                    the layout of local_prices_by_income.tex: healthy (claude_hi 4-10) and unhealthy (0-3) shelf
                                    prices and their ratio, same-product prices, and the four Figure II measures
Prints the bin means and the top-minus-bottom differences.
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
TAB = tdir / 'what_moves' / 'store_prices_by_income.tex'
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#999999'
plt.rcParams.update({'font.size': 24, 'axes.labelsize': 24, 'axes.titlesize': 26, 'xtick.labelsize': 21, 'ytick.labelsize': 22,
                     'axes.spines.top': False, 'axes.spines.right': False})
MIN_TRIPS, MIN_HH = 50, 10
EDGES = [0, 30, 40, 50, 60, 70, 80, np.inf]                # $000s of shopper ZIP median household income (2015 dollars)
MIDS = [25, 35, 45, 55, 65, 75, 100]                        # Allcott et al.'s ZipMedIncomeGroup values
TICKS = ['<30', '30-40', '40-50', '50-60', '60-70', '70-80', '80+']
XLAB = "Shoppers' ZIP median income ($000s)"
CPI = {2006: 201.6, 2007: 207.3, 2008: 215.3, 2009: 214.5, 2010: 218.1, 2011: 224.9, 2012: 229.6, 2013: 233.0, 2014: 236.7,
       2015: 237.0, 2016: 240.0, 2017: 245.1, 2018: 251.1, 2019: 255.7, 2020: 258.8}       # CPI-U, as in analyze_variety_price_index.py
ACCESS = [('addsugar_1000', 'Added sugar per 1,000 calories (g)', 1), ('whole_share', 'Whole-grain share of bread (%)', 100),
          ('produce_share', 'Fruit and vegetable share of calories (%)', 100), ('hi_1000', 'Health Index per 1,000 calories', 1)]
PANELS = [('Added sugar', 'sug_lo', '25 g per 1,000 kcal or less', 'sug_hi', 'more than 25 g'),
          ('Bread', 'whole', 'whole grain', 'refined', 'other bread'),
          ('Fruit and vegetables', 'prod', 'fruit and vegetables', 'nonprod', 'other food'),
          ('Health Index', 'hipos', 'above 0', 'hineg', '0 or below')]


def load():
    s = pd.read_parquet(idir / 'what_moves' / 'store_choice_sets.parquet')
    inc = pd.read_parquet(idir / 'what_moves' / 'store_shopper_income.parquet')
    inc = inc[(inc['n_trips'] >= MIN_TRIPS) & (inc['n_households'] >= MIN_HH) & inc['zip_income'].notna()]
    d = s.merge(inc[['store_code_uc', 'zip_income']], on='store_code_uc', how='inner')
    d['bin'] = pd.cut(d['zip_income'] / 1000, EDGES, labels=MIDS, right=False).astype(int)
    to_2013 = CPI[2013] / d['year'].map(CPI)
    for c in [c for c in d.columns if c.startswith('shelf_')]:
        d[c] = d[c] * to_2013                                      # real 2013 dollars per 1,000 kcal
    for c in [c for c in d.columns if c.startswith('rel_')]:
        d[c] = 100 * (np.exp(d[c]) - 1)                             # percent above the national price of the same UPCs
    st = d.drop_duplicates('store_code_uc')
    cuts = st['zip_income'].quantile([0.25, 0.75]).values
    d['quartile'] = np.select([d['zip_income'] <= cuts[0], d['zip_income'] > cuts[1]], ['bottom', 'top'], default='middle')
    print(f'{len(d):,} store-years, {d.store_code_uc.nunique():,} stores of {s.store_code_uc.nunique():,} RMS stores; '
          f'store quartile cuts of shopper ZIP income ${cuts[0]:,.0f} and ${cuts[1]:,.0f}')
    return d


def axis(ax, title, ylabel):
    ax.set_title(title, pad=14); ax.set_ylabel(ylabel); ax.set_xlabel(XLAB)
    ax.set_xticks(MIDS); ax.set_xticklabels(TICKS, rotation=30)


def fig_access(m):
    fig, axes = plt.subplots(2, 2, figsize=(26, 20))
    fig.subplots_adjust(wspace=0.28, hspace=0.42)
    for ax, (c, lab, k) in zip(axes.flat, ACCESS):
        ax.plot(MIDS, k * m[c].values, color=BLUE, lw=4, marker='o', ms=12)
        axis(ax, lab, '')
    fig.savefig(FIG / 'store_access_by_income.png', bbox_inches='tight', dpi=100); plt.close(fig)
    print(f'saved {FIG / "store_access_by_income.png"}')


def fig_prices(m, prefix, fname, ylabel):
    fig, axes = plt.subplots(2, 2, figsize=(26, 20))
    fig.subplots_adjust(wspace=0.28, hspace=0.42)
    for ax, (title, a, alab, b, blab) in zip(axes.flat, PANELS):
        ax.plot(MIDS, m[f'{prefix}{a}'].values, color=BLUE, lw=4, marker='o', ms=12, label=alab)
        ax.plot(MIDS, m[f'{prefix}{b}'].values, color=RED, lw=4, marker='s', ms=12, label=blab)
        if prefix == 'rel_':
            ax.axhline(0, color=GREY, lw=1.2, ls='--')
        axis(ax, title, ylabel)
        ax.legend(frameon=False, fontsize=21, loc='best')
    fig.savefig(FIG / fname, bbox_inches='tight', dpi=100); plt.close(fig)
    print(f'saved {FIG / fname}')


def table(d):
    t = d[d['quartile'].isin(['top', 'bottom'])]
    q = t.groupby('quartile').mean(numeric_only=True)
    n = t.groupby('quartile')['store_code_uc'].nunique()
    rows = [('top', 'Top quartile of shopper ZIP income'), ('bottom', 'Bottom quartile')]
    lines = ['\\begin{tabular}{lccccc}', '\\toprule',
             ' & \\multicolumn{3}{c}{Price per 1,000 calories of the products offered} & \\multicolumn{2}{c}{Same products, \\% above national price} \\\\',
             '\\cmidrule(lr){2-4} \\cmidrule(lr){5-6}',
             ' & Healthy & Unhealthy & Ratio & Healthy & Unhealthy \\\\', '\\midrule']
    for k, lab in rows:
        r = q.loc[k]
        lines.append(f'{lab} & \\${r["shelf_healthy"]:.2f} & \\${r["shelf_unhealthy"]:.2f} & {r["shelf_healthy"] / r["shelf_unhealthy"]:.3f} & '
                     f'{r["rel_healthy"]:.1f} & {r["rel_unhealthy"]:.1f} \\\\')
    lines += ['\\midrule', ' & Added sugar & Whole-grain & Fruit and vegetables & Health Index & Stores \\\\',
              ' & (g per 1,000 kcal) & share of bread (\\%) & (\\% of calories) & (per 1,000 kcal) & \\\\', '\\midrule']
    for k, lab in rows:
        r = q.loc[k]
        lines.append(f'{lab} & {r["addsugar_1000"]:.1f} & {100 * r["whole_share"]:.1f} & {100 * r["produce_share"]:.2f} & '
                     f'{r["hi_1000"]:.2f} & {n[k]:,} \\\\')
    lines += ['\\bottomrule', '\\end{tabular}']
    TAB.parent.mkdir(parents=True, exist_ok=True)
    TAB.write_text('\n'.join(lines) + '\n')
    print(f'saved {TAB}')


def main():
    d = load()
    m = d.groupby('bin').mean(numeric_only=True).reindex(MIDS)
    show = [c for c, _, _ in ACCESS] + [f'{p}{x}' for p in ('shelf_', 'rel_') for _, a, _, b, _ in PANELS for x in (a, b)]
    print(pd.concat([m[show].round(3), d.groupby('bin').size().rename('store_years')], axis=1).to_string())
    print('top minus bottom bin:', (m[show].iloc[-1] - m[show].iloc[0]).round(3).to_dict())
    FIG.mkdir(parents=True, exist_ok=True)
    fig_access(m)
    fig_prices(m, 'shelf_', 'store_shelf_prices_by_income.png', '\\$ per 1,000 calories (2013)')
    fig_prices(m, 'rel_', 'store_same_prices_by_income.png', '% above national price')
    table(d)


if __name__ == '__main__':
    main()
