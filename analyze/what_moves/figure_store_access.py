"""What stores offer, by the income of the ZIP codes their shoppers live in (Anya 2026-11-10, after Allcott, Diamond and Dube
2019, Figure II): access, not consumption.

Reads interim/what_moves/store_choice_sets.parquet (build_store_choice_sets_rms.py on Oscar: one row per RMS store and year,
measures over the food UPCs the store sold, each weighted by the calories in one package) and store_shopper_income.parquet
(build_store_shopper_income.py: trip-weighted ACS median household income of the panel shoppers' home ZIP codes).
Stores with MIN_TRIPS or more panel trips from MIN_HH or more households. Bins of $10,000 of shopper ZIP income; points are
means across store-years in the bin, 2006-2020, as in their figure.
Output: figs/what_moves/prices/store_access_by_income.png. Prints the bin means, the top-minus-bottom bin differences and the
same by store channel.
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
MIN_TRIPS, MIN_HH = 50, 10
EDGES = [0, 30, 40, 50, 60, 70, 80, 90, np.inf]            # $000s of shopper ZIP median household income
MIDS = [25, 35, 45, 55, 65, 75, 85, 95]
PANELS = [('addsugar_1000', 'Added sugar per 1,000 calories (g)', 1), ('whole_share', 'Whole-grain share of bread (%)', 100),
          ('produce_share', 'Fruit and vegetable share of calories (%)', 100), ('hi_1000', 'Health Index per 1,000 calories', 1),
          ('nova4_share', 'Ultra-processed share of calories (%)', 100)]


def load():
    s = pd.read_parquet(idir / 'what_moves' / 'store_choice_sets.parquet')
    inc = pd.read_parquet(idir / 'what_moves' / 'store_shopper_income.parquet')
    inc = inc[(inc['n_trips'] >= MIN_TRIPS) & (inc['n_households'] >= MIN_HH) & inc['zip_income'].notna()]
    d = s.merge(inc[['store_code_uc', 'zip_income']], on='store_code_uc', how='inner')
    d['bin'] = pd.cut(d['zip_income'] / 1000, EDGES, labels=MIDS, right=False).astype(int)
    d['price_gap'] = 100 * (d['price_healthy'] - d['price_unhealthy'])
    print(f'{len(d):,} store-years, {d.store_code_uc.nunique():,} stores; of {len(s):,} store-years in RMS')
    return d


def main():
    d = load()
    cols = [c for c, _, _ in PANELS] + ['price_healthy', 'price_unhealthy', 'price_gap', 'price_healthy_sold', 'price_unhealthy_sold']
    m = d.groupby('bin')[cols].mean()
    n = d.groupby('bin').size()
    print(pd.concat([m.round(3), n.rename('store_years')], axis=1).to_string())
    print('top minus bottom bin:', (m.iloc[-1] - m.iloc[0]).round(4).to_dict())
    if 'channel_code' in d.columns:
        print(d.groupby(['channel_code', 'bin'])[['hi_1000', 'produce_share', 'price_gap']].mean().unstack(0).round(3).to_string())
    fig, axes = plt.subplots(2, 3, figsize=(32, 19))
    fig.subplots_adjust(wspace=0.32, hspace=0.42)
    for ax, (c, lab, k) in zip(axes.flat, PANELS):
        ax.plot(MIDS, k * m[c].values, color=BLUE, lw=4, marker='o', ms=12)
        ax.set_title(lab, pad=14)
        ax.set_xlabel("Shoppers' ZIP median income ($000s)")
    ax = axes.flat[5]
    for c, col, lab in [('price_healthy', BLUE, 'healthy products'), ('price_unhealthy', RED, 'unhealthy products')]:
        ax.plot(MIDS, 100 * m[c].values, color=col, lw=4, marker='o', ms=12, label=lab)
    ax.axhline(0, color=GREY, lw=1.2, ls='--')
    ax.set_title('Price relative to the national price (%)', pad=14)
    ax.set_xlabel("Shoppers' ZIP median income ($000s)")
    ax.legend(frameon=False, fontsize=22)
    for ax in axes.flat:
        ax.set_xticks(MIDS); ax.set_xticklabels(['<30', '30-40', '40-50', '50-60', '60-70', '70-80', '80-90', '90+'], rotation=30)
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / 'store_access_by_income.png', bbox_inches='tight', dpi=100); plt.close(fig)
    print(f'saved {FIG / "store_access_by_income.png"}')


if __name__ == '__main__':
    main()
