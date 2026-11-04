"""
figure_upf_products.py -- ultra-processed (NOVA group 4) against unprocessed (NOVA 1) products
over time, for notes/obesity.tex. Replaces upc_counts_by_category.png and upf_counts.png
(figure_variety_and_upf.py, deleted 2026-11-03 at Anya's request); renamed from
figure_nova_products.py on 2026-11-04 (ultra-processed wording throughout).

Base: the UPCs bought by Nielsen panelists in the year (interim/health/upc_year_spending.parquet,
build/health/build_upc_year_spending.py) that match a USDA ingredient list classified by
clean/usda/build_usda_nova.py (interim/usda_nielsen_merged/usda_nova.parquet). The USDA file is
a 2024 snapshot, so products discontinued before about 2018 have no ingredient list and the
match rate rises over time; shares among matched products are the comparable series, counts
are not.
Left: share of matched UPCs bought in each NOVA group. Right: share of matched spending in each
group. Group 4 in red, group 1 in blue, groups 2 and 3 in grey.
Output: figs/health/products/upf_products.png
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

FIG = fdir / 'health' / 'products'
FIG.mkdir(parents=True, exist_ok=True)
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 10.5,
                     'axes.spines.top': False, 'axes.spines.right': False})
BLUE, RED, GREY, GREY2 = '#2c5f8a', '#a63d40', '#7a7a7a', '#bbbbbb'
NOVA_LAB = {1: 'Unprocessed or minimally processed (NOVA 1)', 2: 'Culinary ingredients (NOVA 2)',
            3: 'Processed (NOVA 3)', 4: 'Ultra-processed (NOVA 4)'}
NOVA_COL = {1: BLUE, 2: GREY2, 3: GREY, 4: RED}
NOVA_LW = {1: 2.5, 2: 1.5, 3: 1.5, 4: 2.5}


def main():
    u = pd.read_parquet(idir / 'health' / 'upc_year_spending.parquet', columns=['upc', 'panel_year', 'spending'])
    nova = pd.read_parquet(idir / 'usda_nielsen_merged' / 'usda_nova.parquet', columns=['upc13', 'nova'])
    u['upc13'] = '0' + u['upc'].astype(str).str.zfill(12)
    m = u.merge(nova, on='upc13', how='left')
    m['nova'] = m['nova'].astype('float')
    print(f'UPC-years matched to a classified ingredient list: {m.nova.notna().mean():.3f}; '
          f'spending matched: {m.loc[m.nova.notna(), "spending"].sum() / m.spending.sum():.3f}')
    mm = m[m['nova'].notna()]
    upc_share = mm.groupby('panel_year')['nova'].value_counts(normalize=True).unstack()
    spend_share = mm.groupby(['panel_year', 'nova'])['spending'].sum().unstack()
    spend_share = spend_share.div(spend_share.sum(axis=1), axis=0)
    match = m.groupby('panel_year')['nova'].apply(lambda s: s.notna().mean())
    print('\nshare of matched UPCs bought, by NOVA group:'); print((100 * upc_share).round(1).to_string())
    print('\nshare of matched spending, by NOVA group:'); print((100 * spend_share).round(1).to_string())
    print('\nmatch rate (UPCs):'); print(match.round(3).to_dict())

    fig, axes = plt.subplots(1, 2, figsize=(16, 6), sharey=True)
    for ax, tab, title in [(axes[0], upc_share, 'Share of products bought (distinct UPCs)'),
                           (axes[1], spend_share, 'Share of spending')]:
        for g in (4, 1, 3, 2):
            ax.plot(tab.index, 100 * tab[g], color=NOVA_COL[g], lw=NOVA_LW[g], marker='o', ms=5 if g in (1, 4) else 3, label=NOVA_LAB[g])
        ax.set_xlabel('Year'); ax.set_title(title)
        ax.set_ylim(0, 100)
    axes[0].set_ylabel('Percent of matched products or spending')
    axes[0].legend(frameon=False, loc='center left')
    fig.savefig(FIG / 'upf_products.png', bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'saved {FIG / "upf_products.png"}')


if __name__ == '__main__':
    main()
