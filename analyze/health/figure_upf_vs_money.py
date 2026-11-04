"""
figure_upf_vs_money.py -- is ultra-processed food cheaper per calorie? The ultra-processed analogue
of figure_time_vs_money.py's module figure, for the Prices section of notes/obesity.tex (asked 2026-11-04).

Module level (interim/health/module_year_price_calories.parquet, build/health/build_module_price_calories.py):
a module's ultra-processed share = spending on its UPCs in NOVA group 4 / spending on its UPCs with
an ingredient list (clean/usda/build_usda_nova.py), pooled over 2016-2020; its real price per 1,000 kcal
= real spending (2013$) on purchase rows with a weight or volume size / the calories of those rows at
the module's label calorie density pooled over 2004-2020 (as in figure_time_vs_money.py). Modules with
a calorie density, at least 20 labelled UPCs with an ingredient list and at least half of their spending
on sized rows are kept.
  upf_vs_money_modules.png  left: one point per module, ultra-processed share (percent) against price
                            per 1,000 kcal (log scale), area proportional to spending, black diamonds
                            the spending-weighted mean price in each decile of the share, the
                            spending-weighted correlation in the title. Right: spending-weighted real
                            price per 1,000 kcal by year, 2004-2020, for modules above and below the
                            median ultra-processed share (the split of
                            figs/variety/price/variety_price_level_by_upf.png) and for all modules.
Output: figs/health/prices/upf_vs_money_modules.png
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
from figure_time_vs_money import wmean, wcorr

FIG = fdir / 'health' / 'prices'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, GREY = '#2c5f8a', '#a63d40', '#7a7a7a'
plt.rcParams.update({'font.size': 12, 'axes.labelsize': 13, 'axes.titlesize': 12.5, 'legend.fontsize': 10,
                     'axes.spines.top': False, 'axes.spines.right': False})


def main():
    m = pd.read_parquet(idir / 'health' / 'module_year_price_calories.parquet')
    dens = m.groupby('product_module_code').apply(lambda g: 100 * g['cals_matched'].sum() / g['grams_matched'].sum() if g['grams_matched'].sum() > 0 else np.nan)
    m['cals_est'] = m['grams'] * m['product_module_code'].map(dens) / 100
    m = m[(m['cals_est'] > 0) & (m['spend_grams'] >= 0.5 * m['spend_real'])].copy()
    m['price_kcal'] = 1000 * m['spend_grams'] / m['cals_est']
    # ultra-processed share of the module, pooled 2016-2020
    late = m[m['panel_year'].between(2016, 2020)].groupby('product_module_code').agg(
        product_module=('product_module', 'first'), spend=('spend_grams', 'sum'), cals=('cals_est', 'sum'),
        spend_nova4=('spend_nova4', 'sum'), spend_nova_any=('spend_nova_any', 'sum'), n_upcs=('n_upcs', 'sum')).reset_index()
    nova = pd.read_parquet(idir / 'usda_nielsen_merged' / 'usda_nova.parquet', columns=['upc13', 'nova'])
    prods = pd.read_csv(rdir / 'products.tsv', sep='\t', encoding='latin-1', usecols=['upc', 'product_module_code'], dtype={'upc': str})
    prods['upc13'] = '0' + prods['upc'].str.zfill(12)
    n_lab = prods.merge(nova.dropna(), on='upc13').groupby('product_module_code').size().rename('n_labelled')
    late = late.merge(n_lab, left_on='product_module_code', right_index=True, how='left')
    late = late[(late['n_labelled'] >= 20) & (late['spend_nova_any'] > 0)].copy()
    late['upf_share'] = 100 * late['spend_nova4'] / late['spend_nova_any']
    late['price_kcal'] = 1000 * late['spend'] / late['cals']
    late = late[late['price_kcal'].between(0.05, 100)]
    late['log_price'] = np.log(late['price_kcal'])
    r = wcorr(late['upf_share'].values, late['log_price'].values, late['spend'].values)
    print(f'{len(late)} modules; spending-weighted corr(ultra-processed share, log price per 1,000 kcal) = {r:.3f}; '
          f'unweighted {np.corrcoef(late.upf_share, late.log_price)[0, 1]:.3f}')
    late['dec'] = pd.qcut(late['upf_share'].rank(method='first'), 10, labels=False)
    bydec = late.groupby('dec').apply(lambda g: pd.Series({'share': wmean(g['upf_share'], g['spend']), 'price': np.exp(wmean(g['log_price'], g['spend'])), 'n': len(g)}))
    print('\nby decile of the ultra-processed share: mean share, spending-weighted geometric mean $ per 1,000 kcal'); print(bydec.round(2).to_string())
    print('\ncheapest and dearest modules per 1,000 kcal among the mostly ultra-processed:')
    hi = late[late['upf_share'] >= late['upf_share'].median()].sort_values('price_kcal')
    print(hi[['product_module', 'upf_share', 'price_kcal']].head(6).round(2).to_string(index=False)); print(hi[['product_module', 'upf_share', 'price_kcal']].tail(6).round(2).to_string(index=False))
    # over time by module half
    cut = late['upf_share'].median()
    half = late.set_index('product_module_code')['upf_share'].ge(cut).map({True: 'mostly ultra-processed', False: 'mostly not ultra-processed'})
    m['half'] = m['product_module_code'].map(half)
    rows = {}
    for lab in ('mostly ultra-processed', 'mostly not ultra-processed'):
        g = m[m['half'] == lab]
        rows[lab] = g.groupby('panel_year').apply(lambda x: 1000 * x['spend_grams'].sum() / x['cals_est'].sum())
    rows['all modules'] = m[m['half'].notna()].groupby('panel_year').apply(lambda x: 1000 * x['spend_grams'].sum() / x['cals_est'].sum())
    over = pd.DataFrame(rows)
    print(f'\nmedian ultra-processed share {cut:.1f}%; real $ per 1,000 kcal by module half and year:'); print(over.round(2).to_string())

    fig, axes = plt.subplots(1, 2, figsize=(17, 6.5))
    ax = axes[0]
    ax.scatter(late['upf_share'], late['price_kcal'], s=4 + 400 * late['spend'] / late['spend'].max(), color=RED, alpha=0.5, edgecolors='white', linewidth=0.4)
    ax.plot(bydec['share'], bydec['price'], color='black', marker='D', ms=7, lw=1.5, label='Spending-weighted mean by decile of the share')
    ax.set_yscale('log')
    ax.set_xlabel('Ultra-processed share of the module (% of labelled spending in NOVA 4)')
    ax.set_ylabel('Real price per 1,000 kcal (2013\\$, log scale)')
    ax.set_title(f'Product modules, 2016-2020: spending-weighted correlation = {r:.2f}')
    ax.legend(frameon=False, loc='upper right')
    ax = axes[1]
    for lab, col in [('mostly ultra-processed', RED), ('mostly not ultra-processed', BLUE)]:
        ax.plot(over.index, over[lab], color=col, lw=2.5, marker='o', ms=4, label=lab)
    ax.plot(over.index, over['all modules'], color='black', lw=1.5, ls='--', label='all modules')
    ax.set_xlabel('Year'); ax.set_ylabel('Real price per 1,000 kcal (2013\\$)'); ax.set_ylim(0, over.max().max() * 1.25)
    ax.set_title(f'Price per calorie by module half (split at the median share, {cut:.0f}%), spending-weighted'); ax.legend(frameon=False)
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fig.savefig(FIG / 'upf_vs_money_modules.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "upf_vs_money_modules.png"}')


if __name__ == '__main__':
    main()
