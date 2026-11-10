"""Product module: real price per kg and per 1,000 kcal, and minutes of preparation per 1,000 kcal.

For the time-money section of 0_main.tex (Anya 2026-11-10: is healthy food cheaper, and does the order change
once preparation time is valued?). Consumer-panel purchases pooled over 2004-2020.

Money: interim/health/module_year_price_calories.parquet (build/health/build_module_price_calories.py) with the
definitions of analyze/health/figure_time_vs_money.py: real (2013$) spending on purchase rows with a package size,
over their grams and over their calories at the interim/health/upc_dictionary.parquet density; modules with
calories and at least half of their spending on rows with a size.

Time: minutes per preparation = MIN_PER_POINT x (preparation score - 1), the median of the minutes stated in the
score rationales of build/prep_time/build_prep_time_index.py (scores 2-7: 6, 10, 17.5, 20, 26, 32.5 minutes; none
stated for 8 and 9). Modules without a score (condiments, sauces, toppings, sweeteners) take no time. A preparation
serves SERVINGS label servings. Servings bought, UPC by UPC: units (interim/health/upc_year_spending.parquet) x
package grams (utils.food_measures.package_grams on raw/products.tsv) over the UPC's label serving in grams (USDA
FoodData Central, g or ml; else the module median, else the product-group median); powdered drinks sized in
prepared quarts with a per-drink label density: 4 servings of 8 fl oz per quart. Calories per serving = the
module's calories over its servings, both from the same UPC rows; minutes per 1,000 kcal = 1,000 x minutes per
preparation / (SERVINGS x calories per serving).

Healthiness: claude_hi (0-10, interim/rms_variety/claude_hi_scores.parquet) in the fixed ranges HI_CUTS of
analyze/variety/analyze_variety_price_index.py: 0-2, 3, 4-5, 6-10.

Output: interim/what_moves/module_time_price.parquet, one row per module.
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

from utils.food_measures import package_grams, POWDER_MODULES, POWDER_DENSITY_MIN, GRAMS_PER_QT_PREPARED

OUT = idir / 'what_moves' / 'module_time_price.parquet'
SERVINGS = 4                      # label servings per preparation
MIN_PER_POINT = 5                 # minutes per preparation-score point above 1
HI_CUTS, HI_LABELS = [-1, 2, 3, 5, 10], ['0-2', '3', '4-5', '6-10']
SERVING_UNITS = ['g', 'GRM', 'GM', 'ml', 'MLT']


def money():
    m = pd.read_parquet(idir / 'health' / 'module_year_price_calories.parquet')
    m = m.groupby('product_module_code').agg(module=('product_module', 'first'), group=('product_group', 'first'), spend=('spend_real', 'sum'),
                                             spend_g=('spend_grams', 'sum'), grams=('grams', 'sum'), cals=('cals_est', 'sum')).reset_index()
    m = m[(m['cals'] > 0) & (m['spend_g'] >= 0.5 * m['spend'])].copy()
    m['usd_kg'] = 1000 * m['spend_g'] / m['grams']
    m['usd_1000kcal'] = 1000 * m['spend_g'] / m['cals']
    return m


def servings():
    """Servings and calories bought per module, UPC by UPC."""
    p = pd.read_csv(rdir / 'products.tsv', sep='\t', dtype={'upc': str, 'size1_units': str}, encoding='latin-1',
                    usecols=['upc', 'upc_ver_uc', 'product_module_code', 'product_group_code', 'multi', 'size1_amount', 'size1_units'])
    p = p.sort_values('upc_ver_uc').drop_duplicates('upc').drop(columns='upc_ver_uc')
    q = pd.read_parquet(idir / 'health' / 'upc_year_spending.parquet', columns=['upc', 'quantity']).groupby('upc', as_index=False)['quantity'].sum()
    d = q.merge(p, on='upc', how='inner')
    d['upc_13'] = '0' + d['upc'].str.zfill(12)
    d = d.merge(pd.read_parquet(idir / 'health' / 'upc_dictionary.parquet', columns=['upc_13', 'cal_per_100g']), on='upc_13', how='left')
    d['grams'] = d['quantity'] * package_grams(d)
    d['cals'] = d['grams'] * d['cal_per_100g'] / 100
    lab = pd.read_parquet(idir / 'usda_nielsen_merged' / 'usda_wide.parquet', columns=['upc13', 'serving_size', 'serving_size_unit'])
    lab = lab[lab['serving_size_unit'].isin(SERVING_UNITS) & (lab['serving_size'] > 0)]
    lab = lab.groupby('upc13', as_index=False)['serving_size'].median().rename(columns={'upc13': 'upc_13', 'serving_size': 'serv_g'})
    d = d.merge(lab, on='upc_13', how='left')
    d['serv_own'] = d['serv_g'].notna()
    d['serv_g'] = d['serv_g'].fillna(d.groupby('product_module_code')['serv_g'].transform('median'))
    d['serv_g'] = d['serv_g'].fillna(d.groupby('product_group_code')['serv_g'].transform('median'))
    prepared = d['product_module_code'].isin(POWDER_MODULES) & (d['size1_units'].str.strip() == 'QT') & (d['cal_per_100g'] < POWDER_DENSITY_MIN)
    d.loc[prepared, 'serv_g'] = GRAMS_PER_QT_PREPARED / 4
    d = d[(d['cals'] > 0) & (d['serv_g'] > 0)].copy()
    d['servings'] = d['grams'] / d['serv_g']
    s = d.groupby('product_module_code').agg(servings=('servings', 'sum'), cals_upc=('cals', 'sum'),
                                             serv_g=('serv_g', 'median'), n_upcs=('upc', 'size')).reset_index()
    own = d.groupby('product_module_code').apply(lambda x: x.loc[x['serv_own'], 'cals'].sum() / x['cals'].sum()).rename('cal_share_own_serving')
    s = s.merge(own.reset_index(), on='product_module_code', how='left')
    s['kcal_serving'] = s['cals_upc'] / s['servings']
    print(f'UPCs with units, size and calories: {len(d):,}; calories on a UPC\'s own label serving: '
          f'{d.loc[d.serv_own, "cals"].sum() / d["cals"].sum():.1%}', flush=True)
    return s


def main():
    m = money().merge(servings(), on='product_module_code', how='left')
    hi = pd.read_parquet(idir / 'rms_variety' / 'claude_hi_scores.parquet', columns=['product_module_code', 'claude_hi'])
    pt = pd.read_parquet(idir / 'rms_variety' / 'prep_time_scores.parquet', columns=['product_module_code', 'prep_time'])
    m = m.merge(hi, on='product_module_code', how='left').merge(pt, on='product_module_code', how='left')
    m = m[m['claude_hi'].notna() & m['kcal_serving'].notna()].copy()
    m['hi_bin'] = pd.cut(m['claude_hi'], bins=HI_CUTS, labels=HI_LABELS).astype(str)
    m['min_prep'] = MIN_PER_POINT * (m['prep_time'].fillna(1) - 1)
    m['min_1000kcal'] = 1000 * m['min_prep'] / (SERVINGS * m['kcal_serving'])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    m.to_parquet(OUT, index=False)
    print(f'saved {len(m)} modules to {OUT}; share of spending kept {m.spend.sum() / money().spend.sum():.3f}')


if __name__ == '__main__':
    main()
