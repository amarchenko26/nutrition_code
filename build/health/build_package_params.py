"""Inputs for utils/food_measures.package_grams: count-size distributions and powder grams per quart.

Reads raw/products.tsv (latest version of each UPC) and interim/syndigo_nielsen_merged/syndigo_wide.parquet
(Syndigo's package weight g_total and label density).
Outputs (interim/health/):
  count_gram_params.parquet       by module and by product group: mean and sd of total count (size x multi) of
                                  count-sized UPCs; mean, sd, min and max of grams of weight-sized UPCs
                                  (GetGrams.do in the Allcott et al. replication, unweighted across UPCs).
  powder_grams_per_quart.parquet  upc_13, g_per_qt, source, for QT-sized UPCs of the powdered modules. A Syndigo
                                  weight is usable when g_total / size is between 0.5 and 300 g per quart and
                                  the label is at most 900 kcal/100 g: many sugar-free records were filled from
                                  Nielsen's prepared quarts (946 g per quart). Fallbacks: median of usable
                                  records by brand, then by module and sweetener (SS, SF, UN in the brand name).
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.food_measures import UNIT_TO_GRAMS, POWDER_MODULES

import numpy as np
import pandas as pd

OUT = idir / 'health'


def main():
    prod = pd.read_csv(rdir / 'products.tsv', sep='\t', encoding='latin-1', dtype={'upc': str},
                       usecols=['upc', 'upc_ver_uc', 'product_module_code', 'product_group_code', 'brand_descr', 'multi', 'size1_amount', 'size1_units'])
    prod = prod.sort_values(['upc', 'upc_ver_uc']).drop_duplicates('upc', keep='last')
    prod['upc_13'] = '0' + prod['upc'].str.zfill(12)
    prod['unit'] = prod['size1_units'].astype(str).str.strip()
    prod['multi'] = pd.to_numeric(prod['multi'], errors='coerce').fillna(1).clip(lower=1)
    prod['size'] = pd.to_numeric(prod['size1_amount'], errors='coerce')
    prod['grams'] = prod['size'] * prod['multi'] * prod['unit'].map(UNIT_TO_GRAMS)
    prod['total_ct'] = (prod['size'] * prod['multi']).where(prod['unit'] == 'CT')
    # powdered mixes in prepared quarts carry prepared grams here: leave them out of the module weight distribution
    prod.loc[(prod['unit'] == 'QT') & prod['product_module_code'].isin(POWDER_MODULES), 'grams'] = np.nan

    rows = []
    for level, col in (('module', 'product_module_code'), ('group', 'product_group_code')):
        g = prod.groupby(col)
        t = pd.DataFrame({'mean_ct': g['total_ct'].mean(), 'sd_ct': g['total_ct'].std(), 'mean_g': g['grams'].mean(), 'sd_g': g['grams'].std(),
                          'min_g': g['grams'].min(), 'max_g': g['grams'].max()}).reset_index().rename(columns={col: 'code'})
        t['level'] = level
        rows.append(t)
    ct = pd.concat(rows, ignore_index=True)
    ct.to_parquet(OUT / 'count_gram_params.parquet', index=False)
    print(f'count params: {len(ct):,} rows, modules with count and weight sizes: {((ct.level == "module") & ct.mean_ct.notna() & ct.mean_g.notna()).sum():,}')

    pw = prod[prod['product_module_code'].isin(POWDER_MODULES) & (prod['unit'] == 'QT')].copy()
    syn = pd.read_parquet(idir / 'syndigo_nielsen_merged' / 'syndigo_wide.parquet', columns=['upc', 'g_total', 'cal_per_100g']).rename(columns={'upc': 'upc_13'})
    pw = pw.merge(syn, on='upc_13', how='left')
    pw['per_qt'] = pw['g_total'] / pw['size']
    usable = pw['per_qt'].between(0.5, 300) & ~(pw['cal_per_100g'] > 900)
    pw['g_own'] = pw['per_qt'].where(usable)
    pw['sweet'] = pw['brand_descr'].astype(str).str.extract(r'\b(SS|SF|UN)\b', expand=False).fillna('none')
    by_brand = pw.groupby('brand_descr')['g_own'].median().rename('g_brand')
    by_type = pw.groupby(['product_module_code', 'sweet'])['g_own'].median().rename('g_type')
    by_mod = pw.groupby('product_module_code')['g_own'].median().rename('g_mod')
    print('usable grams of powder per prepared quart, median:'); print(by_type.round(1).to_string())
    pw = pw.merge(by_brand, left_on='brand_descr', right_index=True, how='left') \
           .merge(by_type, left_on=['product_module_code', 'sweet'], right_index=True, how='left') \
           .merge(by_mod, left_on='product_module_code', right_index=True, how='left')
    pw['g_per_qt'] = pw['g_own'].fillna(pw['g_brand']).fillna(pw['g_type']).fillna(pw['g_mod'])
    pw['source'] = np.select([pw['g_own'].notna(), pw['g_brand'].notna(), pw['g_type'].notna()], ['own', 'brand', 'module_sweetener'], default='module')
    print('UPCs by source:', pw['source'].value_counts().to_dict(), '; missing:', int(pw['g_per_qt'].isna().sum()))
    pw[['upc_13', 'product_module_code', 'sweet', 'g_per_qt', 'source']].to_parquet(OUT / 'powder_grams_per_quart.parquet', index=False)


if __name__ == '__main__':
    main()
