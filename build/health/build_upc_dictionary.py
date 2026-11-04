"""Product-level calorie density and NOVA group for every Nielsen UPC, fixed over time.

Why: the USDA Branded file is a 2024 snapshot, so the share of grocery calories with a label rises
from 39% (2004) to 81% (2020) and any labelled-only calorie series rises with it. A density assigned
once per UPC, independent of the year, removes that artifact; the earlier module x year imputation
moved with whichever products happened to be labelled that year (Anya 2026-11-03).

Reads raw/products.tsv (every UPC version; the latest version is kept), interim/usda_nielsen_merged/
usda_wide.parquet (label calories per 100 g), usda_nova.parquet (NOVA group from the ingredient list)
and interim/health/module_year_price_calories.parquet (labelled calories and grams by module and
year, own labels only).
Calorie density per 100 g, in tiers:
  1  the UPC's own USDA label (labels above 900 kcal/100 g, the energy of pure fat, are treated as
     wrong matches and dropped);
  2  the labelled UPC of the same brand, product module and size unit with the closest package
     size (same product line, another flavour or variety);
  3  the mean density of the labelled UPCs of the same brand and module;
  4  the module's density pooled over 2004-2020 purchases (sum of labelled calories / sum of
     labelled grams);
  missing when the module has no labelled purchases.
Output: interim/health/upc_dictionary.parquet (upc_13, product_module_code, brand_descr, multi,
cal_per_100g, cal_tier, nova). Read by build_nova_calorie_panel.py and build_module_price_calories.py.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import time
import numpy as np
import pandas as pd

OUT = idir / 'health' / 'upc_dictionary.parquet'


def main():
    t0 = time.time()
    prod = pd.read_csv(rdir / 'products.tsv', sep='\t', encoding='latin-1', dtype={'upc': str},
                       usecols=['upc', 'upc_ver_uc', 'product_module_code', 'brand_descr', 'multi', 'size1_amount', 'size1_units'])
    prod = prod.sort_values(['upc', 'upc_ver_uc']).drop_duplicates('upc', keep='last')
    prod['upc_13'] = '0' + prod['upc'].str.zfill(12)
    print(f'{len(prod):,} UPCs in products.tsv ({time.time() - t0:.0f}s)')
    wide = pd.read_parquet(idir / 'usda_nielsen_merged' / 'usda_wide.parquet', columns=['upc13', 'cal_per_100g']).rename(columns={'upc13': 'upc_13', 'cal_per_100g': 'cal_own'})
    wide.loc[(wide['cal_own'] > 900) | (wide['cal_own'] < 0), 'cal_own'] = np.nan
    nova = pd.read_parquet(idir / 'usda_nielsen_merged' / 'usda_nova.parquet', columns=['upc13', 'nova']).rename(columns={'upc13': 'upc_13'})
    d = prod.merge(wide, on='upc_13', how='left').merge(nova, on='upc_13', how='left')
    d['nova'] = d['nova'].astype('float')
    d['log_size'] = np.log(d['size1_amount'].where(d['size1_amount'] > 0))
    lab = d[d['cal_own'].notna()]
    print(f'own label (tier 1): {len(lab):,} UPCs')
    # tier 2: nearest package size within brand x module x size unit
    keys = ['brand_descr', 'product_module_code', 'size1_units']
    unl = d[d['cal_own'].isna() & d['log_size'].notna() & d[keys].notna().all(axis=1)].copy()
    lab2 = lab[lab['log_size'].notna()].dropna(subset=keys)[keys + ['log_size', 'cal_own']].rename(columns={'cal_own': 'cal_t2'})
    near = pd.merge_asof(unl[['upc_13', 'log_size'] + keys].sort_values('log_size'), lab2.sort_values('log_size'),
                         on='log_size', by=keys, direction='nearest')
    d = d.merge(near[['upc_13', 'cal_t2']], on='upc_13', how='left')
    # tier 3: brand x module mean over labelled UPCs
    t3 = lab.groupby(['brand_descr', 'product_module_code'])['cal_own'].mean().rename('cal_t3').reset_index()
    d = d.merge(t3, on=['brand_descr', 'product_module_code'], how='left')
    # tier 4: module density pooled over 2004-2020 purchases, own labels only
    my = pd.read_parquet(idir / 'health' / 'module_year_price_calories.parquet', columns=['product_module_code', 'cals_matched', 'grams_matched'])
    t4 = my.groupby('product_module_code').sum()
    t4 = (100 * t4['cals_matched'] / t4['grams_matched'].replace(0, np.nan)).rename('cal_t4')
    d = d.merge(t4, left_on='product_module_code', right_index=True, how='left')
    d['cal_per_100g'] = d['cal_own']
    d['cal_tier'] = np.where(d['cal_own'].notna(), 1, 0)
    for tier, col in ((2, 'cal_t2'), (3, 'cal_t3'), (4, 'cal_t4')):
        use = d['cal_per_100g'].isna() & d[col].notna()
        d.loc[use, 'cal_per_100g'] = d.loc[use, col]
        d.loc[use, 'cal_tier'] = tier
    d.loc[d['cal_per_100g'].isna(), 'cal_tier'] = np.nan
    out = d[['upc_13', 'product_module_code', 'brand_descr', 'multi', 'cal_per_100g', 'cal_tier', 'nova']]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    print('UPCs by density tier:'); print(out['cal_tier'].value_counts(dropna=False).sort_index().to_string())
    # how close are the tiers to the truth? leave-one-out check on labelled UPCs: tier-2 and tier-3 predictions vs own label
    chk = lab[lab['log_size'].notna()].dropna(subset=keys).copy()
    g = chk.groupby(keys)
    chk['n_grp'] = g['cal_own'].transform('size')
    chk = chk[chk['n_grp'] >= 2]
    # tier-3 leave-one-out
    s3 = chk.groupby(['brand_descr', 'product_module_code'])['cal_own'].agg(['sum', 'size'])
    chk = chk.merge(s3, left_on=['brand_descr', 'product_module_code'], right_index=True)
    chk['loo_t3'] = (chk['sum'] - chk['cal_own']) / (chk['size'] - 1)
    chk = chk[chk['size'] >= 2]
    err3 = (chk['loo_t3'] - chk['cal_own']).abs() / chk['cal_own'].replace(0, np.nan)
    err4 = (chk['product_module_code'].map(t4) - chk['cal_own']).abs() / chk['cal_own'].replace(0, np.nan)
    print(f'\nleave-one-out check on {len(chk):,} labelled UPCs with a labelled sibling: median |error| of the brand x module mean (tier 3) '
          f'{err3.median():.3f}, of the module pooled density (tier 4) {err4.median():.3f} (share of the true density)')
    print(f'saved {OUT} ({time.time() - t0:.0f}s)')


if __name__ == '__main__':
    main()
