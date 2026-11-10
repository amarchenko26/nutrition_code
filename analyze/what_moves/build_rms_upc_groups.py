"""UPC attributes for the RMS county price index (build_local_price_index_rms.py) and the store choice sets
(build_store_choice_sets_rms.py), both run on Oscar.

For the rich-poor local price table (Anya 2026-11-10: "this needs to be RMS not HMS"). One row per UPC of raw/products.tsv
(the RMS product file) in a food module with a healthiness score: healthy = claude_hi 4-10, unhealthy = 0-3
(interim/rms_variety/claude_hi_scores.parquet); grams per unit = utils.food_measures.package_grams; calories per unit =
grams x the interim/health/upc_dictionary.parquet density (missing when the UPC is not in the dictionary).
For the store choice sets (2026-11-10, measures of Allcott et al. 2019 Figure II): addsugar_unit = grams of added sugar per
unit; hi_1000 = Health Index per 1,000 kcal (GetHealthIndex.do formula of build/hi/usda/build_hi_usda_panel.py, fruit and
vegetables at fixed values); produce = utils.food_measures fruit or vegetable; nova4 = NOVA group 4 (missing without a
group); bread = modules 4000-4002 and whole = whole grain by the upc_descr rules of build_hi_usda_panel.py.
Output: interim/what_moves/rms_upc_groups.parquet (upc as int64), copied to /users/amarche4/data/rms_variety/ on Oscar.
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

from utils.food_measures import package_grams, is_fruit, is_veg

BREAD_MODULES = {4000, 4001, 4002}
NUTR = ['cal_per_100g', 'addsugar_per_100g', 'sugar_per_100g', 'satfat_per_100g', 'sodium_per_100g', 'fiber_per_100g', 'chol_per_100g', 'nova']

OUT = idir / 'what_moves' / 'rms_upc_groups.parquet'


def main():
    p = pd.read_csv(rdir / 'products.tsv', sep='\t', dtype={'upc': str, 'size1_units': str}, encoding='latin-1',
                    usecols=['upc', 'upc_ver_uc', 'upc_descr', 'product_module_code', 'product_group_code', 'multi', 'size1_amount', 'size1_units'])
    p = p.sort_values('upc_ver_uc').drop_duplicates('upc').drop(columns='upc_ver_uc')
    hi = pd.read_parquet(idir / 'rms_variety' / 'claude_hi_scores.parquet', columns=['product_module_code', 'claude_hi'])
    p = p.merge(hi.dropna(), on='product_module_code', how='inner')
    p['upc_13'] = '0' + p['upc'].str.zfill(12)
    p = p.merge(pd.read_parquet(idir / 'health' / 'upc_dictionary.parquet', columns=['upc_13'] + NUTR), on='upc_13', how='left')
    p['grams_unit'] = package_grams(p)
    p['kcal_unit'] = p['grams_unit'] * p['cal_per_100g'] / 100
    p['healthy'] = (p['claude_hi'] >= 4).astype(np.int8)
    p['addsugar_unit'] = p['grams_unit'] * p['addsugar_per_100g'] / 100
    fruit = is_fruit(p['product_module_code'], p['product_group_code']).astype(bool)
    veg = is_veg(p['product_module_code'], p['product_group_code']).astype(bool)
    hi_100g = np.where(fruit | veg, fruit * 100 / 320 + veg * 100 / 390,
                       p['fiber_per_100g'].fillna(0) / 29.5 - p['sugar_per_100g'].fillna(0) / 32.8 - p['satfat_per_100g'].fillna(0) / 17.2
                       - p['sodium_per_100g'].fillna(0) / 2.3 - p['chol_per_100g'].fillna(0) / 0.3)
    p['hi_1000'] = hi_100g / p['cal_per_100g'] * 1000
    p['produce'] = (fruit | veg).astype(np.int8)
    p['nova4'] = np.where(p['nova'].notna(), (p['nova'] == 4).astype(float), np.nan)
    d = p['upc_descr'].fillna('')
    ww = d.str.contains('WW') & ~d.str.startswith('WW')
    p['bread'] = p['product_module_code'].isin(BREAD_MODULES).astype(np.int8)
    p['whole'] = (p['bread'].astype(bool) & (d.str.contains(r'W-W|WWH|WHL|GWHY|WH-G|RY-WL|SPT|G-WH|G-WM| WG |WGW|WW100%') | ww)).astype(np.int8)
    out = p[['upc', 'product_module_code', 'claude_hi', 'healthy', 'grams_unit', 'kcal_unit', 'addsugar_unit', 'hi_1000', 'produce', 'nova4',
             'bread', 'whole']].copy()
    out['upc'] = out['upc'].astype(np.int64)
    out.to_parquet(OUT, index=False)
    print(f'{len(out):,} food UPCs; with grams {out.grams_unit.notna().mean():.1%}, with calories {out.kcal_unit.notna().mean():.1%}; '
          f'healthy {out.healthy.mean():.1%}; saved {OUT}')


if __name__ == '__main__':
    main()
