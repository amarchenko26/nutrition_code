"""Module x year and household x year sugar, calories and grams from purchases, with FIXED per-UPC
densities, for the decomposition of the 2004-2020 fall in sugar per 1,000 kcal (what-moves review,
2026-11-05).

Sugar density per UPC (grams per 100 g), fixed over time like the calorie dictionary
(build/health/build_upc_dictionary.py): tier 1 the UPC's own USDA Branded label (sugar_per_100g, only
UPCs whose calorie label was accepted as tier 1, sugar <= 100 g); tier 2 the mean over the labelled
UPCs of the same brand x module; tier 3 the mean over the labelled UPCs of the module. Added sugar
(addsugar_per_100g) the same way. Calories from the calorie dictionary (all four tiers). Grams =
quantity x multi x size in grams, as in build_nova_calorie_panel.py.

Because every label is a 2024 snapshot, a UPC's density never changes: the series built here can
move only through the mix of products bought (between modules, or among products within a module),
never through reformulation of a given UPC, and never through the label match rate (the labelled-only
columns show that rate).

Output: interim/what_moves/sugar_module_year.parquet (product_module_code, product_module,
product_group, panel_year, grams, kcal, sugar_g, addsug_g, kcal_lab, sugar_lab, spend, n_rows)
        interim/what_moves/sugar_hh_year.parquet (household_code, panel_year, kcal, sugar_g,
        addsug_g, kcal_lab, sugar_lab, spend)
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import gc, time
import numpy as np
import pandas as pd

PURCH = idir / 'purchases_food'
DICT = idir / 'health' / 'upc_dictionary.parquet'
USDA = idir / 'usda_nielsen_merged' / 'usda_wide.parquet'
OUT = idir / 'what_moves'
YEARS = range(2004, 2021)
UNIT_TO_GRAMS = {'OZ': 28.3495, 'LB': 453.592, 'FL OZ': 29.5735, 'QT': 946.353, 'GAL': 3785.41,
                 'ML': 1.0, 'LT': 1000.0, 'KG': 1000.0, 'GM': 1.0, 'GR': 1.0}


def sugar_dictionary():
    dic = pd.read_parquet(DICT).dropna(subset=['cal_per_100g'])
    usda = pd.read_parquet(USDA, columns=['upc13', 'sugar_per_100g', 'addsugar_per_100g']).rename(columns={'upc13': 'upc_13'})
    usda = usda.drop_duplicates('upc_13')
    dic = dic.merge(usda, on='upc_13', how='left')
    own = (dic['cal_tier'] == 1) & dic['sugar_per_100g'].notna() & (dic['sugar_per_100g'] <= 100)
    dic['sugar_own'] = dic['sugar_per_100g'].where(own)
    dic['addsug_own'] = dic['addsugar_per_100g'].where(own & (dic['addsugar_per_100g'] <= 100))
    lab = dic[own]
    bm = lab.groupby(['brand_descr', 'product_module_code'])[['sugar_own', 'addsug_own']].mean().rename(columns={'sugar_own': 'sugar_bm', 'addsug_own': 'addsug_bm'})
    mm = lab.groupby('product_module_code')[['sugar_own', 'addsug_own']].mean().rename(columns={'sugar_own': 'sugar_m', 'addsug_own': 'addsug_m'})
    dic = dic.merge(bm, on=['brand_descr', 'product_module_code'], how='left').merge(mm, on='product_module_code', how='left')
    dic['sugar_per_100g'] = dic['sugar_own'].fillna(dic['sugar_bm']).fillna(dic['sugar_m'])
    dic['sugar_tier'] = np.select([dic['sugar_own'].notna(), dic['sugar_bm'].notna(), dic['sugar_m'].notna()], [1, 2, 3], np.nan)
    dic['addsug_per_100g'] = dic['addsug_own'].fillna(dic['addsug_bm']).fillna(dic['addsug_m'])
    print('sugar dictionary tiers (UPCs):', dic['sugar_tier'].value_counts(dropna=False).to_dict())
    return dic[['upc_13', 'cal_per_100g', 'cal_tier', 'sugar_per_100g', 'sugar_tier', 'addsug_per_100g']]


def main():
    t0 = time.time()
    OUT.mkdir(parents=True, exist_ok=True)
    dic = sugar_dictionary()
    mods, hhs = [], []
    for y in YEARS:
        p = PURCH / f'panel_year={y}'
        if not p.exists():
            continue
        d = pd.read_parquet(p, columns=['upc', 'quantity', 'multi', 'size1_amount', 'size1_units', 'product_module_code', 'product_module',
                                        'product_group', 'department_descr', 'household_code', 'total_price_paid'])
        d = d[d['department_descr'] != 'MAGNET DATA']
        d['upc_13'] = '0' + d['upc'].astype(str).str.zfill(12)
        d = d.merge(dic, on='upc_13', how='left')
        d['grams'] = d['quantity'] * d['multi'].fillna(1).clip(lower=1) * d['size1_amount'] * d['size1_units'].map(UNIT_TO_GRAMS)
        d.loc[d['grams'] <= 0, 'grams'] = np.nan
        d['kcal'] = d['grams'] * d['cal_per_100g'] / 100
        d['sugar_g'] = d['grams'] * d['sugar_per_100g'] / 100
        d['addsug_g'] = d['grams'] * d['addsug_per_100g'] / 100
        lab = (d['cal_tier'] == 1) & (d['sugar_tier'] == 1)
        d['kcal_lab'] = d['kcal'].where(lab)
        d['sugar_lab'] = d['sugar_g'].where(lab)
        m = d.groupby(['product_module_code', 'product_module', 'product_group']).agg(
            grams=('grams', 'sum'), kcal=('kcal', 'sum'), sugar_g=('sugar_g', 'sum'), addsug_g=('addsug_g', 'sum'),
            kcal_lab=('kcal_lab', 'sum'), sugar_lab=('sugar_lab', 'sum'), spend=('total_price_paid', 'sum'), n_rows=('upc', 'size')).reset_index()
        m['panel_year'] = y
        mods.append(m)
        h = d.groupby('household_code').agg(kcal=('kcal', 'sum'), sugar_g=('sugar_g', 'sum'), addsug_g=('addsug_g', 'sum'),
                                             kcal_lab=('kcal_lab', 'sum'), sugar_lab=('sugar_lab', 'sum'), spend=('total_price_paid', 'sum')).reset_index()
        h['panel_year'] = y
        hhs.append(h)
        tot = d['kcal'].sum()
        print(f'{y}: {len(d):,} rows; sugar/1,000 kcal all {1000 * d.sugar_g.sum() / tot:.1f}, labelled only {1000 * d.sugar_lab.sum() / d.kcal_lab.sum():.1f}; '
              f'labelled share of kcal {d.kcal_lab.sum() / tot:.2f}; sugar tier of kcal: own {d.kcal[d.sugar_tier == 1].sum() / tot:.2f} brand {d.kcal[d.sugar_tier == 2].sum() / tot:.2f} '
              f'module {d.kcal[d.sugar_tier == 3].sum() / tot:.2f} ({time.time() - t0:.0f}s)', flush=True)
        del d; gc.collect()
    pd.concat(mods, ignore_index=True).to_parquet(OUT / 'sugar_module_year.parquet', index=False)
    pd.concat(hhs, ignore_index=True).to_parquet(OUT / 'sugar_hh_year.parquet', index=False)
    print(f'saved {OUT} ({time.time() - t0:.0f}s)')


if __name__ == '__main__':
    main()
