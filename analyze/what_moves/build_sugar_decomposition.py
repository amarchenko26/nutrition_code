"""Sugar, calories and volumes from purchases at fixed per-UPC values, for the decomposition of the 2004-2020 fall in
sugar per 1,000 kcal (what-moves review, 2026-11-05). Rewritten after the size rules moved to utils/food_measures.py.

Reads interim/purchases_food/panel_year=<y>/ and interim/health/upc_dictionary.parquet (one set of label values per
UPC, fixed over time, so these series move only through the mix of products bought, never through reformulation of
a given UPC and never through the label match rate). Grams = quantity x utils.food_measures.package_grams.

Outputs (interim/what_moves/):
  sugar_module_year.parquet  panel_year x module, projection-weighted sums: grams, kcal, sugar_g, addsug_g, kcal_lab,
                             sugar_lab (rows with their own label), spend; n_rows unweighted
  sugar_hh_year.parquet      household_code x panel_year, unweighted sums: grams, kcal, sugar_g, addsug_g, kcal_lab,
                             sugar_lab, spend, soda_reg_g, soda_diet_g
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.food_measures import package_grams

import gc, time
import numpy as np
import pandas as pd

PURCH = idir / 'purchases_food'
DICT = idir / 'health' / 'upc_dictionary.parquet'
PAN = idir / 'panelists' / 'panelists_all_years.parquet'
OUT = idir / 'what_moves'
SODA_REG, SODA_DIET = 1484, 1553


def main():
    t0 = time.time()
    OUT.mkdir(parents=True, exist_ok=True)
    dic = pd.read_parquet(DICT, columns=['upc_13', 'cal_per_100g', 'cal_tier', 'sugar_per_100g', 'addsugar_per_100g'])
    pan = pd.read_parquet(PAN, columns=['household_code', 'panel_year', 'projection_factor'])
    mods, hhs = [], []
    for y in range(2004, 2021):
        d = pd.read_parquet(PURCH / f'panel_year={y}', columns=['upc', 'quantity', 'multi', 'size1_amount', 'size1_units', 'product_module_code', 'product_group_code',
                                                                'product_module', 'department_descr', 'household_code', 'total_price_paid'])
        d = d[d['department_descr'] != 'MAGNET DATA'].drop(columns='department_descr')
        d['upc_13'] = '0' + d['upc'].astype(str).str.zfill(12)
        d = d.merge(dic, on='upc_13', how='left')
        wm = pan[pan['panel_year'] == y].set_index('household_code')['projection_factor']
        w = d['household_code'].map(wm).fillna(0.0)
        d['grams'] = d['quantity'] * package_grams(d)
        d['kcal'] = d['grams'] * d['cal_per_100g'] / 100
        d['sugar_g'] = d['grams'] * d['sugar_per_100g'] / 100
        d['addsug_g'] = d['grams'] * d['addsugar_per_100g'] / 100
        own = d['cal_tier'] == 1
        d['kcal_lab'] = d['kcal'].where(own)
        d['sugar_lab'] = d['sugar_g'].where(own)
        d['soda_reg_g'] = d['grams'].where(d['product_module_code'] == SODA_REG, 0.0)
        d['soda_diet_g'] = d['grams'].where(d['product_module_code'] == SODA_DIET, 0.0)
        h = d.groupby('household_code').agg(grams=('grams', 'sum'), kcal=('kcal', 'sum'), sugar_g=('sugar_g', 'sum'), addsug_g=('addsug_g', 'sum'),
                                            kcal_lab=('kcal_lab', 'sum'), sugar_lab=('sugar_lab', 'sum'), spend=('total_price_paid', 'sum'),
                                            soda_reg_g=('soda_reg_g', 'sum'), soda_diet_g=('soda_diet_g', 'sum')).reset_index()
        h['panel_year'] = y
        hhs.append(h)
        vals = ['grams', 'kcal', 'sugar_g', 'addsug_g', 'kcal_lab', 'sugar_lab']
        for c in vals:
            d[c] = d[c] * w
        d['spend'] = d['total_price_paid'] * w
        m = d.groupby(['product_module_code', 'product_module'])[vals + ['spend']].sum().reset_index()
        m['n_rows'] = d.groupby(['product_module_code', 'product_module'])['upc'].size().values
        m['panel_year'] = y
        mods.append(m)
        print(f'{y}: {len(d):,} rows; sugar per 1,000 kcal {1000 * d.sugar_g.sum() / d.kcal.sum():.1f} ({time.time() - t0:.0f}s)', flush=True)
        del d, h, m, w; gc.collect()
    pd.concat(mods, ignore_index=True).to_parquet(OUT / 'sugar_module_year.parquet', index=False)
    pd.concat(hhs, ignore_index=True).to_parquet(OUT / 'sugar_hh_year.parquet', index=False)
    print(f'saved {OUT} ({time.time() - t0:.0f}s)')


if __name__ == '__main__':
    main()
