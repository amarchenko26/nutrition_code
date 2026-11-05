"""Household-year calories by NOVA group, produce calories, grams bought and adult equivalents.

For notes/obesity.tex (calories per person over time; where the calories come from; who eats
ultra-processed food). Reads interim/purchases_food/panel_year=<y>/ (one row per trip-UPC,
2004-2020, MAGNET rows dropped), interim/usda_nielsen_merged/usda_wide.parquet (calories per
100 g from the USDA Branded label) and usda_nova.parquet (NOVA group 1-4 from the ingredient
list, clean/usda/build_usda_nova.py), and interim/panelists/panelists_all_years.parquet.

Per purchase row: grams = quantity x utils.food_measures.package_grams (size x multi x unit, pounds and
count-sized items included, powdered mixes by grams of powder per prepared quart: the rules of the Allcott et al.
replication, 2026-11-05); calories = grams x the UPC's calorie density per 100 g from
interim/health/upc_dictionary.parquet (own USDA label, else the product's nearest labelled relatives, else the
module or a typed commodity value; fixed over time, so the calorie total does not move with the label match rate,
which rises from 39% of calories in 2004 to 81% in 2020 because the USDA file is a 2024 snapshot). NOVA groups are
fixed over time the same way. Produce = the fruit and vegetable module lists of Allcott et al.

Per household-year: grams_all; cals_matched (tier-1 rows); cals_all (all tiers); cals_tier2,
cals_tier3, cals_tier4; cals_nova1..4 (all rows); cals_ownnova1..4 (rows with their own ingredient list); cals_nova_none;
produce_cals_matched, produce_cals_all; spend_all, spend_matched, spend_nova1..4;
ae_total = USDA Dietary Guidelines calorie need of each member (moderately active, age x
sex) / 2,000, summed over the roster (as in analyze/hi/usda/figure_calories_by_income.py),
with ae_complete = 1 when the roster count equals household_size.
Output: interim/health/nova_calorie_panel.parquet
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
from utils.food_measures import package_grams, is_fruit, is_veg, adult_equivalents

PURCH = idir / 'purchases_food'
DICT = idir / 'health' / 'upc_dictionary.parquet'
OUT = idir / 'health' / 'nova_calorie_panel.parquet'
YEARS = range(2004, 2021)


def main():
    t0 = time.time()
    dic = pd.read_parquet(DICT, columns=['upc_13', 'cal_per_100g', 'cal_tier', 'nova', 'nova_own']).dropna(subset=['cal_per_100g'])
    print(f'UPC dictionary: {len(dic):,} UPCs with a density, {dic.nova.notna().sum():,} with a NOVA group')
    out = []
    for y in YEARS:
        p = PURCH / f'panel_year={y}'
        if not p.exists():
            continue
        d = pd.read_parquet(p, columns=['upc', 'quantity', 'multi', 'size1_amount', 'size1_units', 'product_module_code', 'product_group_code', 'product_group',
                                        'product_module', 'department_descr', 'household_code', 'total_price_paid'])
        d = d[d['department_descr'] != 'MAGNET DATA']
        d['upc_13'] = '0' + d['upc'].astype(str).str.zfill(12)
        d = d.merge(dic, on='upc_13', how='left')
        d['grams'] = d['quantity'] * package_grams(d)
        d['cals_all'] = d['grams'] * d['cal_per_100g'] / 100
        d['cals_matched'] = d['cals_all'].where(d['cal_tier'] == 1)
        for t in (2, 3, 4):
            d[f'cals_tier{t}'] = d['cals_all'].where(d['cal_tier'] == t, 0.0)
        prod = is_fruit(d['product_module_code'], d['product_group_code']) | is_veg(d['product_module_code'], d['product_group_code'])
        d['produce_cals_matched'] = d['cals_matched'].where(prod, 0.0)
        d['produce_cals_all'] = d['cals_all'].where(prod, 0.0)
        # cals_nova<g>: all rows with a NOVA group, own ingredient list or the nearest labelled relative (fixed over time);
        # cals_ownnova<g>: rows with their own ingredient list only (the measure until 2026-11-05)
        for g in (1, 2, 3, 4):
            d[f'cals_nova{g}'] = d['cals_all'].where(d['nova'] == g, 0.0)
            d[f'cals_ownnova{g}'] = d['cals_matched'].where(d['nova_own'] == g, 0.0)
            d[f'spend_nova{g}'] = d['total_price_paid'].where(d['nova'] == g, 0.0)
        d['cals_nova_none'] = d['cals_all'].where(d['cals_all'].notna() & d['nova'].isna(), 0.0)
        d['spend_matched'] = d['total_price_paid'].where(d['cals_matched'].notna(), 0.0)
        d['spend_nova_any'] = d['total_price_paid'].where(d['nova'].notna(), 0.0)
        agg = d.groupby('household_code').agg(
            grams_all=('grams', 'sum'), cals_matched=('cals_matched', 'sum'), cals_all=('cals_all', 'sum'),
            cals_tier2=('cals_tier2', 'sum'), cals_tier3=('cals_tier3', 'sum'), cals_tier4=('cals_tier4', 'sum'),
            cals_nova1=('cals_nova1', 'sum'), cals_nova2=('cals_nova2', 'sum'), cals_nova3=('cals_nova3', 'sum'), cals_nova4=('cals_nova4', 'sum'),
            cals_ownnova1=('cals_ownnova1', 'sum'), cals_ownnova2=('cals_ownnova2', 'sum'), cals_ownnova3=('cals_ownnova3', 'sum'), cals_ownnova4=('cals_ownnova4', 'sum'),
            cals_nova_none=('cals_nova_none', 'sum'), produce_cals_matched=('produce_cals_matched', 'sum'), produce_cals_all=('produce_cals_all', 'sum'),
            spend_all=('total_price_paid', 'sum'), spend_matched=('spend_matched', 'sum'), spend_nova_any=('spend_nova_any', 'sum'),
            spend_nova1=('spend_nova1', 'sum'), spend_nova2=('spend_nova2', 'sum'), spend_nova3=('spend_nova3', 'sum'), spend_nova4=('spend_nova4', 'sum'),
            n_rows=('upc', 'size')).reset_index()
        agg['panel_year'] = y
        out.append(agg)
        tot = d['cals_all'].sum()
        print(f'{y}: {len(d):,} rows; grams on {d.grams.notna().mean():.3f}; calories by tier: own label {d.cals_matched.sum() / tot:.3f}, '
              f'brand-size {d.cals_tier2.sum() / tot:.3f}, brand-module {d.cals_tier3.sum() / tot:.3f}, module {d.cals_tier4.sum() / tot:.3f}; '
              f'NOVA on {d.nova.notna().mean():.3f} of rows; {len(agg):,} households ({time.time() - t0:.0f}s)', flush=True)
        del d; gc.collect()
    hh = pd.concat(out, ignore_index=True)
    hh = hh.merge(adult_equivalents(), on=['household_code', 'panel_year'], how='left')
    OUT.parent.mkdir(parents=True, exist_ok=True)
    hh.to_parquet(OUT, index=False)
    print(f'saved {len(hh):,} household-years to {OUT} ({time.time() - t0:.0f}s)')


if __name__ == '__main__':
    main()
