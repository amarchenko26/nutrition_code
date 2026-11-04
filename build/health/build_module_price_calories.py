"""Product module x year: spending, grams, calories and price per calorie from the consumer panel.

For the time-cost versus money-cost figures in notes/obesity.tex (analyze/health/figure_time_vs_money.py).
Reads interim/purchases_food/panel_year=<y>/ (2004-2020, MAGNET rows dropped) and
interim/usda_nielsen_merged/usda_wide.parquet (calories per 100 g) and usda_nova.parquet (NOVA group).
Per module and year: spend_real (total_price_paid_real_2013), spend_nominal, grams (rows with a
weight or volume size; quantity x multi x size since 2026-11-04), spend_grams (spending on those
rows), cals_matched and grams_matched (rows with their own USDA label), cal_per_100g =
cals_matched / grams_matched x 100 (the module's own-label density that year), cals_est = the sum
over rows of grams x the UPC's density from interim/health/upc_dictionary.parquet
(build_upc_dictionary.py: own label, else nearest-sized labelled UPC of the same brand and module,
else brand x module mean, else pooled module density; fixed over time — until 2026-11-04 cals_est
used the module x year own-label density), spend_nova4 and spend_nova_any (spending on rows with a
NOVA group, and on group 4), n_upcs, n_households.
Output: interim/health/module_year_price_calories.parquet
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
OUT = idir / 'health' / 'module_year_price_calories.parquet'
YEARS = range(2004, 2021)
UNIT_TO_GRAMS = {'OZ': 28.3495, 'LB': 453.592, 'FL OZ': 29.5735, 'QT': 946.353, 'GAL': 3785.41,
                 'ML': 1.0, 'LT': 1000.0, 'KG': 1000.0, 'GM': 1.0, 'GR': 1.0}


def main():
    t0 = time.time()
    dic = pd.read_parquet(idir / 'health' / 'upc_dictionary.parquet', columns=['upc_13', 'cal_per_100g', 'cal_tier', 'nova'])
    out = []
    for y in YEARS:
        p = PURCH / f'panel_year={y}'
        if not p.exists():
            continue
        d = pd.read_parquet(p, columns=['upc', 'quantity', 'multi', 'size1_amount', 'size1_units', 'product_module_code', 'product_module', 'product_group',
                                        'department_descr', 'total_price_paid', 'total_price_paid_real_2013', 'household_code'])
        d = d[d['department_descr'] != 'MAGNET DATA']
        d['upc_13'] = '0' + d['upc'].astype(str).str.zfill(12)
        d = d.merge(dic, on='upc_13', how='left')
        d['grams'] = d['quantity'] * d['multi'].fillna(1).clip(lower=1) * d['size1_amount'] * d['size1_units'].map(UNIT_TO_GRAMS)
        d.loc[d['grams'] <= 0, 'grams'] = np.nan
        d['cals_est_row'] = d['grams'] * d['cal_per_100g'] / 100
        d['cals_matched'] = d['cals_est_row'].where(d['cal_tier'] == 1)
        d['grams_matched'] = d['grams'].where(d['cal_tier'] == 1)
        d['spend_grams'] = d['total_price_paid_real_2013'].where(d['grams'].notna(), 0.0)
        d['spend_nova_any'] = d['total_price_paid_real_2013'].where(d['nova'].notna(), 0.0)
        d['spend_nova4'] = d['total_price_paid_real_2013'].where(d['nova'] == 4, 0.0)
        g = d.groupby(['product_module_code']).agg(
            product_module=('product_module', 'first'), product_group=('product_group', 'first'),
            spend_nominal=('total_price_paid', 'sum'), spend_real=('total_price_paid_real_2013', 'sum'), spend_grams=('spend_grams', 'sum'),
            grams=('grams', 'sum'), cals_matched=('cals_matched', 'sum'), grams_matched=('grams_matched', 'sum'), cals_est=('cals_est_row', 'sum'),
            spend_nova_any=('spend_nova_any', 'sum'), spend_nova4=('spend_nova4', 'sum'),
            n_upcs=('upc', 'nunique'), n_households=('household_code', 'nunique'), n_rows=('upc', 'size')).reset_index()
        g['panel_year'] = y
        g['cal_per_100g'] = 100 * g['cals_matched'] / g['grams_matched'].replace(0, np.nan)
        out.append(g)
        print(f'{y}: {len(d):,} rows -> {len(g):,} modules ({time.time() - t0:.0f}s)', flush=True)
        del d; gc.collect()
    df = pd.concat(out, ignore_index=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    print(f'saved {len(df):,} module-years to {OUT}')


if __name__ == '__main__':
    main()
