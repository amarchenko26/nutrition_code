"""Collapse the household purchase files to UPC x panel year.

Reads interim/purchases_food/panel_year=<y>/ (one row per trip-UPC) for 2004-2020 and writes
interim/health/upc_year_spending.parquet: upc, panel_year, product_module_code,
product_module_descr, spending (total_price_paid), quantity, n_households, n_trips.
A UPC counts as sold in a year if at least one panelist bought it. Used by
analyze/health/figure_variety_and_upf.py for UPC counts by category and by
ultra-processed status over time.
"""
import os, time
import pandas as pd

BASE = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim'
OUT_DIR = f'{BASE}/health'
os.makedirs(OUT_DIR, exist_ok=True)
YEARS = range(2004, 2021)

parts = []
t0 = time.time()
for y in YEARS:
    d = f'{BASE}/purchases_food/panel_year={y}'
    df = pd.read_parquet(d, columns=['upc', 'product_module_code', 'product_module', 'total_price_paid', 'quantity', 'household_code', 'trip_code_uc'])
    g = (df.groupby(['upc', 'product_module_code'], as_index=False)
           .agg(product_module_descr=('product_module', 'first'), spending=('total_price_paid', 'sum'),
                quantity=('quantity', 'sum'), n_households=('household_code', 'nunique'), n_trips=('trip_code_uc', 'size')))
    g['panel_year'] = y
    parts.append(g)
    print(f'{y}: {len(df):,} trip-UPC rows -> {len(g):,} UPCs ({time.time() - t0:.0f}s)', flush=True)
    del df
out = pd.concat(parts, ignore_index=True)
out['upc'] = out['upc'].astype(str)
out.to_parquet(f'{OUT_DIR}/upc_year_spending.parquet', index=False)
print(f'saved {len(out):,} UPC-years to {OUT_DIR}/upc_year_spending.parquet')
