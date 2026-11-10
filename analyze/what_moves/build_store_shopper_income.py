"""Neighborhood income of each RMS store, from the home ZIP codes of the panelists who shop there.

For Anya's 2026-11-10 request (access to healthy food by neighborhood income, after Allcott, Diamond and Dube 2019,
Figure II): their store file had 5-digit ZIP codes, ours has 3-digit ZIP and county only. Consumer-panel trips carry the
RMS store code, so a store gets the income of the ZIP codes its panel shoppers live in.
Reads interim/purchases_food/panel_year=<y>/ (2006-2020; trip_code_uc, store_code_uc, household_code; store codes above 0),
interim/panelists/panelists_all_years.parquet (panelist_zip_code by household and year) and
raw/acs/acs5_2015_zcta_median_hh_income.csv (ACS 2011-2015 median household income by ZIP code tabulation area).
Per store, over all its trips: n_trips, n_households, zip_income = the trip-weighted mean of the shoppers' ZIP median
household income (2015 dollars), zip_income_share = share of trips whose home ZIP has an income.
Output: interim/what_moves/store_shopper_income.parquet.
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

OUT = idir / 'what_moves' / 'store_shopper_income.parquet'
YEARS = range(2006, 2021)


def main():
    t0 = time.time()
    z = pd.read_csv(rdir / 'acs' / 'acs5_2015_zcta_median_hh_income.csv', dtype={'zcta': str})
    z = z.dropna(subset=['median_hh_income']).set_index(z['zcta'].astype(int)[z['median_hh_income'].notna()])['median_hh_income']
    pan = pd.read_parquet(idir / 'panelists' / 'panelists_all_years.parquet', columns=['household_code', 'panel_year', 'panelist_zip_code'])
    pan['zip'] = pd.to_numeric(pan['panelist_zip_code'], errors='coerce')
    pan['zip_income'] = pan['zip'].map(z)
    parts = []
    for y in YEARS:
        d = pd.read_parquet(idir / 'purchases_food' / f'panel_year={y}', columns=['trip_code_uc', 'store_code_uc', 'household_code'])
        d = d[d['store_code_uc'] > 0].drop_duplicates('trip_code_uc')
        d = d.merge(pan.loc[pan['panel_year'] == y, ['household_code', 'zip_income']], on='household_code', how='left')
        g = d.groupby('store_code_uc').agg(n_trips=('trip_code_uc', 'size'), n_inc=('zip_income', 'count'), inc_sum=('zip_income', 'sum'))
        hh = d[['store_code_uc', 'household_code']].drop_duplicates()
        parts.append((g, hh))
        print(f'{y}: {len(d):,} trips at {len(g):,} RMS stores ({time.time() - t0:.0f}s)', flush=True)
    g = pd.concat([p[0] for p in parts]).groupby(level=0).sum()
    hh = pd.concat([p[1] for p in parts]).drop_duplicates().groupby('store_code_uc').size().rename('n_households')
    out = g.join(hh)
    out['zip_income'] = out['inc_sum'] / out['n_inc']
    out['zip_income_share'] = out['n_inc'] / out['n_trips']
    out = out.drop(columns=['inc_sum', 'n_inc']).reset_index()
    out.to_parquet(OUT, index=False)
    print(f'saved {len(out):,} stores to {OUT}; with 50+ trips and 10+ households: '
          f'{int(((out.n_trips >= 50) & (out.n_households >= 10)).sum()):,}')
    print(out[['n_trips', 'n_households', 'zip_income', 'zip_income_share']].describe().round(1).to_string())


if __name__ == '__main__':
    main()
