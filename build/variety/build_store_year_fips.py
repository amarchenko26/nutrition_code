"""County of every RMS store in every year it sells food -- run locally, then copy the output to Oscar.

build_product_variety.py used to map stores to counties from the 2006 stores file only (38,285 of the 60,327 RMS stores), so the
barcode x county files left out the stores that entered later (67-69% of RMS food spending kept in 2018-2020). The county of each
store-year comes from that year's stores file, which build_store_choice_sets_rms.py already carries into
interim/what_moves/store_choice_sets.parquet (store_code_uc, year, fips = state x 1000 + county).

A store-year without a county (51 of 561,862) takes the county of the same store in its nearest year with one.

Output: interim/rms_variety/store_year_fips.parquet (store int32, year int16, fips str of 5 digits).
Copy to /users/amarche4/data/rms_variety/ for build_upc_fips_spending_rms.py.
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

SRC = idir / 'what_moves' / 'store_choice_sets.parquet'
OUT = idir / 'rms_variety' / 'store_year_fips.parquet'

d = pd.read_parquet(SRC, columns=['store_code_uc', 'year', 'fips'])
d = d.dropna(subset=['store_code_uc']).drop_duplicates(['store_code_uc', 'year'])
d['store'] = d['store_code_uc'].astype(np.int32)
n_missing = int(d['fips'].isna().sum())

# nearest year of the same store with a county
have = d.dropna(subset=['fips'])
fill = d[d['fips'].isna()][['store', 'year']].merge(have[['store', 'year', 'fips']], on='store', suffixes=('', '_o'))
fill['gap'] = (fill['year'] - fill['year_o']).abs()
fill = fill.sort_values(['store', 'year', 'gap', 'year_o']).drop_duplicates(['store', 'year'])
d = d.merge(fill[['store', 'year', 'fips']].rename(columns={'fips': 'fips_fill'}), on=['store', 'year'], how='left')
d['fips'] = d['fips'].fillna(d['fips_fill'])
left = int(d['fips'].isna().sum())
d = d.dropna(subset=['fips'])

out = pd.DataFrame({'store': d['store'].values, 'year': d['year'].astype(np.int16).values,
                    'fips': d['fips'].astype(int).astype(str).str.zfill(5).values})
out = out.sort_values(['year', 'store']).reset_index(drop=True)
OUT.parent.mkdir(parents=True, exist_ok=True)
out.to_parquet(OUT, index=False)

moved = out.groupby('store')['fips'].nunique()
print(f'{len(out):,} store-years, {out.store.nunique():,} stores, {out.fips.nunique():,} counties')
print(f'store-years without a county in their own stores file: {n_missing}; filled from another year: {n_missing - left}; dropped: {left}')
print(f'stores with more than one county over the years: {(moved > 1).sum():,}')
print('stores by year:', out.groupby('year')['store'].size().to_dict())
print(f'saved {OUT}')
