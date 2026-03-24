"""
build_upc_spending.py  (run on OSCAR)

Aggregates per-year UPC×FIPS parquets into a single UPC-level total spending file.
Used to expenditure-weight module healthiness scores in build_module_healthiness.py.

Output: upc_total_spending.parquet  (upc, product_module_code, total_spending)
"""

import pandas as pd
from pathlib import Path

RMS_VARIETY_DIR = Path('/users/amarche4/data/rms_variety')
OUT_PATH        = RMS_VARIETY_DIR / 'upc_total_spending.parquet'

YEARS = list(range(2006, 2021))

chunks = []
for year in YEARS:
    p = RMS_VARIETY_DIR / f'rms_upc_fips_spending_{year}.parquet'
    if not p.exists():
        print(f"  {year}: missing, skipping")
        continue
    df = pd.read_parquet(p, columns=['upc', 'product_module_code', 'total_spending'])
    chunks.append(df)
    print(f"  {year}: {len(df):,} rows")

combined = pd.concat(chunks, ignore_index=True)
out = combined.groupby(['upc', 'product_module_code'], as_index=False)['total_spending'].sum()
out.to_parquet(OUT_PATH, index=False)
print(f"\nSaved: {OUT_PATH}  ({len(out):,} UPCs)")
