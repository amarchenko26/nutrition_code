"""
build_upc_first_year_county.py  --  run on OSCAR (before build_innovation_reg_data.py)

Builds upc_first_year_county.parquet:
  upc, product_module_code, fips, first_year_national, first_year_county

  first_year_national = first year UPC appears in ANY county
  first_year_county   = first year UPC appears in THIS county

Used by build_innovation_reg_data.py to count county-level new UPCs.

Transfer output back with:
  scp amarche4@oscar.ccv.brown.edu:/users/amarche4/data/rms_variety/upc_first_year_county.parquet .
"""

import pandas as pd
from pathlib import Path
import gc

RMS_VAR = Path('/users/amarche4/data/rms_variety')
YEARS   = list(range(2007, 2021))

def log(msg):
    print(msg, flush=True)

log("Building UPC first-year by county...")

chunks = []
for yr in YEARS:
    path = RMS_VAR / f'rms_upc_fips_spending_{yr}.parquet'
    if not path.exists():
        log(f"  {yr}: not found, skipping")
        continue
    df = pd.read_parquet(path, columns=['upc', 'product_module_code', 'fips'])
    df = df.drop_duplicates(['upc', 'product_module_code', 'fips'])
    df['year'] = yr
    chunks.append(df)
    log(f"  {yr}: {len(df):,} unique upc-module-fips rows")
    gc.collect()

log("\nConcatenating all years...")
all_df = pd.concat(chunks, ignore_index=True)
del chunks; gc.collect()
log(f"  Total: {len(all_df):,} rows across {all_df['fips'].nunique()} counties")

log("Computing first_year_county (min year per upc-module-fips)...")
first_county = (all_df.groupby(['upc', 'product_module_code', 'fips'])['year']
                .min().rename('first_year_county').reset_index())

log("Computing first_year_national (min year per upc-module)...")
first_national = (all_df.groupby(['upc', 'product_module_code'])['year']
                  .min().rename('first_year_national').reset_index())

del all_df; gc.collect()

out = first_county.merge(first_national, on=['upc', 'product_module_code'], how='left')
del first_county, first_national; gc.collect()

log(f"\nOutput: {len(out):,} upc-module-fips rows")
log(f"  {out['fips'].nunique()} counties, {out['product_module_code'].nunique()} modules")

out_path = RMS_VAR / 'upc_first_year_county.parquet'
out.to_parquet(out_path, index=False)
log(f"Saved: {out_path}")
log("\nNext: run build_innovation_reg_data.py")
