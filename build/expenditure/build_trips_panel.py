"""
Build HH-year grocery trips panel.

Counts unique shopping trips per household per year from Nielsen Consumer Panel.
A trip is identified by trip_code_uc.

Output columns:
  household_code, panel_year
  n_trips          -- number of unique shopping trips per year
  avg_spend_trip   -- average spending per trip (real 2013$)

Saved to:
  interim/panel_dataset/trips_hh_year.parquet
  interim/panel_dataset/trips_hh_year.dta
"""

import pandas as pd
import numpy as np
from pathlib import Path
import gc
import warnings
warnings.filterwarnings('ignore')

import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
BASE      = data
PURCHASES = BASE / 'interim' / 'purchases_food'
OUT_DIR   = BASE / 'interim' / 'panel_dataset'
OUT_DIR.mkdir(parents=True, exist_ok=True)

YEARS = range(2004, 2021)

print("Building household-year grocery trips panel...")
frames = []

for year in YEARS:
    part = PURCHASES / f'panel_year={year}'
    if not part.exists():
        print(f"  {year}: no data")
        continue

    # Load columns needed for trip counting
    load_cols = ['household_code', 'trip_code_uc', 'total_spent_real_2013']
    p = pd.read_parquet(part, columns=load_cols)
    p = p.dropna(subset=['household_code', 'trip_code_uc', 'total_spent_real_2013'])

    # Aggregate within-trip spending (in case same trip has multiple items)
    trip_spending = p.groupby(['household_code', 'trip_code_uc'])['total_spent_real_2013'].sum().reset_index()

    # Aggregate to household-year level
    agg = trip_spending.groupby('household_code').agg({
        'trip_code_uc': 'nunique',           # number of unique trips
        'total_spent_real_2013': 'mean'      # average spending per trip
    }).reset_index()

    # Flatten column names
    agg.columns = ['household_code', 'n_trips', 'avg_spend_trip']
    agg['panel_year'] = year

    frames.append(agg)
    print(f"  {year}: {len(agg):,} HHs, avg {agg['n_trips'].mean():.0f} trips/yr")
    del p, trip_spending, agg
    gc.collect()

# Combine and save
combined = pd.concat(frames, ignore_index=True)
print(f"\nCombined: {len(combined):,} HH-year obs")
print(f"Columns: {list(combined.columns)}")

out_pq = OUT_DIR / 'trips_hh_year.parquet'
out_dta = OUT_DIR / 'trips_hh_year.dta'
combined.to_parquet(out_pq, index=False)
combined.to_stata(str(out_dta), write_index=False)
print(f"Saved: {out_pq}")
print(f"Saved: {out_dta}")
