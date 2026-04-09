"""
build_county_income_shock.py  --  run LOCALLY

Builds county x year income panel + leave-one-out (LO) instrument.

Leave-one-out instrument for county c:
  For each demographic cell (educ x occup x hh_size), compute the
  projection-weighted mean income of that cell in all OTHER counties.
  County c's predicted income = Σ_cell (cell share in c) x (LO cell income).

This is analogous to Part 1's HH-level instrument, lifted to the county level.

Output: county_income_shocks.parquet
  fips, year, income_raw, income_hat, d_log_income, d_log_income_hat

Transfer to OSCAR: scp county_income_shocks.parquet amarche4@oscar.ccv.brown.edu:/users/amarche4/data/rms_variety/
"""

import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/')
OUT = BASE / Path('panel_dataset/county_income_shocks.parquet')

MIN_HH_PER_COUNTY = 5   # drop county-years with fewer HHs
MIN_CELL_N        = 30  # minimum leave-out cell size (same as build_iv.py)

# Cell bin definitions — identical to build_iv.py
SIZE_BINS   = [0, 1, 2, 4, np.inf]
SIZE_LABELS = ['1', '2', '3-4', '5+']

EDUC_BINS   = [0, 11.9, 12.9, 15.9, np.inf]
EDUC_LABELS = ['lt_hs', 'hs', 'some_col', 'col_plus']

OCC_MAP = {
    0: np.nan, 1: 'white', 2: 'white',   3: 'white',   4: 'white',
    5: 'blue',  6: 'blue',  7: 'blue',
    8: 'service', 9: 'service', 11: 'service',
    10: 'not_emp', 12: 'not_emp',
}

# ============================================================
# LOAD PANELISTS + INCOME
# ============================================================
print("Loading panelists...")
pan = pd.read_parquet(BASE / 'panelists/panelists_all_years.parquet',
                      columns=['household_code', 'panel_year',
                               'fips_state_code', 'fips_county_code',
                               'projection_factor',
                               'hh_avg_yrsofschool',
                               'male_head_occupation', 'female_head_occupation',
                               'household_size'])
pan = pan.rename(columns={'panel_year': 'year'})
pan = pan[(pan['year'] >= 2004) & (pan['year'] <= 2020)]
pan['fips'] = (pan['fips_state_code'].astype(str).str.extract(r'(\d+)')[0].str.zfill(2) +
               pan['fips_county_code'].astype(str).str.extract(r'(\d+)')[0].str.zfill(3))

hhy = pd.read_parquet(BASE / 'panel_dataset/panel_hh_year.parquet',
                      columns=['household_code', 'panel_year', 'real_income'])
hhy = hhy.rename(columns={'panel_year': 'year'})

df = (pan.merge(hhy, on=['household_code', 'year'], how='inner')
         .dropna(subset=['real_income', 'projection_factor', 'fips']))
df = df[df['real_income'] > 0]
print(f"  {len(df):,} HH-year obs")

# ============================================================
# DEMOGRAPHIC CELLS — same bins as build_iv.py
# ============================================================
df['size_bin'] = pd.cut(pd.to_numeric(df['household_size'], errors='coerce'),
                         bins=SIZE_BINS, labels=SIZE_LABELS)

df['educ_bin'] = pd.cut(pd.to_numeric(df['hh_avg_yrsofschool'], errors='coerce'),
                         bins=EDUC_BINS, labels=EDUC_LABELS)

# Occupation: male head, fall back to female (same logic as build_iv.py)
df['occ_bin'] = pd.to_numeric(df['male_head_occupation'], errors='coerce').map(OCC_MAP)
fem_occ = pd.to_numeric(df['female_head_occupation'], errors='coerce').map(OCC_MAP)
df['occ_bin'] = df['occ_bin'].where(df['occ_bin'].notna(), fem_occ)

df = df.dropna(subset=['size_bin', 'educ_bin', 'occ_bin'])
# cell includes year (same as build_iv.py line 141)
df['cell'] = (df['year'].astype(str) + '|' +
              df['size_bin'].astype(str) + '|' +
              df['educ_bin'].astype(str) + '|' +
              df['occ_bin'].astype(str))
print(f"  {len(df):,} rows with complete cell data")

# ============================================================
# COUNTY-YEAR RAW INCOME (projection-weighted)
# ============================================================
print("Computing county-year income...")

def wavg(g):
    return np.average(g['real_income'], weights=g['projection_factor'])

county_raw = (df.groupby(['fips', 'year'])
              .apply(wavg)
              .rename('income_raw').reset_index())

# Require minimum HH count
hh_count = df.groupby(['fips', 'year']).size().rename('n_hh').reset_index()
county_raw = county_raw.merge(hh_count, on=['fips', 'year'])
county_raw = county_raw[county_raw['n_hh'] >= MIN_HH_PER_COUNTY]
print(f"  {county_raw['fips'].nunique()} counties, {len(county_raw):,} county-year obs")

# ============================================================
# LEAVE-ONE-OUT INSTRUMENT
# ============================================================
print("Computing leave-one-out instrument...")

# Step 1: cell x year totals (across all counties)
#   numerator = Σ_{hh in cell,year} (projection_factor x real_income)
#   denominator = Σ_{hh in cell,year} projection_factor
df['wt_income'] = df['projection_factor'] * df['real_income']

cell_yr = (df.groupby(['cell', 'year'])
           .agg(tot_wt_income=('wt_income', 'sum'),
                tot_wt=('projection_factor', 'sum'))
           .reset_index())

# Step 2: cell x county x year totals (for leave-one-out subtraction)
cell_cty_yr = (df.groupby(['cell', 'fips', 'year'])
               .agg(cty_wt_income=('wt_income', 'sum'),
                    cty_wt=('projection_factor', 'sum'))
               .reset_index())

# Step 3: leave-one-out cell income for each (fips, cell, year)
#   LO_income_{cell,c,t} = (tot_wt_income - cty_wt_income) / (tot_wt - cty_wt)
lo = cell_cty_yr.merge(cell_yr, on=['cell', 'year'])
lo['lo_wt']        = lo['tot_wt']        - lo['cty_wt']
lo['lo_wt_income'] = lo['tot_wt_income'] - lo['cty_wt_income']
lo = lo[lo['lo_wt'] > 0]
lo['lo_income'] = lo['lo_wt_income'] / lo['lo_wt']

# Step 4: county c's predicted income = Σ_cell (cell share in c) x LO_income
#   cell share = total projection weight of cell in county / total county weight
cty_tot_wt = df.groupby(['fips', 'year'])['projection_factor'].sum().rename('cty_tot_wt').reset_index()
lo = lo.merge(cty_tot_wt, on=['fips', 'year'])
lo['cell_share'] = lo['cty_wt'] / lo['cty_tot_wt']   # cell's share of county weight

lo['hat_contribution'] = lo['cell_share'] * lo['lo_income']
county_hat = (lo.groupby(['fips', 'year'])['hat_contribution']
              .sum().rename('income_hat').reset_index())

# ============================================================
# ASSEMBLE + COMPUTE LOG CHANGES
# ============================================================
out = county_raw[['fips', 'year', 'income_raw']].merge(
      county_hat, on=['fips', 'year'], how='inner')

out = out.sort_values(['fips', 'year'])
out['log_income']     = np.log(out['income_raw'])
out['log_income_hat'] = np.log(out['income_hat'])

# First differences (year-on-year log change)
out['d_log_income']     = out.groupby('fips')['log_income'].diff()
out['d_log_income_hat'] = out.groupby('fips')['log_income_hat'].diff()

out = out.dropna(subset=['d_log_income', 'd_log_income_hat'])
print(f"  Output: {len(out):,} county-year obs with valid income changes")
print(f"  First stage corr(d_log_income, d_log_income_hat): "
      f"{out['d_log_income'].corr(out['d_log_income_hat']):.3f}")

out.to_parquet(OUT, index=False)
print(f"\nSaved: {OUT}")
print("Next: scp county_income_shocks.parquet amarche4@oscar.ccv.brown.edu:/users/amarche4/data/rms_variety/")
