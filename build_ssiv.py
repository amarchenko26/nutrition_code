"""
Build Bartik shift-share IV at the zip × year level.

    SSIV_{z,t} = Σ_o [ share_{z,o,base} × shift_{o,t,-z} ]

  shares  = zip z's projection-factor-weighted occupation share at BASE_YEAR
  shifts  = leave-one-zip-out weighted mean real income for occupation o in year t

Uses the full panelists file (not sampled) so LOO shifts are well-populated.

Cell definition (start with simplest — occupation only):
  occ: white / blue / service / not_emp  (4 bins from Nielsen occ codes)

Output: zip × year panel
  interim/panel_dataset/ssiv_zip_year.parquet / .dta
  Columns: zip_code, panel_year, ssiv_income, avg_income, n_hh
    ssiv_income  -- the Bartik IV (instrument for avg_income)
    avg_income   -- actual weighted mean real income in zip-year ($000s, 2010$)
    n_hh         -- number of HH-year obs in zip-year (use for quality filter)
"""

import pandas as pd
import numpy as np
from pathlib import Path

BASE      = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
PANELISTS = BASE / 'interim' / 'panelists' / 'panelists_all_years.parquet'
OUT_PATH  = BASE / 'interim' / 'panel_dataset' / 'ssiv_zip_year.parquet'

BASE_YEAR = 2004   # year for computing initial occupation shares
MIN_ZIP_N = 10     # minimum HHs in a zip-year to include in output

CPI = {
    2004: 188.9, 2005: 195.3, 2006: 201.6, 2007: 207.3, 2008: 215.3,
    2009: 214.5, 2010: 218.1, 2011: 224.9, 2012: 229.6, 2013: 233.0,
    2014: 236.7, 2015: 237.0, 2016: 240.0, 2017: 245.1, 2018: 251.1,
    2019: 255.7, 2020: 258.8, 2021: 271.0, 2022: 292.7, 2023: 304.7,
    2024: 314.0,
}

OCC_MAP = {
    0:  np.nan,      # No Head
    1:  'white',     2:  'white',    3:  'white',    4:  'white',
    5:  'blue',      6:  'blue',     7:  'blue',
    8:  'service',   9:  'service',  11: 'service',
    10: 'not_emp',   12: 'not_emp',
}

def log(msg):
    print(msg, flush=True)


# ============================================================
# LOAD & PREP
# ============================================================
log("Loading panelists...")
pan = pd.read_parquet(PANELISTS)
log(f"  {len(pan):,} HH-year obs")

cpi_base = CPI[2010]
pan['real_income'] = pan['household_income_midpoint'] * (cpi_base / pan['panel_year'].map(CPI)) / 1000
pan['zip'] = pan['panelist_zip_code'].astype(str).str.zfill(5)
pan['w']   = pd.to_numeric(pan['projection_factor'], errors='coerce')

# Occupation: male head, fall back to female
pan['occ'] = pd.to_numeric(pan.get('male_head_occupation', pd.Series(dtype=float)),
                           errors='coerce').map(OCC_MAP)
if 'female_head_occupation' in pan.columns:
    fem_occ = pd.to_numeric(pan['female_head_occupation'], errors='coerce').map(OCC_MAP)
    pan['occ'] = pan['occ'].where(pan['occ'].notna(), fem_occ)

pan_v = pan.dropna(subset=['real_income', 'zip', 'w', 'occ']).copy()
pan_v = pan_v[pan_v['w'] > 0]
log(f"  {len(pan_v):,} rows with complete data")


# ============================================================
# STEP 1: BASE-YEAR ZIP × OCC SHARES
# ============================================================
log(f"Computing base-year ({BASE_YEAR}) occupation shares by zip...")
base = pan_v[pan_v['panel_year'] == BASE_YEAR].copy()

zip_occ_w = base.groupby(['zip', 'occ'])['w'].sum()
zip_w      = base.groupby('zip')['w'].sum()
shares = (zip_occ_w / zip_w).rename('share').reset_index()
log(f"  {shares['zip'].nunique():,} zips in base year, "
    f"{shares.groupby('zip')['share'].sum().describe()[['mean','min','max']].to_dict()}")


# ============================================================
# STEP 2: LEAVE-ONE-ZIP-OUT INCOME SHIFT BY OCC × YEAR
# ============================================================
log("Computing leave-one-zip-out shifts...")
pan_v['wi'] = pan_v['w'] * pan_v['real_income']

# National totals by occ × year
occ_yr = (pan_v.groupby(['occ', 'panel_year'])[['wi', 'w']]
          .sum().reset_index()
          .rename(columns={'wi': 'tot_wi', 'w': 'tot_w'}))

# Zip contributions by occ × year
zip_occ_yr = (pan_v.groupby(['zip', 'occ', 'panel_year'])[['wi', 'w']]
              .sum().reset_index()
              .rename(columns={'wi': 'zip_wi', 'w': 'zip_w'}))

# LOO shift = (national - zip) / (national_w - zip_w)
shifts = zip_occ_yr.merge(occ_yr, on=['occ', 'panel_year'])
shifts['lo_wi'] = shifts['tot_wi'] - shifts['zip_wi']
shifts['lo_w']  = shifts['tot_w']  - shifts['zip_w']
shifts['shift'] = shifts['lo_wi'] / shifts['lo_w'].replace(0, np.nan)


# ============================================================
# STEP 3: SSIV = Σ_o share_{z,o,base} × shift_{o,t,-z}
# ============================================================
log("Computing SSIV...")
iv_inputs = shares.merge(shifts[['zip', 'occ', 'panel_year', 'shift']],
                         on=['zip', 'occ'], how='inner')
iv_inputs['contribution'] = iv_inputs['share'] * iv_inputs['shift']
ssiv = (iv_inputs.groupby(['zip', 'panel_year'])['contribution']
        .sum().reset_index()
        .rename(columns={'contribution': 'ssiv_income', 'zip': 'zip_code'}))


# ============================================================
# STEP 4: ZIP-YEAR AVERAGE INCOME (endogenous variable)
# ============================================================
avg_inc = (pan_v.groupby(['zip', 'panel_year'])
           .apply(lambda g: pd.Series({
               'avg_income': np.average(g['real_income'], weights=g['w']),
               'n_hh': len(g),
           }))
           .reset_index()
           .rename(columns={'zip': 'zip_code'}))

out = ssiv.merge(avg_inc, on=['zip_code', 'panel_year'], how='inner')
out = out[out['n_hh'] >= MIN_ZIP_N].copy()


# ============================================================
# DIAGNOSTICS
# ============================================================
log(f"\n  {len(out):,} zip-year obs ({out['zip_code'].nunique():,} zips, "
    f"{out['panel_year'].nunique()} years)")
log(f"  SSIV mean={out['ssiv_income'].mean():.2f}, "
    f"std={out['ssiv_income'].std():.2f}")
log(f"  Corr(ssiv, avg_income): "
    f"{out[['ssiv_income','avg_income']].corr().iloc[0,1]:.3f}")


# ============================================================
# SAVE
# ============================================================
out.to_parquet(OUT_PATH, index=False)
out.to_stata(str(OUT_PATH).replace('.parquet', '.dta'), version=118)
log(f"\nSaved: {OUT_PATH}")
log(f"Columns: {list(out.columns)}")
log("Done.")
