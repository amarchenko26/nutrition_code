"""
Build leave-one-out income IV for each household-year.

Builds two IVs:
  iv_income_zip  -- leave-one-zip-out (excludes own zip code)
  iv_income_fips -- leave-one-county-out (excludes own 5-digit FIPS county)

Cell definition (coarsened to keep cells populated):
  - household_size_bin: 1, 2, 3-4, 5+
  - educ_bin:           <HS, HS grad, Some college, College+
  - occ_bin:            White collar, Blue collar, Service/Other, Not employed
  use male if available otherwise fall back to female

Output:
  interim/panel_dataset/iv_income.parquet / .dta
  Columns: household_code, panel_year,
           iv_income_zip, iv_cell_n_lo_zip, cell_zip_share,
           iv_income_fips, iv_cell_n_lo_fips, cell_fips_share
"""

import pandas as pd
import numpy as np
from pathlib import Path

BASE      = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
PANELISTS = BASE / 'interim' / 'panelists' / 'panelists_all_years.parquet'
OUT_PATH  = BASE / 'interim' / 'panel_dataset' / 'iv_income.parquet'

# Minimum leave-out cell size — drop IV for HHs whose cell has fewer
# than this many leave-out observations (too noisy to be useful).
MIN_CELL_N = 30

# ============================================================
# CELL BIN DEFINITIONS
# ============================================================

# Household size
SIZE_BINS   = [0, 1, 2, 4, np.inf]
SIZE_LABELS = ['1', '2', '3-4', '5+']

# Education (years of schooling from hh_avg_yrsofschool)
EDUC_BINS   = [0, 11.9, 12.9, 15.9, np.inf]
EDUC_LABELS = ['lt_hs', 'hs', 'some_col', 'col_plus']

# Occupation bins (Nielsen codes — see codebook)
# White collar: Professional/Technical (1), Manager/Admin (2), Sales (3), Clerical (4)
# Blue collar:  Craftsman (5), Operative (6), Laborer (7)
# Service/Other: Service (8), Farmer (9), Student (11), Other
# Not employed: Not Employed for Pay (10 or 12), No Head (0) → NaN
OCC_MAP = {
    0:  np.nan,       # No Head
    1:  'white',      # Professional/Technical
    2:  'white',      # Manager/Admin
    3:  'white',      # Sales
    4:  'white',      # Clerical
    5:  'blue',       # Craftsman/Foreman
    6:  'blue',       # Operative
    7:  'blue',       # Laborer
    8:  'service',    # Service
    9:  'service',    # Farmer/Farm Labor
    10: 'not_emp',    # Not Employed (some years use code 10)
    11: 'service',    # Student
    12: 'not_emp',    # Not Employed for Pay
}


def bin_occupation(pan):
    """Pick the head's occupation: male if present, else female."""
    occ = pd.to_numeric(pan.get('male_head_occupation', pd.Series(dtype=float)),
                        errors='coerce').map(OCC_MAP)
    if 'female_head_occupation' in pan.columns:
        female_occ = pd.to_numeric(pan['female_head_occupation'],
                                   errors='coerce').map(OCC_MAP)
        # Use female if male is missing
        occ = occ.where(occ.notna(), female_occ)
    return occ


def leave_one_out_iv(pan_v, geo_col):
    """
    Compute leave-one-geo-out IV.
    Returns DataFrame with household_code, panel_year, iv_income, iv_cell_n_lo, cell_geo_share.
    """
    cell_tot = (pan_v.groupby('cell')[['wi', 'w']]
                .sum()
                .rename(columns={'wi': 'cell_wi', 'w': 'cell_w'}))
    cell_geo_tot = (pan_v.groupby(['cell', geo_col])[['wi', 'w']]
                    .sum()
                    .rename(columns={'wi': 'cz_wi', 'w': 'cz_w'})
                    .reset_index())

    pv = pan_v[['household_code', 'panel_year', 'cell', geo_col, 'w', 'wi']].copy()
    pv = pv.merge(cell_tot, on='cell', how='left')
    pv = pv.merge(cell_geo_tot, on=['cell', geo_col], how='left')

    lo_wi = pv['cell_wi'] - pv['cz_wi']
    lo_w  = pv['cell_w']  - pv['cz_w']
    pv['iv_income']    = lo_wi / lo_w.replace(0, np.nan)
    pv['iv_cell_n_lo'] = lo_w
    pv.loc[pv['iv_cell_n_lo'] < MIN_CELL_N, 'iv_income'] = np.nan

    geo_w_tot = (pan_v.groupby([geo_col, 'panel_year'])['w']
                 .sum().rename('geo_w_tot').reset_index())
    pv = pv.merge(geo_w_tot, on=[geo_col, 'panel_year'], how='left')
    pv['cell_geo_share'] = pv['cz_w'] / pv['geo_w_tot']

    return pv[['household_code', 'panel_year', 'iv_income', 'iv_cell_n_lo', 'cell_geo_share']]


def main():
    print("Loading panelists...")
    pan = pd.read_parquet(PANELISTS)
    print(f"  {len(pan):,} HH-year obs")

    # --------------------------------------------------------
    # Prep variables
    # --------------------------------------------------------
    pan['inc']  = pd.to_numeric(pan['household_income_midpoint'], errors='coerce') / 1000
    pan['zip']  = pan['panelist_zip_code'].astype(str).str.zfill(5)
    pan['fips'] = (pan['fips_state_code'].astype(str).str.zfill(2) +
                   pan['fips_county_code'].astype(str).str.zfill(3))
    pan['w']    = pd.to_numeric(pan['projection_factor'], errors='coerce')

    # Education bin
    if 'hh_avg_yrsofschool' in pan.columns:
        educ_src = pan['hh_avg_yrsofschool']
    else:
        educ_src = pd.to_numeric(pan['male_head_education'], errors='coerce').map(
            {0: np.nan, 1: 6, 2: 10, 3: 12, 4: 14, 5: 16, 6: 18})
    pan['educ_bin'] = pd.cut(educ_src, bins=EDUC_BINS, labels=EDUC_LABELS)

    # Household size bin
    pan['size_bin'] = pd.cut(
        pd.to_numeric(pan['household_size'], errors='coerce'),
        bins=SIZE_BINS, labels=SIZE_LABELS)

    # Occupation bin
    pan['occ_bin'] = bin_occupation(pan)

    # Cell identifier
    pan['cell'] = (pan['panel_year'].astype(str) + '|'
                   + pan['size_bin'].astype(str) + '|'
                   + pan['educ_bin'].astype(str) + '|'
                   + pan['occ_bin'].astype(str))

    # --------------------------------------------------------
    # Drop rows missing any cell-defining or income variable
    # --------------------------------------------------------
    req = ['inc', 'zip', 'fips', 'w', 'size_bin', 'educ_bin', 'occ_bin']
    pan_v = pan.dropna(subset=req).copy()
    pan_v = pan_v[pan_v['w'] > 0]
    print(f"  {len(pan_v):,} rows with complete cell + income data")

    pan_v['wi'] = pan_v['w'] * pan_v['inc']

    # --------------------------------------------------------
    # Compute both IVs
    # --------------------------------------------------------
    print("Computing zip IV...")
    zip_iv = leave_one_out_iv(pan_v, 'zip').rename(columns={
        'iv_income':    'iv_income_zip',
        'iv_cell_n_lo': 'iv_cell_n_lo_zip',
        'cell_geo_share': 'cell_zip_share',
    })

    print("Computing FIPS IV...")
    fips_iv = leave_one_out_iv(pan_v, 'fips').rename(columns={
        'iv_income':    'iv_income_fips',
        'iv_cell_n_lo': 'iv_cell_n_lo_fips',
        'cell_geo_share': 'cell_fips_share',
    })

    out = zip_iv.merge(fips_iv, on=['household_code', 'panel_year'], how='outer')

    # --------------------------------------------------------
    # Diagnostics
    # --------------------------------------------------------
    for col, label in [('iv_income_zip', 'zip'), ('iv_income_fips', 'FIPS')]:
        n_valid = out[col].notna().sum()
        print(f"\n  {label} IV non-null: {n_valid:,} / {len(out):,}")
        print(f"  {label} IV mean:     {out[col].mean():.2f} ($000s)")
        print(f"  {label} IV std:      {out[col].std():.2f}")

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------
    out.to_parquet(OUT_PATH, index=False)
    out.to_stata("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/iv_income.dta")

    print(f"\nSaved: {OUT_PATH}")
    print(f"Columns: {list(out.columns)}")
    print("Done.")


if __name__ == '__main__':
    main()
