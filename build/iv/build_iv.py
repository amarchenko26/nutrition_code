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

Income is the bracket midpoint (top bracket collapsed to $100k+ in every year
by clean_panelist.py) deflated to 2013 dollars with the same CPI table as
real_income in build_hi_panel.py, in $1,000s -- so the instrument and the
endogenous variable are on the same price basis (review 2026-09-15, §4.5).

Output:
  interim/panel_dataset/iv_income.parquet / .dta
  Columns: household_code, panel_year,
           iv_income_zip, iv_cell_n_lo_zip, cell_zip_share,
           iv_income_fips, iv_cell_n_lo_fips, cell_fips_share
  iv_cell_n_lo_* is the number of leave-out donor HOUSEHOLDS (not a weight sum).
"""

import pandas as pd
import numpy as np
from pathlib import Path

BASE      = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
PANELISTS = BASE / 'interim' / 'panelists' / 'panelists_all_years.parquet'
OUT_PATH  = BASE / 'interim' / 'panel_dataset' / 'iv_income.parquet'

# Minimum leave-out cell size — drop IV for HHs whose cell has fewer
# than this many leave-out donor HOUSEHOLDS (too noisy to be useful).
# (Until 2026-09-16 this was compared against the leave-out SUM OF
# PROJECTION WEIGHTS, so it never bound.)
MIN_CELL_N = 30

# CPI-U annual averages, same table as build_hi_panel.py; base year 2013
CPI = {
    2004: 188.9, 2005: 195.3, 2006: 201.6, 2007: 207.3, 2008: 215.3,
    2009: 214.5, 2010: 218.1, 2011: 224.9, 2012: 229.6, 2013: 233.0,
    2014: 236.7, 2015: 237.0, 2016: 240.0, 2017: 245.1, 2018: 251.1,
    2019: 255.7, 2020: 258.8, 2021: 271.0, 2022: 292.7, 2023: 304.7,
    2024: 314.0,
}
CPI_BASE = CPI[2013]


def deflate_income_k(pan, col='household_income_midpoint'):
    """Nominal income column -> 2013 dollars in $1,000s (same CPI as real_income)."""
    nominal = pd.to_numeric(pan[col], errors='coerce')
    year = pd.to_numeric(pan['panel_year'], errors='coerce')
    return nominal * (CPI_BASE / year.map(CPI)) / 1000

# ============================================================
# CELL BIN DEFINITIONS
# ============================================================

# Household size
SIZE_BINS   = [0, 1, 2, 4, np.inf]
SIZE_LABELS = ['1', '2', '3-4', '5+']

# Education (years of schooling from hh_avg_yrsofschool)
EDUC_BINS   = [0, 11.9, 12.9, 15.9, np.inf]
EDUC_LABELS = ['lt_hs', 'hs', 'some_col', 'col_plus']

# Occupation bins (Nielsen codes per the Kilts codebook, corrected 2026-09-10)
# White collar: Professional (1), Manager/Admin/Owner (2), Clerical (3), Sales (4)
# Blue collar:  Craftsman/Foreman (5), Operative/Driver (6), Laborer (11:
#               construction worker, shipping, fisherman, gardener, lumberman)
# Service/other: Service worker (8), Farmer (9) -- labor_supply.tex,
#               "Measurement of key variables", final 2026-09-16.
# Armed forces (7): EXCLUDED (NaN). A head in the armed forces defines no cell;
#               bin_occupation() then falls back to the female head, and a
#               household whose only/both heads are armed forces has no cell.
# Not employed: Not Employed for Pay (12) AND code 10. The codebook labels 10
#               "Students employed <30 hours", but in the data 89-100% of
#               code-10 heads report "not employed for pay" in every era, 0%
#               work 35+ hours, and they look like code-12 heads on age and
#               income (review 2026-09-15, §4.1) -- so they are binned with
#               the not-employed. No Head (0) → NaN
OCC_MAP = {
    0:  np.nan,       # No Head
    1:  'white',      # Professional
    2:  'white',      # Manager/Admin/Owner
    3:  'white',      # Clerical
    4:  'white',      # Sales
    5:  'blue',       # Craftsman/Foreman
    6:  'blue',       # Operative/Driver
    7:  np.nan,       # Armed forces -- excluded
    8:  'service',    # Service worker
    9:  'service',    # Farmer/Farm Labor
    10: 'not_emp',    # "Student employed <30h" per codebook; not employed in the data
    11: 'blue',       # Laborer
    12: 'not_emp',    # Not Employed for Pay
}
# Occupation codes that mean the head works (everything except 0, 7, 10, 12)
EMPLOYED_OCC_CODES = {1, 2, 3, 4, 5, 6, 8, 9, 11}


def bin_occupation(pan):
    """Pick the head's occupation bin: the male head's if he is present and
    not in the armed forces (code 7 maps to NaN), else the female head's."""
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
    cell_tot = (pan_v.groupby('cell')
                .agg(cell_wi=('wi', 'sum'), cell_w=('w', 'sum'), cell_n=('w', 'size')))
    cell_geo_tot = (pan_v.groupby(['cell', geo_col])
                    .agg(cz_wi=('wi', 'sum'), cz_w=('w', 'sum'), cz_n=('w', 'size'))
                    .reset_index())

    pv = pan_v[['household_code', 'panel_year', 'cell', geo_col, 'w', 'wi']].copy()
    pv = pv.merge(cell_tot, on='cell', how='left')
    pv = pv.merge(cell_geo_tot, on=['cell', geo_col], how='left')

    lo_wi = pv['cell_wi'] - pv['cz_wi']
    lo_w  = pv['cell_w']  - pv['cz_w']
    lo_n  = pv['cell_n']  - pv['cz_n']          # leave-out donor HOUSEHOLD count
    pv['iv_income']    = lo_wi / lo_w.replace(0, np.nan)
    pv['iv_cell_n_lo'] = lo_n
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
    pan['inc']  = deflate_income_k(pan)          # 2013$ in $1,000s, like real_income
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
        n_small = (out[col.replace('iv_income', 'iv_cell_n_lo')] < MIN_CELL_N).sum()
        print(f"\n  {label} IV non-null: {n_valid:,} / {len(out):,} "
              f"({n_small:,} dropped: fewer than {MIN_CELL_N} leave-out donor households)")
        print(f"  {label} IV mean:     {out[col].mean():.2f} ($000s, 2013$)")
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
