"""
Build leave-one-out income IV for each household-year.

For each HH in a given year, the IV is the projection-factor-weighted
average income of all other households in the same demographic cell
(household_size × education × occupation), nationwide, EXCLUDING
households in the same zip code.

    IV_it = Σ_{j ∉ zip_i, j ∈ cell_i} (w_j * inc_j) / Σ w_j

This instruments for household income using national income trends
among demographically similar households, purged of local area shocks.

Cell definition (coarsened to keep cells populated):
  - household_size_bin: 1, 2, 3-4, 5+
  - educ_bin:           <HS, HS grad, Some college, College+
  - occ_bin:            White collar, Blue collar, Service/Other, Not employed

Output:
  interim/panel_dataset/iv_income.parquet
  Columns: household_code, panel_year, iv_income, iv_cell_n_lo
    iv_income    -- leave-one-zip-out weighted mean income of cell ($000s)
    iv_cell_n_lo -- leave-out sample size (use to drop weak cells)
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


def main():
    print("Loading panelists...")
    pan = pd.read_parquet(PANELISTS)
    print(f"  {len(pan):,} HH-year obs")

    # --------------------------------------------------------
    # Prep variables
    # --------------------------------------------------------
    pan['inc'] = pd.to_numeric(pan['household_income_midpoint'], errors='coerce') / 1000  # $000s
    pan['zip'] = pan['panelist_zip_code'].astype(str).str.zfill(5)
    pan['w']   = pd.to_numeric(pan['projection_factor'], errors='coerce')

    # Education bin
    if 'hh_avg_yrsofschool' in pan.columns:
        educ_src = pan['hh_avg_yrsofschool']
    else:
        # Fall back to male head education code midpoints
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
    req = ['inc', 'zip', 'w', 'size_bin', 'educ_bin', 'occ_bin']
    pan_v = pan.dropna(subset=req).copy()
    pan_v = pan_v[pan_v['w'] > 0]
    print(f"  {len(pan_v):,} rows with complete cell + income data")

    pan_v['wi'] = pan_v['w'] * pan_v['inc']

    # --------------------------------------------------------
    # Efficient leave-one-zip-out computation
    #
    # IV_i = (Σ_{cell} w*inc - Σ_{cell,zip_i} w*inc)
    #        / (Σ_{cell} w    - Σ_{cell,zip_i} w)
    # --------------------------------------------------------
    print("Computing cell totals...")
    cell_tot = (pan_v.groupby('cell')[['wi', 'w']]
                .sum()
                .rename(columns={'wi': 'cell_wi', 'w': 'cell_w'}))

    cell_zip_tot = (pan_v.groupby(['cell', 'zip'])[['wi', 'w']]
                    .sum()
                    .rename(columns={'wi': 'cz_wi', 'w': 'cz_w'})
                    .reset_index())

    pan_v = pan_v.merge(cell_tot, on='cell', how='left')
    pan_v = pan_v.merge(cell_zip_tot, on=['cell', 'zip'], how='left')

    lo_wi = pan_v['cell_wi'] - pan_v['cz_wi']
    lo_w  = pan_v['cell_w']  - pan_v['cz_w']

    pan_v['iv_income']    = lo_wi / lo_w.replace(0, np.nan)
    pan_v['iv_cell_n_lo'] = lo_w   # weighted N of leave-out group

    # Null out IV for thin cells
    pan_v.loc[pan_v['iv_cell_n_lo'] < MIN_CELL_N, 'iv_income'] = np.nan

    # --------------------------------------------------------
    # Diagnostics
    # --------------------------------------------------------
    n_valid = pan_v['iv_income'].notna().sum()
    print(f"\n  IV non-null:  {n_valid:,} / {len(pan_v):,} HH-years")
    print(f"  IV mean:      {pan_v['iv_income'].mean():.2f} ($000s)")
    print(f"  IV std:       {pan_v['iv_income'].std():.2f}")
    print(f"  Corr(iv, inc): {pan_v[['iv_income','inc']].corr().iloc[0,1]:.3f}")

    cell_sizes = pan_v.groupby('cell')['w'].sum()
    print(f"\n  Cells: {len(cell_sizes):,} unique cell-years")
    print(f"  Median cell weight: {cell_sizes.median():.0f}")
    print(f"  Cells with lo_n < {MIN_CELL_N}: "
          f"{(pan_v.groupby('cell')['iv_cell_n_lo'].first() < MIN_CELL_N).sum()}")

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------
    out = pan_v[['household_code', 'panel_year', 'iv_income', 'iv_cell_n_lo']].copy()
    out.to_parquet(OUT_PATH, index=False)
    out.to_stata("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/panel_dataset/iv_income.dta")

    print(f"\nSaved: {OUT_PATH}")
    print(f"Columns: {list(out.columns)}")
    print("Done.")


if __name__ == '__main__':
    main()
