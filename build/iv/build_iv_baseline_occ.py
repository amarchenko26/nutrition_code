"""Leave-one-county-out income IV with BASELINE-occupation assignment.

Same cells and leave-out machinery as build_iv.py (year x size_bin x educ_bin
x occ_bin, county-FIPS leave-out, MIN_CELL_N 30), with one change: each
household's ASSIGNED occupation bin is frozen at its first panel year with a
non-missing occupation ("a plumber at baseline always gets plumber shocks").
Donor cell means still use households' CURRENT occupation (Bartik logic:
baseline assignment x current cell conditions), so a household's own job loss
or retirement can never move it into a different shock cell.

Output: interim/panel_dataset/iv_income_baseline_occ.parquet / .dta
  Columns: household_code, panel_year, iv_income_baseline_fips,
           iv_cell_n_lo_baseline_fips, baseline_occ_bin
"""

import pandas as pd
import numpy as np
from pathlib import Path

# cell bins, occupation map, CPI deflation and MIN_CELL_N are shared with build_iv.py
from build_iv import (OCC_MAP, MIN_CELL_N, deflate_income_k,
                      SIZE_BINS, SIZE_LABELS, EDUC_BINS, EDUC_LABELS,
                      BASE, PANELISTS)

OUT_STEM  = BASE / 'interim' / 'panel_dataset' / 'iv_income_baseline_occ'


def main():
    print("Loading panelists...")
    pan = pd.read_parquet(PANELISTS, columns=[
        'household_code', 'panel_year', 'household_income_midpoint',
        'fips_state_code', 'fips_county_code', 'projection_factor',
        'household_size', 'hh_avg_yrsofschool',
        'male_head_occupation', 'female_head_occupation'])

    pan['inc']  = deflate_income_k(pan)          # 2013$ in $1,000s, like real_income
    pan['fips'] = (pan['fips_state_code'].astype(str).str.zfill(2) +
                   pan['fips_county_code'].astype(str).str.zfill(3))
    pan['w']    = pd.to_numeric(pan['projection_factor'], errors='coerce')
    pan['educ_bin'] = pd.cut(pan['hh_avg_yrsofschool'], bins=EDUC_BINS, labels=EDUC_LABELS)
    pan['size_bin'] = pd.cut(pd.to_numeric(pan['household_size'], errors='coerce'),
                             bins=SIZE_BINS, labels=SIZE_LABELS)

    occ = pd.to_numeric(pan['male_head_occupation'], errors='coerce').map(OCC_MAP)
    female_occ = pd.to_numeric(pan['female_head_occupation'], errors='coerce').map(OCC_MAP)
    pan['occ_bin'] = occ.where(occ.notna(), female_occ)

    # Baseline occupation: first panel year with a non-missing occ_bin
    base = (pan.dropna(subset=['occ_bin'])
              .sort_values(['household_code', 'panel_year'])
              .groupby('household_code')['occ_bin'].first()
              .rename('baseline_occ_bin'))
    pan = pan.merge(base, on='household_code', how='left')

    # ---- donor cell aggregates: CURRENT occupation, as in build_iv.py ----
    req = ['inc', 'fips', 'w', 'size_bin', 'educ_bin', 'occ_bin']
    don = pan.dropna(subset=req).copy()
    don = don[don['w'] > 0]
    don['wi'] = don['w'] * don['inc']
    don['cell'] = (don['panel_year'].astype(str) + '|' + don['size_bin'].astype(str)
                   + '|' + don['educ_bin'].astype(str) + '|' + don['occ_bin'].astype(str))
    print(f"  donor rows: {len(don):,}")

    cell_tot = don.groupby('cell').agg(cell_wi=('wi', 'sum'), cell_w=('w', 'sum'),
                                       cell_n=('w', 'size'))
    cell_geo = don.groupby(['cell', 'fips']).agg(cz_wi=('wi', 'sum'), cz_w=('w', 'sum'),
                                                 cz_n=('w', 'size')).reset_index()

    # ---- assignment: same cell key but with BASELINE occupation ----
    asg = pan.dropna(subset=['fips', 'size_bin', 'educ_bin', 'baseline_occ_bin']).copy()
    asg['cell'] = (asg['panel_year'].astype(str) + '|' + asg['size_bin'].astype(str)
                   + '|' + asg['educ_bin'].astype(str) + '|' + asg['baseline_occ_bin'].astype(str))
    asg = asg.merge(cell_tot, on='cell', how='left')
    asg = asg.merge(cell_geo, on=['cell', 'fips'], how='left')
    asg[['cz_wi', 'cz_w', 'cz_n']] = asg[['cz_wi', 'cz_w', 'cz_n']].fillna(0)  # no co-cell HHs in own county

    lo_wi = asg['cell_wi'] - asg['cz_wi']
    lo_w  = asg['cell_w']  - asg['cz_w']
    lo_n  = asg['cell_n']  - asg['cz_n']        # leave-out donor HOUSEHOLD count
    asg['iv_income_baseline_fips'] = lo_wi / lo_w.replace(0, np.nan)
    asg['iv_cell_n_lo_baseline_fips'] = lo_n
    asg.loc[asg['iv_cell_n_lo_baseline_fips'] < MIN_CELL_N, 'iv_income_baseline_fips'] = np.nan

    out = asg[['household_code', 'panel_year', 'iv_income_baseline_fips',
               'iv_cell_n_lo_baseline_fips', 'baseline_occ_bin']].copy()
    n = out['iv_income_baseline_fips'].notna().sum()
    print(f"  IV non-null: {n:,} / {len(out):,}")
    print(f"  IV mean {out['iv_income_baseline_fips'].mean():.2f}, "
          f"sd {out['iv_income_baseline_fips'].std():.2f} ($000s)")
    same = (asg['occ_bin'] == asg['baseline_occ_bin']).mean()
    print(f"  share of HH-years where current occ == baseline occ: {same:.1%}")

    out.to_parquet(OUT_STEM.with_suffix('.parquet'), index=False)
    out['baseline_occ_bin'] = out['baseline_occ_bin'].astype(str)
    out.to_stata(OUT_STEM.with_suffix('.dta'), write_index=False, version=118)
    print(f"Saved: {OUT_STEM}.parquet / .dta")


if __name__ == '__main__':
    main()
