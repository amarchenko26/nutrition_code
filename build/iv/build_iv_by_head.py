"""
Build HEAD-SPECIFIC leave-one-county-out income IVs (male head and female head).

Same design as build_iv.py, except the demographic cell is defined by EACH
HEAD'S OWN education x occupation (not the male-preferred household assignment),
so the two heads of the same household can receive different shocks:

  iv_income_male_fips   -- leave-one-county-out avg HH income among households
                           whose MALE head is in the same
                           hh_size_bin x own-educ_bin x own-occ_bin cell
  iv_income_female_fips -- same, for the FEMALE head's own cell

Nielsen reports income only at the household level, so the IV is a cell average
of *household* income; it proxies labor demand for that head's skill group.
As in build_iv.py, "not employed" is its own occupation bin (codes 10/12);
only absent heads / missing educ get NaN. Reuses leave_one_out_iv(), OCC_MAP,
and bins from build_iv.py.

Output:
  interim/panel_dataset/iv_income_by_head.parquet / .dta
  Columns: household_code, panel_year,
           iv_income_male_fips,   iv_cell_n_lo_male_fips,
           iv_income_female_fips, iv_cell_n_lo_female_fips
"""

import pandas as pd
import numpy as np

from build_iv import (leave_one_out_iv, OCC_MAP, MIN_CELL_N, deflate_income_k,
                      SIZE_BINS, SIZE_LABELS, EDUC_BINS, EDUC_LABELS,
                      BASE, PANELISTS)

OUT_PATH = BASE / 'interim' / 'panel_dataset' / 'iv_income_by_head.parquet'

# Nielsen education codes -> years of schooling (same map as build_iv.py)
EDUC_YEARS = {0: np.nan, 1: 6, 2: 10, 3: 12, 4: 14, 5: 16, 6: 18}


def head_iv(pan, sex):
    """Leave-one-county-out IV from the `sex` head's own educ x occ cell."""
    p = pan.copy()

    educ_yrs = pd.to_numeric(p[f'{sex}_head_education'], errors='coerce').map(EDUC_YEARS)
    p['educ_bin'] = pd.cut(educ_yrs, bins=EDUC_BINS, labels=EDUC_LABELS)
    p['occ_bin'] = pd.to_numeric(p[f'{sex}_head_occupation'], errors='coerce').map(OCC_MAP)

    p['cell'] = (p['panel_year'].astype(str) + '|'
                 + p['size_bin'].astype(str) + '|'
                 + p['educ_bin'].astype(str) + '|'
                 + p['occ_bin'].astype(str))

    req = ['inc', 'fips', 'w', 'size_bin', 'educ_bin', 'occ_bin']
    p_v = p.dropna(subset=req).copy()
    p_v = p_v[p_v['w'] > 0]
    print(f"  {sex}: {len(p_v):,} HH-years with a {sex} head + complete cell data")

    p_v['wi'] = p_v['w'] * p_v['inc']

    iv = leave_one_out_iv(p_v, 'fips').rename(columns={
        'iv_income':    f'iv_income_{sex}_fips',
        'iv_cell_n_lo': f'iv_cell_n_lo_{sex}_fips',
    })
    return iv.drop(columns='cell_geo_share')


def main():
    print("Loading panelists...")
    pan = pd.read_parquet(PANELISTS)
    print(f"  {len(pan):,} HH-year obs")

    pan['inc']  = deflate_income_k(pan)          # 2013$ in $1,000s, like real_income
    pan['fips'] = (pan['fips_state_code'].astype(str).str.zfill(2) +
                   pan['fips_county_code'].astype(str).str.zfill(3))
    pan['w']    = pd.to_numeric(pan['projection_factor'], errors='coerce')
    pan['size_bin'] = pd.cut(
        pd.to_numeric(pan['household_size'], errors='coerce'),
        bins=SIZE_BINS, labels=SIZE_LABELS)

    print("Computing male-head IV...")
    male_iv = head_iv(pan, 'male')
    print("Computing female-head IV...")
    female_iv = head_iv(pan, 'female')

    out = male_iv.merge(female_iv, on=['household_code', 'panel_year'], how='outer')

    for sex in ['male', 'female']:
        col = f'iv_income_{sex}_fips'
        print(f"\n  {col} non-null: {out[col].notna().sum():,} / {len(out):,}")
        print(f"  {col} mean: {out[col].mean():.2f} ($000s)   std: {out[col].std():.2f}")
    both = out[['iv_income_male_fips', 'iv_income_female_fips']].dropna()
    print(f"\n  HH-years with BOTH head IVs: {len(both):,}"
          f"   corr: {both['iv_income_male_fips'].corr(both['iv_income_female_fips']):.3f}")

    out.to_parquet(OUT_PATH, index=False)
    out.to_stata(str(OUT_PATH.with_suffix('.dta')), write_index=False)
    print(f"\nSaved: {OUT_PATH}")
    print("Done.")


if __name__ == '__main__':
    main()
