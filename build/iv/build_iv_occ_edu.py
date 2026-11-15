"""
Build leave-one-county-out income IVs with OCCUPATION x EDUCATION cells
(no household-size bin), for the cell-definition robustness check.

Same design as build_iv.py / build_iv_by_head.py, except the demographic cell
is year x educ_bin x occ_bin only:

  iv_income_occedu_fips        -- male-preferred household assignment
                                  (educ = hh_avg_yrsofschool, occ = male head
                                  with female fallback), as in build_iv.py
  iv_income_male_occedu_fips   -- the MALE head's own educ x occ cell
  iv_income_female_occedu_fips -- the FEMALE head's own educ x occ cell

Output:
  interim/panel_dataset/iv_income_occ_edu.parquet / .dta
"""

import pandas as pd

from build_iv import (leave_one_out_iv, bin_occupation, OCC_MAP, deflate_income_k,
                      EDUC_BINS, EDUC_LABELS, BASE, PANELISTS)
from build_iv_by_head import EDUC_YEARS

OUT_PATH = BASE / 'interim' / 'panel_dataset' / 'iv_income_occ_edu.parquet'


def hh_iv(pan):
    """Male-preferred household cell, year x educ x occ."""
    p = pan.copy()
    p['educ_bin'] = pd.cut(p['hh_avg_yrsofschool'], bins=EDUC_BINS, labels=EDUC_LABELS)
    p['occ_bin'] = bin_occupation(p)
    p['cell'] = (p['panel_year'].astype(str) + '|'
                 + p['educ_bin'].astype(str) + '|'
                 + p['occ_bin'].astype(str))

    p_v = p.dropna(subset=['inc', 'fips', 'w', 'educ_bin', 'occ_bin']).copy()
    p_v = p_v[p_v['w'] > 0]
    print(f"  hh: {len(p_v):,} HH-years with complete cell + income data")

    p_v['wi'] = p_v['w'] * p_v['inc']
    iv = leave_one_out_iv(p_v, 'fips').rename(columns={
        'iv_income':    'iv_income_occedu_fips',
        'iv_cell_n_lo': 'iv_cell_n_lo_occedu_fips',
    })
    return iv.drop(columns='cell_geo_share')


def head_iv(pan, sex):
    """The `sex` head's own educ x occ cell, year x educ x occ."""
    p = pan.copy()
    educ_yrs = pd.to_numeric(p[f'{sex}_head_education'], errors='coerce').map(EDUC_YEARS)
    p['educ_bin'] = pd.cut(educ_yrs, bins=EDUC_BINS, labels=EDUC_LABELS)
    p['occ_bin'] = pd.to_numeric(p[f'{sex}_head_occupation'], errors='coerce').map(OCC_MAP)
    p['cell'] = (p['panel_year'].astype(str) + '|'
                 + p['educ_bin'].astype(str) + '|'
                 + p['occ_bin'].astype(str))

    p_v = p.dropna(subset=['inc', 'fips', 'w', 'educ_bin', 'occ_bin']).copy()
    p_v = p_v[p_v['w'] > 0]
    print(f"  {sex}: {len(p_v):,} HH-years with a {sex} head + complete cell data")

    p_v['wi'] = p_v['w'] * p_v['inc']
    iv = leave_one_out_iv(p_v, 'fips').rename(columns={
        'iv_income':    f'iv_income_{sex}_occedu_fips',
        'iv_cell_n_lo': f'iv_cell_n_lo_{sex}_occedu_fips',
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

    print("Computing household (male-preferred) occ x edu IV...")
    out = hh_iv(pan)
    print("Computing male-head occ x edu IV...")
    out = out.merge(head_iv(pan, 'male'), on=['household_code', 'panel_year'], how='outer')
    print("Computing female-head occ x edu IV...")
    out = out.merge(head_iv(pan, 'female'), on=['household_code', 'panel_year'], how='outer')

    for col in ['iv_income_occedu_fips', 'iv_income_male_occedu_fips',
                'iv_income_female_occedu_fips']:
        print(f"\n  {col} non-null: {out[col].notna().sum():,} / {len(out):,}")
        print(f"  {col} mean: {out[col].mean():.2f} ($000s)   std: {out[col].std():.2f}")

    out.to_parquet(OUT_PATH, index=False)
    out.to_stata(str(OUT_PATH.with_suffix('.dta')), write_index=False)
    print(f"\nSaved: {OUT_PATH}")
    print("Done.")


if __name__ == '__main__':
    main()
