"""
Build a RETIREE-ONLY leave-one-county-out income IV.

For households where NO head works (every present head's occupation is
"not employed for pay", codes 10/12), the standard cell average mixes in the
earnings of other households' working spouses — a household is assigned the
not-employed cell off the male head alone even if the female head works.
This IV removes that: the donor pool is restricted to households where no
head works, so the cell average reflects retiree income only (Social
Security, pensions, asset income). Cell = year x hh_size_bin x educ_bin
(occupation is constant within this universe). Same leave-one-county-out
logic, bins, and MIN_CELL_N as build_iv.py.

Output:
  interim/panel_dataset/iv_income_retiree.parquet / .dta
  Columns: household_code, panel_year,
           iv_income_retiree_fips, iv_cell_n_lo_retiree_fips
"""

import pandas as pd
import numpy as np

from build_iv import (leave_one_out_iv, MIN_CELL_N, deflate_income_k,
                      EMPLOYED_OCC_CODES,
                      SIZE_BINS, SIZE_LABELS, EDUC_BINS, EDUC_LABELS,
                      BASE, PANELISTS)

OUT_PATH = BASE / 'interim' / 'panel_dataset' / 'iv_income_retiree.parquet'


def main():
    print("Loading panelists...")
    pan = pd.read_parquet(PANELISTS)
    print(f"  {len(pan):,} HH-year obs")

    m_occ = pd.to_numeric(pan['male_head_occupation'], errors='coerce')
    f_occ = pd.to_numeric(pan['female_head_occupation'], errors='coerce')

    def head_state(occ):
        # 'works' = employed occupation codes (1-6, 8, 9, 11) / 'not_emp' =
        # 12 and 10 (code 10 heads report no paid work in the data, see
        # build_iv.OCC_MAP) / 'army' = 7 (excluded from all cells) /
        # 'absent' (0 or missing)
        works = occ.isin(EMPLOYED_OCC_CODES)
        not_emp = occ.isin([10, 12])
        army = occ.eq(7)
        return works, not_emp, army

    m_works, m_ne, m_army = head_state(m_occ)
    f_works, f_ne, f_army = head_state(f_occ)

    # No head works, at least one head is present (in the not-emp cell), and
    # no head is in the armed forces
    no_working_head = ~m_works & ~f_works & (m_ne | f_ne) & ~m_army & ~f_army
    pan = pan[no_working_head].copy()
    print(f"  {len(pan):,} HH-years with no working head")

    pan['inc']  = deflate_income_k(pan)          # 2013$ in $1,000s, like real_income
    pan['fips'] = (pan['fips_state_code'].astype(str).str.zfill(2) +
                   pan['fips_county_code'].astype(str).str.zfill(3))
    pan['w']    = pd.to_numeric(pan['projection_factor'], errors='coerce')
    pan['size_bin'] = pd.cut(
        pd.to_numeric(pan['household_size'], errors='coerce'),
        bins=SIZE_BINS, labels=SIZE_LABELS)
    pan['educ_bin'] = pd.cut(pan['hh_avg_yrsofschool'],
                             bins=EDUC_BINS, labels=EDUC_LABELS)

    pan['cell'] = (pan['panel_year'].astype(str) + '|'
                   + pan['size_bin'].astype(str) + '|'
                   + pan['educ_bin'].astype(str))

    req = ['inc', 'fips', 'w', 'size_bin', 'educ_bin']
    pan_v = pan.dropna(subset=req).copy()
    pan_v = pan_v[pan_v['w'] > 0]
    print(f"  {len(pan_v):,} rows with complete cell + income data")

    pan_v['wi'] = pan_v['w'] * pan_v['inc']

    print("Computing retiree-only FIPS IV...")
    iv = leave_one_out_iv(pan_v, 'fips').rename(columns={
        'iv_income':    'iv_income_retiree_fips',
        'iv_cell_n_lo': 'iv_cell_n_lo_retiree_fips',
    }).drop(columns='cell_geo_share')

    col = 'iv_income_retiree_fips'
    print(f"\n  {col} non-null: {iv[col].notna().sum():,} / {len(iv):,}")
    print(f"  {col} mean: {iv[col].mean():.2f} ($000s)   std: {iv[col].std():.2f}")

    iv.to_parquet(OUT_PATH, index=False)
    iv.to_stata(str(OUT_PATH.with_suffix('.dta')), write_index=False)
    print(f"\nSaved: {OUT_PATH}")
    print("Done.")


if __name__ == '__main__':
    main()
