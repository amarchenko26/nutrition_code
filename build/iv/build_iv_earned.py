"""
VALIDATION-ONLY (do not use for estimation -- see labor_supply.tex,
"Validating income timing"): Note 3's official two-year income lag does NOT
describe actual reporting. Panelists report annualized CURRENT income at the
survey (Nielsen concurs; instructions changed in 2011), so shifting income
by two years MIS-times it and the IVs built here have no first stage
(corr of changes with own income ~0.002 vs 0.09 survey-timed). Kept because
that collapse is itself the evidence.

Original intent: leave-one-county-out income IVs with income attributed per
Kilts Note 3 (panel-year-t file = calendar t-2 income, so year-t "earned"
income = the t+2 file's report, built by clean_panelist.py as
household_income_midpoint_earned), paired with year-t cells.

Builds, with earned (nominal, as in the originals) income:
  iv_income_earned_fips        -- size x educ x occ cells (as build_iv.py)
  iv_income_occedu_earned_fips -- occ x edu cells (as build_iv_occ_edu.py)

Output:
  interim/panel_dataset/iv_income_earned.parquet / .dta
"""

import pandas as pd

from build_iv import (leave_one_out_iv, bin_occupation, deflate_income_k,
                      SIZE_BINS, SIZE_LABELS, EDUC_BINS, EDUC_LABELS,
                      BASE, PANELISTS)

OUT_PATH = BASE / 'interim' / 'panel_dataset' / 'iv_income_earned.parquet'


def build(pan, with_size, name):
    p = pan.copy()
    parts = [p['panel_year'].astype(str)]
    if with_size:
        parts.append(p['size_bin'].astype(str))
    parts += [p['educ_bin'].astype(str), p['occ_bin'].astype(str)]
    p['cell'] = parts[0].str.cat(parts[1:], sep='|')

    req = ['inc', 'fips', 'w', 'educ_bin', 'occ_bin'] + (['size_bin'] if with_size else [])
    p_v = p.dropna(subset=req).copy()
    p_v = p_v[p_v['w'] > 0]
    print(f"  {name}: {len(p_v):,} HH-years with earned income + complete cell data")

    p_v['wi'] = p_v['w'] * p_v['inc']
    iv = leave_one_out_iv(p_v, 'fips').rename(columns={
        'iv_income':    f'iv_income_{name}_fips',
        'iv_cell_n_lo': f'iv_cell_n_lo_{name}_fips',
    })
    return iv.drop(columns='cell_geo_share')


def main():
    print("Loading panelists...")
    pan = pd.read_parquet(PANELISTS)
    print(f"  {len(pan):,} HH-year obs")

    pan['inc']  = deflate_income_k(pan, 'household_income_midpoint_earned')   # 2013$, $1,000s
    pan['fips'] = (pan['fips_state_code'].astype(str).str.zfill(2) +
                   pan['fips_county_code'].astype(str).str.zfill(3))
    pan['w']    = pd.to_numeric(pan['projection_factor'], errors='coerce')
    pan['size_bin'] = pd.cut(
        pd.to_numeric(pan['household_size'], errors='coerce'),
        bins=SIZE_BINS, labels=SIZE_LABELS)
    pan['educ_bin'] = pd.cut(pan['hh_avg_yrsofschool'], bins=EDUC_BINS, labels=EDUC_LABELS)
    pan['occ_bin'] = bin_occupation(pan)

    print("Computing size x educ x occ earned IV...")
    out = build(pan, with_size=True, name='earned')
    print("Computing occ x edu earned IV...")
    out = out.merge(build(pan, with_size=False, name='occedu_earned'),
                    on=['household_code', 'panel_year'], how='outer')

    for col in ['iv_income_earned_fips', 'iv_income_occedu_earned_fips']:
        print(f"\n  {col} non-null: {out[col].notna().sum():,} / {len(out):,}")
        print(f"  {col} mean: {out[col].mean():.2f} ($000s)   std: {out[col].std():.2f}")

    out.to_parquet(OUT_PATH, index=False)
    out.to_stata(str(OUT_PATH.with_suffix('.dta')), write_index=False)
    print(f"\nSaved: {OUT_PATH}")
    print("Done.")


if __name__ == '__main__':
    main()
