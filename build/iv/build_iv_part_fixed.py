"""
build_iv_part_fixed.py  --  run LOCALLY

The main instrument with one part of the household's group held at t-1 (Anya 2026-11-13; size first, then occupation and
schooling). The main instrument (build_iv.py, iv_income_fips) is the leave-one-county-out mean income of the household's group,
occupation x schooling x size, in each year:
    d_iv          = z[occupation_t,   schooling_t,   size_t  ](t) - z[occupation_t-1, schooling_t-1, size_t-1](t-1)
Each version here holds one part at its t-1 value in both years and lets the other two change as in the main instrument:
    d_iv_sizefix  = z[occupation_t,   schooling_t,   size_t-1](t) - (same t-1 term)
    d_iv_occfix   = z[occupation_t-1, schooling_t,   size_t  ](t) - (same t-1 term)
    d_iv_educfix  = z[occupation_t,   schooling_t-1, size_t  ](t) - (same t-1 term)
so a change in the held part does not move the shock (occupation held: a head who stops working keeps the income change of the
working group). Where the held part does not change, each version equals the main instrument (asserted). Group incomes for every
group x county x year come from build_iv_predicted.cell_income_table (the build_iv formula, checked against iv_income_fips); each
year uses the household's county in that year, as the main instrument does.

Output: interim/panel_dataset/iv_part_fixed.parquet / .dta: household_code, panel_year (= t), d_iv_sizefix, d_iv_occfix,
d_iv_educfix ($1,000s 2013, like iv_income_fips; the .do files divide by 10), size_switch, occ_switch, educ_switch (1 if that part
differs between t-1 and t). Read by analyze/what_moves/build_iv_robustness.do (Table 9 rows sizefix, occfix, educfix).
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import numpy as np
import pandas as pd

from build_iv_predicted import prep_panel, cell_income_table, validate_against_production

OUT = idir / 'panel_dataset' / 'iv_part_fixed.parquet'
PARTS = {'size': 'size_bin', 'occ': 'occ_bin', 'educ': 'educ_bin'}
BINS = ['size_bin', 'educ_bin', 'occ_bin']                  # the order of build_iv_predicted's cell_key
COLS = ['fips', 'cell_key'] + BINS


def key(d, held):
    """cell_key with the bin `held` taken from t-1."""
    v = {b: d[b + '_l'] if b == held else d[b] for b in BINS}
    return v['size_bin'].astype(str) + '|' + v['educ_bin'].astype(str) + '|' + v['occ_bin'].astype(str)


def main():
    print('Panel and group incomes for every group x county x year...')
    pan = prep_panel()
    z = cell_income_table(pan)
    validate_against_production(pan, z)

    cur = pan[['household_code', 'panel_year'] + COLS]
    lag = cur.assign(panel_year=cur['panel_year'] + 1).rename(columns={c: c + '_l' for c in COLS})
    d = cur.merge(lag, on=['household_code', 'panel_year'], how='inner')
    d = d[d['cell_key'].notna() & d['cell_key_l'].notna()].copy()
    zl = z.assign(panel_year=z['panel_year'] + 1).rename(columns={'fips': 'fips_l', 'cell_key': 'cell_key_l', 'z': 'z_l'})
    d = d.merge(zl, on=['panel_year', 'fips_l', 'cell_key_l'], how='left')
    d = d.merge(z.rename(columns={'z': 'z_t'}), on=['panel_year', 'fips', 'cell_key'], how='left')
    d['d_iv_main'] = d['z_t'] - d['z_l']
    print(f"  household-year pairs: {len(d):,}; SD of the main shock ($1,000s 2013) {d['d_iv_main'].std():.2f}")

    out = d[['household_code', 'panel_year']].copy()
    for part, b in PARTS.items():
        d['cell_' + part] = key(d, b)
        d = d.merge(z.rename(columns={'cell_key': 'cell_' + part, 'z': 'z_' + part}), on=['panel_year', 'fips', 'cell_' + part], how='left')
        fix, sw = f'd_iv_{part}fix', f'{part}_switch'
        d[fix] = d['z_' + part] - d['z_l']
        d[sw] = (d[b] != d[b + '_l']).astype(float)
        same = d[sw] == 0
        gap = (d.loc[same, fix] - d.loc[same, 'd_iv_main']).abs().max()
        assert gap < 1e-9, (part, gap)
        s = d[d[sw] == 1]
        print(f"  {part} held at t-1: the {part} part changes in {d[sw].mean():.1%} of pairs; there the shock averages "
              f"{s['d_iv_main'].mean():.2f} (main) vs {s[fix].mean():.2f}; defined {d[fix].notna().sum():,}; SD {d[fix].std():.2f}; "
              f"correlation with the main shock {d[['d_iv_main', fix]].corr().iloc[0, 1]:.3f}")
        out[fix], out[sw] = d[fix].values, d[sw].values
    out.to_parquet(OUT, index=False)
    out.to_stata(str(OUT.with_suffix('.dta')), write_index=False)
    print(f'Saved {OUT} and .dta')


if __name__ == '__main__':
    main()
