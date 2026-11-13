"""
build_iv_size_fixed.py  --  run LOCALLY

The main instrument with the household-size part of the group held at t-1 (Anya 2026-11-13). The main instrument (build_iv.py,
iv_income_fips) is the leave-one-county-out mean income of the household's group, occupation x schooling x size, in each year:
    d_iv         = z[occupation_t, schooling_t, size_t](t) - z[occupation_t-1, schooling_t-1, size_t-1](t-1)
Here the size group is the household's size group at t-1 in both years:
    d_iv_sizefix = z[occupation_t, schooling_t, size_t-1](t) - z[occupation_t-1, schooling_t-1, size_t-1](t-1)
so moving between size groups (1, 2, 3-4, 5+) does not move the shock, while changes of occupation (job loss) and schooling
still do; where the size group does not change, the two are equal. Group incomes for every group x county x year come from
build_iv_predicted.cell_income_table (the build_iv formula, checked against iv_income_fips); each year uses the household's
county in that year, as the main instrument does.

Output: interim/panel_dataset/iv_size_fixed.parquet / .dta: household_code, panel_year (= t), d_iv_sizefix ($1,000s 2013, like
iv_income_fips; the .do files divide by 10), size_switch (1 if the size group differs between t-1 and t).
Used by analyze/what_moves/build_iv_robustness.do sizefix (printed).
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

OUT = idir / 'panel_dataset' / 'iv_size_fixed.parquet'
COLS = ['fips', 'occ_bin', 'educ_bin', 'size_bin', 'cell_key']


def main():
    print('Panel and group incomes for every group x county x year...')
    pan = prep_panel()
    z = cell_income_table(pan)
    validate_against_production(pan, z)

    cur = pan[['household_code', 'panel_year'] + COLS]
    lag = cur.assign(panel_year=cur['panel_year'] + 1).rename(columns={c: c + '_l' for c in COLS})
    d = cur.merge(lag, on=['household_code', 'panel_year'], how='inner')
    d = d[d['cell_key'].notna() & d['cell_key_l'].notna()].copy()
    d['cell_sf'] = d['size_bin_l'].astype(str) + '|' + d['educ_bin'].astype(str) + '|' + d['occ_bin'].astype(str)
    zt = z.rename(columns={'cell_key': 'cell_key', 'z': 'z_t'})
    d = d.merge(zt, on=['panel_year', 'fips', 'cell_key'], how='left')
    d = d.merge(z.rename(columns={'cell_key': 'cell_sf', 'z': 'z_sf'}), on=['panel_year', 'fips', 'cell_sf'], how='left')
    zl = z.assign(panel_year=z['panel_year'] + 1).rename(columns={'fips': 'fips_l', 'cell_key': 'cell_key_l', 'z': 'z_l'})
    d = d.merge(zl, on=['panel_year', 'fips_l', 'cell_key_l'], how='left')
    d['d_iv_main'] = d['z_t'] - d['z_l']
    d['d_iv_sizefix'] = d['z_sf'] - d['z_l']
    d['size_switch'] = (d['size_bin'] != d['size_bin_l']).astype(float)

    same = d['size_switch'] == 0
    gap = (d.loc[same, 'd_iv_sizefix'] - d.loc[same, 'd_iv_main']).abs().max()
    print(f"  household-year pairs: {len(d):,}; size group changes: {d['size_switch'].mean():.1%}; "
          f"largest gap between the two shocks where it does not: {gap:.2e}")
    assert gap < 1e-9
    print(f"  shock defined: main {d['d_iv_main'].notna().sum():,}, size fixed {d['d_iv_sizefix'].notna().sum():,}")
    print('  SD ($1,000s 2013): main {:.2f}, size fixed {:.2f}; correlation {:.3f}'.format(
        d['d_iv_main'].std(), d['d_iv_sizefix'].std(), d[['d_iv_main', 'd_iv_sizefix']].corr().iloc[0, 1]))
    sw = d[d['size_switch'] == 1]
    print('  where the size group changes, mean shock ($1,000s): main {:.2f}, size fixed {:.2f} (size group up: {:.2f} vs {:.2f}; down: {:.2f} vs {:.2f})'.format(
        sw['d_iv_main'].mean(), sw['d_iv_sizefix'].mean(),
        *[x for c in ['up', 'down'] for x in (lambda s: (s['d_iv_main'].mean(), s['d_iv_sizefix'].mean()))(
            sw[(sw['size_bin'].map({'1': 1, '2': 2, '3-4': 3, '5+': 4}) > sw['size_bin_l'].map({'1': 1, '2': 2, '3-4': 3, '5+': 4})) == (c == 'up')])]))

    out = d[['household_code', 'panel_year', 'd_iv_sizefix', 'size_switch']].copy()
    out.to_parquet(OUT, index=False)
    out.to_stata(str(OUT.with_suffix('.dta')), write_index=False)
    print(f'Saved {OUT} and .dta')


if __name__ == '__main__':
    main()
