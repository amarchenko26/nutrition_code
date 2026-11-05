"""Household x year main retailer, for the store-switching design (what-moves-nutrition review, 2026-11-05).

The main retailer is the retailer code with the largest share of the household's food spending in the panel year.
Reads interim/purchases_food/panel_year=<y>/ (retailer_code, total_price_paid, one year at a time).
Output: interim/what_moves/hh_year_main_retailer.parquet (household_code, panel_year, retailer_code, main_share)
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import gc
import pandas as pd

OUT = idir / 'what_moves' / 'hh_year_main_retailer.parquet'


def main():
    out = []
    for y in range(2004, 2021):
        d = pd.read_parquet(idir / f'purchases_food/panel_year={y}', columns=['household_code', 'retailer_code', 'total_price_paid', 'department_descr'])
        d = d[d['department_descr'] != 'MAGNET DATA']
        g = d.groupby(['household_code', 'retailer_code'], observed=True)['total_price_paid'].sum().rename('spend').reset_index()
        g['tot'] = g.groupby('household_code')['spend'].transform('sum')
        g = g.sort_values(['household_code', 'spend']).drop_duplicates('household_code', keep='last')
        g['main_share'] = g['spend'] / g['tot']
        g['panel_year'] = y
        out.append(g[['household_code', 'panel_year', 'retailer_code', 'main_share']])
        print(y, len(g), round(g['main_share'].mean(), 3), flush=True)
        del d, g; gc.collect()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.concat(out, ignore_index=True).to_parquet(OUT, index=False)
    print(f'saved {OUT}')


if __name__ == '__main__':
    main()
