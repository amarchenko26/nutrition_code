"""Household-year inputs of build_soda_tax_did.do (what-moves review, 2026-11-05).

Outputs (interim/what_moves/):
  upf_share_hh_year.dta     hhid, year, upf_share (interim/health/nova_calorie_panel.parquet).
  soda_quantity_hh_year.dta hhid, year, soda_oz (ounces of module 1484 SOFT DRINKS - CARBONATED, regular soda; diet is
                            module 1553) and home_share (percent of those ounces bought at stores in the household's own
                            3-digit zip), for households in PA, CA, WA, CO and IL, 2010-2020. Ounces are quantity x
                            utils.food_measures.package_grams / 28.35, so multipacks count in full.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.food_measures import package_grams, UNIT_TO_GRAMS

import gc
import numpy as np
import pandas as pd

OUT = idir / 'what_moves'
STATES = [42, 6, 53, 8, 17]


def main():
    n = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet', columns=['household_code', 'panel_year', 'cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4'])
    n['upf_share'] = 100 * n['cals_nova4'] / n[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
    u = n.rename(columns={'household_code': 'hhid', 'panel_year': 'year'})[['hhid', 'year', 'upf_share']]
    u['hhid'] = u['hhid'].astype('int64'); u['year'] = u['year'].astype(int)
    u.to_stata(OUT / 'upf_share_hh_year.dta', write_index=False, version=118)
    pan = pd.read_parquet(idir / 'panelists' / 'panelists_all_years.parquet', columns=['household_code', 'panel_year', 'panelist_zip_code', 'fips_state_code'])
    pan['state'] = pd.to_numeric(pan['fips_state_code'], errors='coerce')
    pan = pan[pan['state'].isin(STATES)]
    pan['zip3'] = pd.to_numeric(pan['panelist_zip_code'], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(5).str[:3]
    parts = []
    for y in range(2010, 2021):
        d = pd.read_parquet(idir / f'purchases_food/panel_year={y}', columns=['upc', 'household_code', 'quantity', 'multi', 'size1_amount', 'size1_units', 'product_module_code', 'product_group_code', 'store_zip3'],
                            filters=[('product_module_code', '==', 1484)])
        d['upc_13'] = '0' + d['upc'].astype(str).str.zfill(12)
        d['oz'] = d['quantity'] * package_grams(d) / UNIT_TO_GRAMS['OZ']
        p = pan[pan['panel_year'] == y][['household_code', 'zip3']]
        d = d.merge(p, on='household_code', how='inner')
        d['sz3'] = pd.to_numeric(d['store_zip3'], errors='coerce').fillna(-1).astype(int).astype(str).str.zfill(3)
        d['home_oz'] = d['oz'].where(d['sz3'] == d['zip3'], 0.0)
        g = d.groupby('household_code').agg(soda_oz=('oz', 'sum'), home_oz=('home_oz', 'sum')).reset_index()
        g['home_share'] = 100 * g['home_oz'] / g['soda_oz'].replace(0, np.nan)
        # households in these states with no soda that year have zero ounces
        g = p[['household_code']].merge(g[['household_code', 'soda_oz', 'home_share']], on='household_code', how='left').fillna({'soda_oz': 0.0})
        g['year'] = y
        parts.append(g)
        print(y, len(g), round(g['soda_oz'].mean()), flush=True)
        del d; gc.collect()
    q = pd.concat(parts, ignore_index=True).rename(columns={'household_code': 'hhid'})
    q['hhid'] = q['hhid'].astype('int64'); q['year'] = q['year'].astype(int)
    q.to_stata(OUT / 'soda_quantity_hh_year.dta', write_index=False, version=118)
    print(f'saved {OUT}')


if __name__ == '__main__':
    main()
