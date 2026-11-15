"""Composite grocery baskets: high- vs low-HEI 4-person metro households.

For the talk's example slide (SSSS: Atlanta; AML Oct 2026: Boston/Providence).
Two DUA-compliant "illustrative households": 4-person metro HHs (2016-20) in the
top vs bottom decile (and quartile) of the full HEI, with each group's basket
AGGREGATED across all its HHs -- top product modules by real spending and
per-HH monthly dollars. No individual household's records are disclosed.

Prints group stats (N, mean HEI, median income) + top-15 module baskets.
"""
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path

import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
B = idir
YEARS = range(2016, 2021)

# regions as {(state_fips, county_fips), ...}
REGIONS = {
    # Atlanta MSA core counties (GA fips 13)
    'atlanta': {(13, c) for c in
                {121, 89, 67, 135, 63, 57, 97, 113, 117, 151, 247, 223, 77, 297,
                 143, 227, 45, 217, 255}},
    # Boston-Cambridge-Newton MSA (MA + southern NH) + Providence-Warwick MSA
    # (all RI + Bristol Cty MA)
    'boston_providence':
        {(25, c) for c in {9, 17, 21, 23, 25, 5}}        # MA: Essex, Middlesex,
                                                          # Norfolk, Plymouth,
                                                          # Suffolk, Bristol
        | {(33, c) for c in {15, 17}}                     # NH: Rockingham, Strafford
        | {(44, c) for c in {1, 3, 5, 7, 9}},             # RI: all five counties
}
REGION = 'boston_providence'

pan = pd.read_parquet(B / 'panelists/panelists_all_years.parquet',
                      columns=['household_code', 'panel_year', 'household_size',
                               'fips_state_code', 'fips_county_code',
                               'household_income_midpoint'])
hei = pd.read_parquet(B / 'panel_dataset/hei_usda_panel.parquet',
                      columns=['household_code', 'panel_year', 'hei_usda'])
d = pan.merge(hei, on=['household_code', 'panel_year'])
in_region = pd.MultiIndex.from_frame(d[['fips_state_code', 'fips_county_code']]) \
              .isin(REGIONS[REGION])
d = d[d.panel_year.isin(YEARS) & (d.household_size == 4) & in_region]
print(f'{REGION} 4-person HH-years {min(YEARS)}-{max(YEARS)}: {len(d):,} '
      f'({d.household_code.nunique():,} unique HHs)')

groups = {}
for lab, lo, hi in [('top decile', .9, 1.01), ('bottom decile', -.01, .1),
                    ('top quartile', .75, 1.01), ('bottom quartile', -.01, .25)]:
    qlo, qhi = d.hei_usda.quantile(max(lo, 0)), d.hei_usda.quantile(min(hi, 1))
    g = d[(d.hei_usda >= qlo) & (d.hei_usda <= qhi)] if lo > 0 else d[d.hei_usda <= qhi]
    if lo > 0.5:
        g = d[d.hei_usda >= qlo]
    groups[lab] = g
    print(f'  {lab}: {len(g):,} HH-years, {g.household_code.nunique()} HHs, '
          f'mean HEI {g.hei_usda.mean():.0f}, median income ${g.household_income_midpoint.median():,.0f}')

# ---- pull purchases for all selected HH-years in one pass per year ----
sel = pd.concat([g[['household_code', 'panel_year']].assign(grp=lab)
                 for lab, g in groups.items()])
purch = []
for yr in YEARS:
    hhs = sel.loc[sel.panel_year == yr, 'household_code'].unique()
    t = pq.read_table(B / f'purchases_food/panel_year={yr}',
                      columns=['household_code', 'product_module', 'total_price_paid_real_2013'],
                      filters=[('household_code', 'in', hhs.tolist())]).to_pandas()
    t['panel_year'] = yr
    purch.append(t)
purch = pd.concat(purch)
print(f'\npurchase rows pulled: {len(purch):,}')

for lab, g in groups.items():
    keys = g[['household_code', 'panel_year']]
    p = purch.merge(keys, on=['household_code', 'panel_year'])
    months = len(keys) * 12
    tot = p.total_price_paid_real_2013.sum()
    tab = (p.groupby('product_module').total_price_paid_real_2013.sum()
             .sort_values(ascending=False))
    print(f'\n===== {lab.upper()} — grocery spend ${tot/months:,.0f}/HH-month =====')
    for name, s in tab.head(15).items():
        print(f'  {s/tot:6.1%}  ${s/months:6.2f}/mo  {name}')
