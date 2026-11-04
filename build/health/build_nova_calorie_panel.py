"""Household-year calories by NOVA group, produce calories, grams bought and adult equivalents.

For notes/obesity.tex (calories per person over time; where the calories come from; who eats
ultra-processed food). Reads interim/purchases_food/panel_year=<y>/ (one row per trip-UPC,
2004-2020, MAGNET rows dropped), interim/usda_nielsen_merged/usda_wide.parquet (calories per
100 g from the USDA Branded label) and usda_nova.parquet (NOVA group 1-4 from the ingredient
list, clean/usda/build_usda_nova.py), and interim/panelists/panelists_all_years.parquet.

Per purchase row: grams = quantity x multi (units in a multipack; omitted until 2026-11-04)
x Nielsen size1 converted to grams (rows whose size unit is a count, not a weight or volume,
have no grams); calories = grams x the UPC's calorie density per 100 g from
interim/health/upc_dictionary.parquet (build_upc_dictionary.py, 2026-11-04: tier 1 the UPC's own
USDA label, tier 2 the nearest-sized labelled UPC of the same brand, module and size unit, tier 3
the brand x module mean, tier 4 the module's pooled 2004-2020 density; fixed over time, so the
calorie total does not move with the label match rate, which rises from 39% of calories in 2004
to 81% in 2020 because the USDA file is a 2024 snapshot). Until 2026-11-04 unlabelled rows took
the module x year mean density of labelled rows, which moved with the labelled set.
Produce = fruit and vegetable modules and groups as in build_hi_usda_panel.py.

Per household-year: grams_all; cals_matched (tier-1 rows); cals_all (all tiers); cals_tier2,
cals_tier3, cals_tier4; cals_nova1..4 (tier-1 rows with an ingredient list); cals_nova_none;
produce_cals_matched, produce_cals_all; spend_all, spend_matched, spend_nova1..4;
ae_total = USDA Dietary Guidelines calorie need of each member (moderately active, age x
sex) / 2,000, summed over the roster (as in analyze/hi/usda/figure_calories_by_income.py),
with ae_complete = 1 when the roster count equals household_size.
Output: interim/health/nova_calorie_panel.parquet
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import gc, time
import numpy as np
import pandas as pd

PURCH = idir / 'purchases_food'
DICT = idir / 'health' / 'upc_dictionary.parquet'
PAN = idir / 'panelists' / 'panelists_all_years.parquet'
OUT = idir / 'health' / 'nova_calorie_panel.parquet'
YEARS = range(2004, 2021)
UNIT_TO_GRAMS = {'OZ': 28.3495, 'LB': 453.592, 'FL OZ': 29.5735, 'QT': 946.353, 'GAL': 3785.41,
                 'ML': 1.0, 'LT': 1000.0, 'KG': 1000.0, 'GM': 1.0, 'GR': 1.0}
FRUIT_GROUPS = {'FRUIT - CANNED', 'FRUIT - DRIED', 'FRUIT'}
FRUIT_MODULES_FRESH = {4010, 4085, 4180, 4225, 4355, 4470}
VEG_GROUPS = {'VEGETABLES - CANNED', 'VEGETABLES-FROZEN', 'VEGETABLES AND GRAINS - DRIED'}
VEG_MODULES_FRESH = {4015, 4020, 4023, 4050, 4055, 4060, 4140, 4230, 4275, 4280, 4350, 4400, 4415, 4460, 4475}


def calorie_need(age, male):
    """USDA DGA estimated daily calorie need, moderately active (midpoints)."""
    bins = [4, 9, 14, 19, 31, 51, np.inf]
    male_need = [1200, 1500, 2000, 2600, 2700, 2500, 2300]
    female_need = [1200, 1400, 1800, 2000, 2100, 2000, 1800]
    idx = np.searchsorted(bins, age, side='right')
    return np.where(male, np.take(male_need, idx), np.take(female_need, idx))


def adult_equivalents():
    cols = (['household_code', 'panel_year', 'household_size', 'male_head_birth', 'female_head_birth'] +
            [f'member_{i}_{v}' for i in range(1, 8) for v in ('birth', 'relationship_sex')])
    pan = pd.read_parquet(PAN, columns=cols)
    pan = pan[pan['panel_year'].isin(YEARS)]
    people = []
    for role, sexmale in [('male_head_birth', True), ('female_head_birth', False)]:
        p = pan[['household_code', 'panel_year', role]].rename(columns={role: 'birth'}).dropna()
        p['male'] = sexmale
        people.append(p)
    for i in range(1, 8):
        p = pan[['household_code', 'panel_year', f'member_{i}_birth', f'member_{i}_relationship_sex']] \
            .rename(columns={f'member_{i}_birth': 'birth', f'member_{i}_relationship_sex': 'rel'}).dropna()
        p['male'] = p['rel'].isin([1, 3, 5])
        people.append(p[['household_code', 'panel_year', 'birth', 'male']])
    people = pd.concat(people, ignore_index=True)
    people['age'] = (people['panel_year'] - people['birth']).clip(0, 100)
    people['ae'] = calorie_need(people['age'].values, people['male'].values) / 2000.0
    ae = people.groupby(['household_code', 'panel_year']).agg(ae_total=('ae', 'sum'), n_counted=('ae', 'size')).reset_index()
    ae = ae.merge(pan[['household_code', 'panel_year', 'household_size']], on=['household_code', 'panel_year'])
    ae['ae_complete'] = (ae['n_counted'] == ae['household_size']).astype(int)
    return ae[['household_code', 'panel_year', 'ae_total', 'ae_complete']]


def main():
    t0 = time.time()
    dic = pd.read_parquet(DICT, columns=['upc_13', 'cal_per_100g', 'cal_tier', 'nova']).dropna(subset=['cal_per_100g'])
    print(f'UPC dictionary: {len(dic):,} UPCs with a density, {dic.nova.notna().sum():,} with a NOVA group')
    out = []
    for y in YEARS:
        p = PURCH / f'panel_year={y}'
        if not p.exists():
            continue
        d = pd.read_parquet(p, columns=['upc', 'quantity', 'multi', 'size1_amount', 'size1_units', 'product_module_code', 'product_group',
                                        'product_module', 'department_descr', 'household_code', 'total_price_paid'])
        d = d[d['department_descr'] != 'MAGNET DATA']
        d['upc_13'] = '0' + d['upc'].astype(str).str.zfill(12)
        d = d.merge(dic, on='upc_13', how='left')
        d['grams'] = d['quantity'] * d['multi'].fillna(1).clip(lower=1) * d['size1_amount'] * d['size1_units'].map(UNIT_TO_GRAMS)
        d.loc[d['grams'] <= 0, 'grams'] = np.nan
        d['cals_all'] = d['grams'] * d['cal_per_100g'] / 100
        d['cals_matched'] = d['cals_all'].where(d['cal_tier'] == 1)
        for t in (2, 3, 4):
            d[f'cals_tier{t}'] = d['cals_all'].where(d['cal_tier'] == t, 0.0)
        d['is_fruit'] = (d['product_group'].isin(FRUIT_GROUPS) | d['product_module_code'].isin(FRUIT_MODULES_FRESH)
                         | d['product_module'].str.contains('FROZEN FRUITS|FRUIT JUICE|FRUIT DRINK', case=False, na=False))
        d['is_veg'] = (d['product_group'].isin(VEG_GROUPS) | d['product_module_code'].isin(VEG_MODULES_FRESH)
                       | d['product_module'].str.contains('VEGETABLE.*FROZEN|TOMATO PASTE|TOMATO SAUCE|TOMATO PUREE|TOMATOES.*CANNED|TOMATOES.*STEWED|MUSHROOM', case=False, na=False))
        prod = d['is_fruit'] | d['is_veg']
        d['produce_cals_matched'] = d['cals_matched'].where(prod, 0.0)
        d['produce_cals_all'] = d['cals_all'].where(prod, 0.0)
        for g in (1, 2, 3, 4):
            d[f'cals_nova{g}'] = d['cals_matched'].where(d['nova'] == g, 0.0)
            d[f'spend_nova{g}'] = d['total_price_paid'].where(d['nova'] == g, 0.0)
        d['cals_nova_none'] = d['cals_matched'].where(d['cals_matched'].notna() & d['nova'].isna(), 0.0)
        d['spend_matched'] = d['total_price_paid'].where(d['cals_matched'].notna(), 0.0)
        d['spend_nova_any'] = d['total_price_paid'].where(d['nova'].notna(), 0.0)
        agg = d.groupby('household_code').agg(
            grams_all=('grams', 'sum'), cals_matched=('cals_matched', 'sum'), cals_all=('cals_all', 'sum'),
            cals_tier2=('cals_tier2', 'sum'), cals_tier3=('cals_tier3', 'sum'), cals_tier4=('cals_tier4', 'sum'),
            cals_nova1=('cals_nova1', 'sum'), cals_nova2=('cals_nova2', 'sum'), cals_nova3=('cals_nova3', 'sum'), cals_nova4=('cals_nova4', 'sum'),
            cals_nova_none=('cals_nova_none', 'sum'), produce_cals_matched=('produce_cals_matched', 'sum'), produce_cals_all=('produce_cals_all', 'sum'),
            spend_all=('total_price_paid', 'sum'), spend_matched=('spend_matched', 'sum'), spend_nova_any=('spend_nova_any', 'sum'),
            spend_nova1=('spend_nova1', 'sum'), spend_nova2=('spend_nova2', 'sum'), spend_nova3=('spend_nova3', 'sum'), spend_nova4=('spend_nova4', 'sum'),
            n_rows=('upc', 'size')).reset_index()
        agg['panel_year'] = y
        out.append(agg)
        tot = d['cals_all'].sum()
        print(f'{y}: {len(d):,} rows; grams on {d.grams.notna().mean():.3f}; calories by tier: own label {d.cals_matched.sum() / tot:.3f}, '
              f'brand-size {d.cals_tier2.sum() / tot:.3f}, brand-module {d.cals_tier3.sum() / tot:.3f}, module {d.cals_tier4.sum() / tot:.3f}; '
              f'NOVA on {d.nova.notna().mean():.3f} of rows; {len(agg):,} households ({time.time() - t0:.0f}s)', flush=True)
        del d; gc.collect()
    hh = pd.concat(out, ignore_index=True)
    hh = hh.merge(adult_equivalents(), on=['household_code', 'panel_year'], how='left')
    OUT.parent.mkdir(parents=True, exist_ok=True)
    hh.to_parquet(OUT, index=False)
    print(f'saved {len(hh):,} household-years to {OUT} ({time.time() - t0:.0f}s)')


if __name__ == '__main__':
    main()
