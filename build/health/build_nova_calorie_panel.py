"""Household-year calories by NOVA group, produce calories, grams bought and adult equivalents.

For notes/obesity.tex (calories per person over time; where the calories come from; who eats
ultra-processed food). Reads interim/purchases_food/panel_year=<y>/ (one row per trip-UPC,
2004-2020, MAGNET rows dropped), interim/usda_nielsen_merged/usda_wide.parquet (calories per
100 g from the USDA Branded label) and usda_nova.parquet (NOVA group 1-4 from the ingredient
list, clean/usda/build_usda_nova.py), and interim/panelists/panelists_all_years.parquet.

Per purchase row: grams = quantity x Nielsen size1 converted to grams (rows whose size unit is
a count, not a weight or volume, have no grams); matched calories = grams x label calories
per 100 g (as in build/hi/usda/build_hi_usda_panel.py); imputed calories, for rows with grams
but no label, = grams x the spending-weighted mean calories per 100 g of the labelled UPCs in
the same product module and year (module mean over all years as the fallback), so that the
calorie total does not rise mechanically with the label match rate (the USDA file is a 2024
snapshot and matches 15% of UPCs bought in 2004 and 53% in 2020). Produce = fruit and
vegetable modules and groups as in build_hi_usda_panel.py.

Per household-year: grams_all; cals_matched; cals_all (matched + imputed); cals_nova1..4
(matched rows with an ingredient list); cals_nova_none (matched rows without one);
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
WIDE = idir / 'usda_nielsen_merged' / 'usda_wide.parquet'
NOVA = idir / 'usda_nielsen_merged' / 'usda_nova.parquet'
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
    wide = pd.read_parquet(WIDE, columns=['upc13', 'cal_per_100g']).rename(columns={'upc13': 'upc_13'})
    nova = pd.read_parquet(NOVA, columns=['upc13', 'nova']).rename(columns={'upc13': 'upc_13'})
    nova['nova'] = nova['nova'].astype('float')
    dic = wide.merge(nova, on='upc_13', how='left')
    print(f'label dictionary: {len(dic):,} UPCs, {dic.nova.notna().sum():,} with a NOVA group')
    # module-level calorie density pooled over years (fallback for the imputation)
    out = []
    dens_all = None
    for y in YEARS:
        p = PURCH / f'panel_year={y}'
        if not p.exists():
            continue
        d = pd.read_parquet(p, columns=['upc', 'quantity', 'size1_amount', 'size1_units', 'product_module_code', 'product_group',
                                        'product_module', 'department_descr', 'household_code', 'total_price_paid'])
        d = d[d['department_descr'] != 'MAGNET DATA']
        d['upc_13'] = '0' + d['upc'].astype(str).str.zfill(12)
        d = d.merge(dic, on='upc_13', how='left')
        d['grams'] = d['quantity'] * d['size1_amount'] * d['size1_units'].map(UNIT_TO_GRAMS)
        d.loc[d['grams'] <= 0, 'grams'] = np.nan
        d['cals_matched'] = d['grams'] * d['cal_per_100g'] / 100
        # module x year density of labelled rows, spending-weighted; fallback: module over all years so far
        lab = d[d['cals_matched'].notna() & (d['total_price_paid'] > 0)]
        dens = (lab.groupby('product_module_code').apply(lambda g: np.average(g['cal_per_100g'], weights=g['total_price_paid']))
                .rename('dens_my'))
        d = d.merge(dens, left_on='product_module_code', right_index=True, how='left')
        d['cals_imp'] = np.where(d['cals_matched'].notna(), d['cals_matched'], d['grams'] * d['dens_my'] / 100)
        d['cals_all'] = d['cals_imp']
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
            cals_nova1=('cals_nova1', 'sum'), cals_nova2=('cals_nova2', 'sum'), cals_nova3=('cals_nova3', 'sum'), cals_nova4=('cals_nova4', 'sum'),
            cals_nova_none=('cals_nova_none', 'sum'), produce_cals_matched=('produce_cals_matched', 'sum'), produce_cals_all=('produce_cals_all', 'sum'),
            spend_all=('total_price_paid', 'sum'), spend_matched=('spend_matched', 'sum'), spend_nova_any=('spend_nova_any', 'sum'),
            spend_nova1=('spend_nova1', 'sum'), spend_nova2=('spend_nova2', 'sum'), spend_nova3=('spend_nova3', 'sum'), spend_nova4=('spend_nova4', 'sum'),
            n_rows=('upc', 'size')).reset_index()
        agg['panel_year'] = y
        out.append(agg)
        print(f'{y}: {len(d):,} rows; grams on {d.grams.notna().mean():.3f}; label match {d.cal_per_100g.notna().mean():.3f} of rows, '
              f'{d.cals_matched.sum() / d.cals_all.sum():.3f} of calories; NOVA on {d.nova.notna().mean():.3f} of rows; '
              f'{len(agg):,} households ({time.time() - t0:.0f}s)', flush=True)
        del d, lab; gc.collect()
    hh = pd.concat(out, ignore_index=True)
    hh = hh.merge(adult_equivalents(), on=['household_code', 'panel_year'], how='left')
    OUT.parent.mkdir(parents=True, exist_ok=True)
    hh.to_parquet(OUT, index=False)
    print(f'saved {len(hh):,} household-years to {OUT} ({time.time() - t0:.0f}s)')


if __name__ == '__main__':
    main()
