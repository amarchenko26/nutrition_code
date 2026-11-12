"""Nutrition facts and NOVA group for every food UPC, fixed over time, from either label source.

Why: the USDA Branded file is a 2024 snapshot and the Syndigo labels start in 2005 with thin early coverage, so
the share of grocery calories with an own label rises from 39% (2004) to 81% (2020) and any labelled-only series
moves with that share. One set of values per UPC, independent of the year, removes the artifact. Allcott et al.
(2019) do the same step by regression (impute_nut_facts_for_missing_products.do); this uses the nearest labelled
relatives of the product (Anya 2026-11-03; extended from calories to every nutrient 2026-11-05).

Usage: python build_upc_dictionary.py usda     -> interim/health/upc_dictionary.parquet
       python build_upc_dictionary.py syndigo  -> interim/health/upc_dictionary_syndigo.parquet (added sugar since 2026-11-12, from
                                                  clean/syndigo/clean_syndigo_added_sugar.py)

For each nutrient separately, the value per 100 g is, in order:
  1  the UPC's own label;
  2  the closest relative: the mean over labelled UPCs with the same Nielsen description in the module (pack size removed:
     'CTL BR M 2% LF VAD PL F'), else the mean over labelled UPCs of the same brand, module and size unit at the closest
     package size. Leave-one-out on USDA labels, by spending, UPCs where both exist: calories within 10% on 83% with the same
     description and 69% with the closest size, saturated fat within 0.5 g on 73% and 58%; milk 85% and 21%, 96% and 26%.
     Until 2026-11-05 one relative at the closest size was taken, arbitrarily among ties, so a gallon of store-brand milk
     copied whole or skim by chance. Allcott et al. condition on product, type, flavor and milk fat in their regressions;
  3  the mean over labelled UPCs of the same brand and module;
  4  the module mean over labelled UPCs, weighted by the UPC's 2004-2020 spending;
  5  the mean over the product group's labelled UPCs.
Labels are cleaned first. The whole label goes when it is above 900 kcal/100 g, above five times its module's median
density by at least 200 kcal (a false UPC match: a soda labelled as chocolate; pepperoni among deli meats and salad kits
with dressing stay in), or below a twentieth of the median by at least 100 kcal (wrong serving grams: a 10-lb bag of sugar at
0.3 kcal/100 g; a label of exactly zero stays). A sugar, saturated fat, cholesterol or fiber line missing from a label counts
as zero where the module's labels that report the line have a median near zero (1 g, 0.5 g, 5 mg, 0.5 g per 100 g). One field goes when above 100 g (sugar,
fiber, fat), 40 g (sodium) or 3 g (cholesterol) per 100 g. Saturated fat is never above total fat, as in Allcott et al.
(prepare_gladson_nut_facts_for_imputation.do); saturated fat and sugar go when above the calories they imply, with 20%
and 5 kcal of slack for rounding (some labels carry the sodium figure in the saturated fat field: Mountain Dew, 60 mg
sodium, "60 g" saturated fat). Monounsaturated and polyunsaturated fat go when the fats exceed the calories they imply or
total fat. A typed commodity value (USDA Standard
Reference, kcal per 100 g) replaces tiers 2-5 for calories in fresh commodity modules, whose labelled UPCs are
atypical (apple chips in FRESH APPLES); Allcott et al. also hand-enter these modules (nut_facts_for_missing_modules).
NOVA (usda_nova.parquet; both sources use it): own ingredient list, else the most common group among UPCs with the same
description in the module, else among the same brand, module and unit at the closest size, else the brand x module mode,
else the module's spending-weighted mode.
Columns: upc_13, product_module_code, product_group_code, brand_descr, multi, cal_per_100g, cal_tier, the nutrients
per 100 g, nova, nova_tier. Read by build_module_price_calories.py, build_nova_calorie_panel.py and the three
nutrition panels (hi, hi_usda, hei_usda).
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import time
import numpy as np
import pandas as pd

SOURCES = {
    'usda': (idir / 'usda_nielsen_merged' / 'usda_wide.parquet', 'upc13', idir / 'health' / 'upc_dictionary.parquet',
             ['cal', 'sugar', 'satfat', 'chol', 'sodium', 'fiber', 'addsugar', 'totfat', 'mufat', 'pufat']),
    'syndigo': (idir / 'syndigo_nielsen_merged' / 'syndigo_wide.parquet', 'upc', idir / 'health' / 'upc_dictionary_syndigo.parquet',
                ['cal', 'sugar', 'satfat', 'chol', 'sodium', 'fiber', 'addsugar', 'totfat', 'mofat', 'pofat']),
}
# Syndigo's added sugar is not in syndigo_wide (clean_syndigo.py keeps the label year with the most nutrient lines, and a line only
# post-2016 labels carry would change that choice): clean/syndigo/clean_syndigo_added_sugar.py writes it apart (2026-11-12).
SYNDIGO_ADDSUGAR = idir / 'syndigo_nielsen_merged' / 'syndigo_added_sugar.parquet'
CAPS = {'cal': 900, 'sugar': 100, 'addsugar': 100, 'satfat': 100, 'totfat': 100, 'mufat': 100, 'pufat': 100, 'mofat': 100, 'pofat': 100,
        'fiber': 100, 'sodium': 40, 'chol': 3}
# USDA Standard Reference, raw, kcal per 100 g
COMMODITY_KCAL = {'FRESH APPLES': 52, 'FRESH ORANGES': 47, 'FRESH GRAPEFRUIT': 42, 'FRESH KIWI': 61, 'FRESH STRAWBERRIES': 32, 'FRESH CRANBERRIES': 46,
                  'FRESH FRUIT-REMAINING': 60, 'FRESH POTATOES': 77, 'FRESH TOMATOES': 18, 'FRESH LETTUCE': 15, 'FRESH CARROTS': 41, 'FRESH MUSHROOMS': 22,
                  'FRESH ONIONS': 40, 'FRESH CELERY': 16, 'FRESH SPINACH': 23, 'FRESH CAULIFLOWER': 25, 'FRESH RADISHES': 16, 'FRESH GARLIC': 149,
                  'FRESH HERBS': 30, 'FRESH VEGETABLES-REMAINING': 35, 'EGGS-FRESH': 143}


def fill_tiers(d, col, keys, spend):
    """Apply tiers 2-5 to one nutrient column; returns value and tier."""
    val = d[col].copy()
    tier = pd.Series(np.where(val.notna(), 1, np.nan), index=d.index)
    lab = d[val.notna()]
    # tier 2: same description in the module
    same = lab[lab['descr_norm'] != ''].groupby(['product_module_code', 'descr_norm'])[col].mean().rename('_same')
    m2 = d[['product_module_code', 'descr_norm']].merge(same, left_on=['product_module_code', 'descr_norm'], right_index=True, how='left')['_same']
    use = val.isna() & m2.notna()
    val[use] = m2[use]
    tier[use] = 2
    # tier 2: same brand, module and size unit, closest size; relatives of the same size are averaged
    unl = d[val.isna() & d['log_size'].notna() & d[keys].notna().all(axis=1)]
    l2 = lab[lab['log_size'].notna()].dropna(subset=keys).groupby(keys + ['log_size'], as_index=False, observed=True)[col].mean().rename(columns={col: '_t2'})
    if len(unl) and len(l2):
        near = pd.merge_asof(unl[keys + ['log_size']].reset_index().sort_values('log_size'), l2.sort_values('log_size'), on='log_size', by=keys, direction='nearest').set_index('index')['_t2']
        use = near.dropna().index
        val.loc[use] = near.loc[use]
        tier.loc[use] = 2
    # tier 3
    t3 = lab.groupby(['brand_descr', 'product_module_code'])[col].mean().rename('_t3')
    m3 = d[['brand_descr', 'product_module_code']].merge(t3, left_on=['brand_descr', 'product_module_code'], right_index=True, how='left')['_t3']
    use = val.isna() & m3.notna()
    val[use] = m3[use]
    tier[use] = 3
    # tier 4: spending-weighted module mean
    w = spend.reindex(lab['upc_13']).fillna(0).values + 1e-6
    t4 = (lab[col].values * w)
    t4 = pd.Series(t4).groupby(lab['product_module_code'].values).sum() / pd.Series(w).groupby(lab['product_module_code'].values).sum()
    m4 = d['product_module_code'].map(t4)
    use = val.isna() & m4.notna()
    val[use] = m4[use]
    tier[use] = 4
    # tier 5
    t5 = lab.groupby('product_group_code')[col].mean()
    m5 = d['product_group_code'].map(t5)
    use = val.isna() & m5.notna()
    val[use] = m5[use]
    tier[use] = 5
    return val, tier


def main(source):
    t0 = time.time()
    path, key, out_path, nuts = SOURCES[source]
    prod = pd.read_csv(rdir / 'products.tsv', sep='\t', encoding='latin-1', dtype={'upc': str},
                       usecols=['upc', 'upc_ver_uc', 'upc_descr', 'product_module_code', 'product_module_descr', 'product_group_code', 'brand_descr', 'multi', 'size1_amount', 'size1_units'])
    prod = prod.sort_values(['upc', 'upc_ver_uc']).drop_duplicates('upc', keep='last')
    prod['upc_13'] = '0' + prod['upc'].str.zfill(12)
    us = pd.read_parquet(idir / 'health' / 'upc_year_spending.parquet', columns=['upc', 'product_module_code', 'spending'])
    us['upc_13'] = '0' + us['upc'].astype(str).str.zfill(12)
    spend = us.groupby('upc_13')['spending'].sum()
    food_modules = set(us['product_module_code'].dropna().astype(int))
    prod = prod[prod['product_module_code'].isin(food_modules)].copy()
    print(f'{len(prod):,} food UPCs ({time.time() - t0:.0f}s)')
    cols = [f'{n}_per_100g' for n in nuts]
    if source == 'syndigo':
        wide = pd.read_parquet(path, columns=[key] + [c for c in cols if c != 'addsugar_per_100g']).drop_duplicates(key)
        wide = wide.merge(pd.read_parquet(SYNDIGO_ADDSUGAR, columns=['upc', 'addsugar_per_100g']), on='upc', how='left')[[key] + cols]
    else:
        wide = pd.read_parquet(path, columns=[key] + cols)
    wide = wide.rename(columns={key: 'upc_13'}).drop_duplicates('upc_13')
    # impossible labels
    n_dense = int((wide['cal_per_100g'] > CAPS['cal']).sum())
    wide.loc[wide['cal_per_100g'] > CAPS['cal'], cols] = np.nan
    for n in nuts:
        c = f'{n}_per_100g'
        wide.loc[(wide[c] > CAPS[n]) | (wide[c] < 0), c] = np.nan
    over_total = wide['satfat_per_100g'] > wide['totfat_per_100g']
    wide.loc[over_total, 'satfat_per_100g'] = wide.loc[over_total, 'totfat_per_100g']
    implied = 1.2 * wide['cal_per_100g'] + 5
    no_sat = 9 * wide['satfat_per_100g'] > implied
    wide.loc[no_sat, 'satfat_per_100g'] = np.nan
    no_sugar = 4 * wide['sugar_per_100g'] > implied
    wide.loc[no_sugar, 'sugar_per_100g'] = np.nan
    if 'addsugar_per_100g' in wide.columns:
        wide.loc[4 * wide['addsugar_per_100g'] > implied, 'addsugar_per_100g'] = np.nan
    print(f'labels above 900 kcal/100 g: {n_dense:,}; saturated fat set to total fat: {int(over_total.sum()):,}; saturated fat dropped: {int(no_sat.sum()):,}; sugar dropped: {int(no_sugar.sum()):,}')
    fats = [c for c in ('mufat_per_100g', 'pufat_per_100g', 'mofat_per_100g', 'pofat_per_100g', 'satfat_per_100g') if c in wide.columns]
    fat3 = wide[fats].sum(axis=1, min_count=1)
    bad = ((9 * fat3 > 1.1 * wide['cal_per_100g']) | (fat3 > wide['totfat_per_100g'] + 1)).fillna(False)
    wide.loc[bad, [c for c in fats if c != 'satfat_per_100g']] = np.nan
    print(f'{len(wide):,} labels; fat fields dropped as impossible on {int(bad.sum()):,}')
    d = prod.merge(wide, on='upc_13', how='left')
    d['descr_norm'] = d['upc_descr'].fillna('').str.strip().str.replace(r'\s+\d+P$', '', regex=True).str.replace(r'\s+', ' ', regex=True)
    # false matches and wrong serving grams: far from the module's usual density
    med = d.groupby('product_module_code')['cal_per_100g'].transform('median')
    far = (d['cal_per_100g'] > np.maximum(5 * med, med + 200)) | ((d['cal_per_100g'] > 0) & (d['cal_per_100g'] < med / 20) & (med - d['cal_per_100g'] > 100))
    print(f'labels far from their module: {int(far.sum()):,}; of labelled spending {d.loc[far, "upc_13"].map(spend).sum() / d.loc[d["cal_per_100g"].notna(), "upc_13"].map(spend).sum():.4f}')
    print(d[far].assign(sp=d.loc[far, 'upc_13'].map(spend)).sort_values('sp', ascending=False).head(15)[['product_module_descr', 'brand_descr', 'cal_per_100g', 'sp']].round(1).to_string(index=False))
    d.loc[far, cols] = np.nan
    # a line missing from a label counts as zero where the module's labels that report the line are near zero. Allcott et al.
    # do this everywhere (replace `var'=0 if `var'==.). Labels leave out zero lines, and filling them from relatives spreads
    # one bad label across a brand (one Dr Pepper label with 7.9 g of fat). On UPCs with both labels, by spending: where Syndigo
    # has no line, USDA reports zero for 84% of sugar, 96% of saturated fat, 97% of cholesterol, 88% of fiber, and 3% of sodium
    # and 55% of total fat, which stay with the relatives. Elsewhere a missing line is a gap in the data, as for cookies
    labelled = d['cal_per_100g'].notna()
    for n, near_zero in (('sugar', 1.0), ('satfat', 0.5), ('chol', 0.005), ('fiber', 0.5)):
        c = f'{n}_per_100g'
        zero = labelled & d[c].isna() & (d.groupby('product_module_code')[c].transform('median') <= near_zero)
        d.loc[zero, c] = 0.0
        print(f'  {c}: missing line set to zero on {int(zero.sum()):,} labels; still missing on {int((labelled & d[c].isna()).sum()):,}')
    nova = pd.read_parquet(idir / 'usda_nielsen_merged' / 'usda_nova.parquet', columns=['upc13', 'nova']).rename(columns={'upc13': 'upc_13'}).drop_duplicates('upc_13')
    d = d.merge(nova, on='upc_13', how='left')
    d['nova'] = d['nova'].astype('float')
    d['log_size'] = np.log(pd.to_numeric(d['size1_amount'], errors='coerce').where(lambda x: x > 0))
    keys = ['brand_descr', 'product_module_code', 'size1_units']
    d = d.reset_index(drop=True)
    tiers = {}
    for c in cols:
        d[c], tiers[c] = fill_tiers(d, c, keys, spend)
        print(f'  {c}: own {int((tiers[c] == 1).sum()):,}; tiers {tiers[c].value_counts().sort_index().to_dict()}; missing {int(d[c].isna().sum()):,} ({time.time() - t0:.0f}s)', flush=True)
    d['cal_tier'] = tiers['cal_per_100g']
    # commodity calories replace imputed values only
    ref = d['product_module_descr'].map(COMMODITY_KCAL)
    use = ref.notna() & (d['cal_tier'] != 1)
    d.loc[use, 'cal_per_100g'] = ref[use]
    d.loc[use, 'cal_tier'] = 6
    # NOVA
    w = d['upc_13'].map(spend).fillna(0)
    nv = d['nova'].copy()
    ntier = pd.Series(np.where(nv.notna(), 1, np.nan), index=d.index)
    lab = d[nv.notna()]
    sm = lab[lab['descr_norm'] != ''].groupby(['product_module_code', 'descr_norm', 'nova']).size().rename('n').reset_index().sort_values('n').drop_duplicates(['product_module_code', 'descr_norm'], keep='last')
    ms = d[['product_module_code', 'descr_norm']].merge(sm, on=['product_module_code', 'descr_norm'], how='left')['nova']
    use = nv.isna() & ms.notna()
    nv[use] = ms[use]
    ntier[use] = 2
    unl = d[nv.isna() & d['log_size'].notna() & d[keys].notna().all(axis=1)]
    atsize = lab[lab['log_size'].notna()].dropna(subset=keys).groupby(keys + ['log_size', 'nova'], observed=True).size().rename('n').reset_index().sort_values('n').drop_duplicates(keys + ['log_size'], keep='last')
    near = pd.merge_asof(unl[keys + ['log_size']].reset_index().sort_values('log_size'),
                         atsize[keys + ['log_size', 'nova']].rename(columns={'nova': '_n'}).sort_values('log_size'),
                         on='log_size', by=keys, direction='nearest').set_index('index')['_n'].dropna()
    nv.loc[near.index] = near
    ntier.loc[near.index] = 2
    b = lab.groupby(['brand_descr', 'product_module_code', 'nova']).size().rename('n').reset_index().sort_values('n').drop_duplicates(['brand_descr', 'product_module_code'], keep='last')
    mb = d[['brand_descr', 'product_module_code']].merge(b, on=['brand_descr', 'product_module_code'], how='left')['nova']
    use = nv.isna() & mb.notna()
    nv[use] = mb[use]
    ntier[use] = 3
    lw = lab.assign(w=lab['upc_13'].map(spend).fillna(0) + 1e-6)
    mm = lw.groupby(['product_module_code', 'nova'])['w'].sum().reset_index().sort_values('w').drop_duplicates('product_module_code', keep='last').set_index('product_module_code')['nova']
    m4 = d['product_module_code'].map(mm)
    use = nv.isna() & m4.notna()
    nv[use] = m4[use]
    ntier[use] = 4
    d['nova_own'] = d['nova']
    d['nova'] = nv
    d['nova_tier'] = ntier
    out = d[['upc_13', 'product_module_code', 'product_group_code', 'brand_descr', 'multi', 'cal_tier', 'nova', 'nova_own', 'nova_tier'] + cols]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    print('UPCs by calorie tier:', out['cal_tier'].value_counts(dropna=False).sort_index().to_dict())
    print('UPCs by NOVA tier:', out['nova_tier'].value_counts(dropna=False).sort_index().to_dict())
    print(f'saved {out_path} ({time.time() - t0:.0f}s)')


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'usda')
