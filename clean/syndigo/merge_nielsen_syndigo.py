"""Turn Syndigo labels into nutrients per 100 g, on the same grams as Nielsen purchases.

Reads interim/syndigo/syndigo_nutrients_master.parquet (clean_syndigo.py), raw/products.tsv and
interim/health/edible_factor.parquet (build/health/build_package_params.py).
Writes interim/syndigo_nielsen_merged/syndigo_wide.parquet: one row per UPC, nutrients per 100 g, package grams
g_total and serving grams g_serving_size.

Syndigo is the current name of Gladson, the label files of Allcott et al. (2019), in the same layout. Grams per serving
follow their prepare_gladson_nut_facts_for_imputation.do. In order:
  1  the serving as stated, when stated in a unit of weight or fluid ('8 fl oz', '30 g');
  2  the package over the servings per container. The package is Nielsen's size x multi (x the label-basis factor of
     utils/food_measures.py), or Syndigo's own item size when Nielsen's is a count. When multi is above 1 the label may
     count servings per pack or per unit: the reading whose density is nearer the module's median is used;
  3  one unit of the package, when the serving is '1 can', '1 bar' and so on and servings per container are missing;
  4  a serving stated in cups, tablespoons or teaspoons x the module's grams per cup, the median over the module's labels
     where step 2 or 3 gives the grams. Taking a cup as water makes a cup of cereal 237 g and not 40 g.
Otherwise the label has no grams and gives no values: build_upc_dictionary.py fills them from the product's relatives.

Until 2026-11-05 this file took grams per serving as Syndigo's item size over servings per container, then the serving
as stated with a cup as water. Multipacks came out 4 to 24 times too dense (a can's weight over a pack's servings) and
cereal five times too thin. Comparison with USDA labels of the same UPC, calories per 100 g within 25%: 79% of labels
before, 91% after; multipacks 34% and 87%; ready-to-eat cereal 31% and 97%; carbonated soda 72% and 91%.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.food_measures import UNIT_TO_GRAMS

import numpy as np
import pandas as pd

SYNDIGO_PATH = idir / 'syndigo' / 'syndigo_nutrients_master.parquet'
OUTPUT_DIR = idir / 'syndigo_nielsen_merged'

NUTRIENT_COL_MAP = {
    'Calories':            'cal_per_100g',
    'Total Fat':           'totfat_per_100g',
    'Saturated Fat':       'satfat_per_100g',
    'Polyunsaturated Fat': 'pofat_per_100g',
    'Monounsaturated Fat': 'mofat_per_100g',
    'Cholesterol':         'chol_per_100g',
    'Sodium':              'sodium_per_100g',
    'Dietary Fiber':       'fiber_per_100g',
    'Sugars':              'sugar_per_100g',
}

# grams per stated unit, weight or fluid
SERVING_GRAMS = {'oz': 28.3495, 'ounce': 28.3495, 'ounces': 28.3495, 'oz dry': 28.3495, '0z': 28.3495, 'lb': 453.592, 'lbs': 453.592,
                 'g': 1.0, 'gr': 1.0, 'gm': 1.0, 'gram': 1.0, 'grams': 1.0, 'kg': 1000.0,
                 'fl oz': 29.5735, 'floz': 29.5735, 'fl.oz': 29.5735, 'f. oz': 29.5735, 'fl': 29.5735, 'fluid ounce': 29.5735, 'fluid ounces': 29.5735,
                 'ml': 1.0, 'cc': 1.0, 'cl': 10.0, 'l': 1000.0, 'lt': 1000.0, 'ltr': 1000.0, 'liter': 1000.0,
                 'pt': 473.176, 'pint': 473.176, 'qt': 946.353, 'quart': 946.353, 'gal': 3785.41}
# cups per stated unit
SERVING_CUPS = {'cup': 1.0, 'cups': 1.0, 'cu': 1.0, 'cup dry': 1.0, 'cup dry mix': 1.0, 'cup mix': 1.0, 'cup condensed soup': 1.0,
                'tbsp': 1 / 16, 'tbs': 1 / 16, 'tablespoon': 1 / 16, 'tablespoons': 1 / 16, 'tbsp unpopped': 1 / 16,
                'tsp': 1 / 48, 'teaspoon': 1 / 48, 'teaspoons': 1 / 48}
ONE_UNIT = {'bar', 'bars', 'bottle', 'can', 'container', 'jar', 'meal', 'package', 'pkg', 'pckg', 'pouch', 'packet', 'envelope', 'carton', 'tub',
            'each', 'ea', 'pack', 'bag', 'box'}
MIN_LABELS_PER_CUP = 5


def plausible(s):
    return s.where((s > 0) & (s < 20000))


def serving_grams(u, edible):
    """u: one row per UPC. Adds pack_g, g_serving, serving_source."""
    uom = u['servingsizeuom'].astype(str).str.strip().str.lower().str.replace(r'\s+', ' ', regex=True)
    amount = pd.to_numeric(u['servingsizetext'], errors='coerce').abs()
    servings = u['servingspercontainer']
    multi = u['multi'].fillna(1)
    module = u['product_module_code']
    factor = module.map(edible).fillna(1.0)
    unit_g = (u['size1_amount'] * u['size1_units'].map(UNIT_TO_GRAMS) * factor).fillna(u['g_total'])
    u['pack_g'] = unit_g * multi

    s_stated = plausible(amount * uom.map(SERVING_GRAMS))
    s_pack = plausible(u['pack_g'] / servings)
    s_unit = plausible(unit_g / servings)
    s_one = plausible(unit_g.where(servings.isna() & (amount == 1) & uom.isin(ONE_UNIT)))

    # a multipack's servings may count the pack or one unit
    kcal = u['kcal_serving']
    safe = (kcal / s_stated * 100).where(lambda x: x.between(0, 900))
    safe = safe.fillna((kcal / s_pack * 100).where(lambda x: x.between(0, 900) & (multi == 1)))
    med = safe.where(kcal > 0).groupby(module).transform('median')
    gap_pack = np.abs(np.log((kcal / s_pack * 100 + 1) / (med + 1)))
    gap_unit = np.abs(np.log((kcal / s_unit * 100 + 1) / (med + 1)))
    s_pack = s_pack.where(~((multi > 1) & (kcal > 0) & (gap_unit < gap_pack)), s_unit)

    # servings in cups: the module's grams per cup
    cups = amount * uom.map(SERVING_CUPS)
    per_cup = (s_pack.fillna(s_one) / cups).where(lambda x: x.between(5, 1200))
    by_module = per_cup.groupby(module).agg(['median', 'count'])
    by_module = by_module.loc[by_module['count'] >= MIN_LABELS_PER_CUP, 'median']
    s_cups = plausible(cups * module.map(by_module))

    u['g_serving'] = s_stated.fillna(s_pack).fillna(s_one).fillna(s_cups)
    u['serving_source'] = np.select([s_stated.notna(), s_pack.notna(), s_one.notna(), s_cups.notna()],
                                    ['stated', 'package', 'one unit', 'cups'], default='none')
    return u


def main():
    print('NIELSEN-SYNDIGO MERGE')
    syndigo = pd.read_parquet(SYNDIGO_PATH)
    print(f'  {syndigo["upc"].nunique():,} UPCs, {len(syndigo):,} rows')

    prod = pd.read_csv(rdir / 'products.tsv', sep='\t', encoding='latin-1', low_memory=False, dtype={'upc': str},
                       usecols=['upc', 'upc_ver_uc', 'product_module_code', 'multi', 'size1_amount', 'size1_units'])
    prod = prod.sort_values(['upc', 'upc_ver_uc']).drop_duplicates('upc', keep='last')
    prod['upc'] = '0' + prod['upc'].str.zfill(12)
    prod['multi'] = pd.to_numeric(prod['multi'], errors='coerce').fillna(1).clip(lower=1)
    prod['size1_amount'] = pd.to_numeric(prod['size1_amount'], errors='coerce')
    prod['size1_units'] = prod['size1_units'].astype(str).str.strip()
    edible = pd.read_parquet(idir / 'health' / 'edible_factor.parquet').set_index('product_module_code')['factor']

    kcal = syndigo[syndigo['nutrient_id'] == 1].drop_duplicates('upc').set_index('upc')['g_nut_per_serving']
    u = syndigo.drop_duplicates('upc')[['upc', 'g_total', 'servingspercontainer', 'servingsizetext', 'servingsizeuom']].copy()
    u['kcal_serving'] = u['upc'].map(kcal)
    u = u.merge(prod, on='upc', how='left')
    print(f'  UPCs in Nielsen products: {u["product_module_code"].notna().sum():,}')
    u = serving_grams(u, edible)
    print('  grams per serving by source:', u['serving_source'].value_counts().to_dict())

    merged = syndigo.drop(columns=['g_total', 'g_serving_size', 'nut_per_100g']).merge(u[['upc', 'pack_g', 'g_serving']], on='upc', how='left')
    merged['nut_per_100g'] = merged['g_nut_per_serving'] / merged['g_serving'] * 100

    wide = merged[merged['nutrient'].isin(NUTRIENT_COL_MAP)].pivot_table(index='upc', columns='nutrient', values='nut_per_100g', aggfunc='first')
    wide.columns = [NUTRIENT_COL_MAP[c] for c in wide.columns]
    wide = wide.reset_index().merge(u[['upc', 'pack_g', 'g_serving']].rename(columns={'pack_g': 'g_total', 'g_serving': 'g_serving_size'}), on='upc', how='left')

    # nutrients are in g/100 g; these indicate unit misclassification
    wide.loc[wide['sodium_per_100g'] > 5, 'sodium_per_100g'] = np.nan
    wide.loc[wide['chol_per_100g'] > 2, 'chol_per_100g'] = np.nan

    print(f'  {len(wide):,} UPCs, columns: {list(wide.columns)}')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(OUTPUT_DIR / 'syndigo_wide.parquet', index=False)
    print(f'  saved {OUTPUT_DIR / "syndigo_wide.parquet"}')


if __name__ == '__main__':
    main()
