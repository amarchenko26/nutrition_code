"""Minutes of preparation per package and per 1,000 kcal, for every food barcode.

Reads  interim/prep_time/upc_prep_minutes.parquet (minutes per cooking session and basis, build_prep_minutes_upc.py)
       interim/prep_time/upc_base.parquet (package size, units bought)
       raw/products.tsv (product group code, for the package-grams rules)
       interim/health/upc_dictionary.parquet (calorie density per barcode)
       interim/usda_nielsen_merged/usda_wide.parquet (label serving size in g or ml)
Writes interim/prep_time/upc_prep_package.parquet, one row per barcode:
       min_session, basis   from the rules
       grams_pkg            grams in one package (utils.food_measures.package_grams)
       serv_g, serv_own     the label serving in grams; serv_own = the barcode's own USDA label, otherwise the median of its
                            module, otherwise of its product group
       servings_pkg         grams_pkg / serv_g
       units_pkg            for basis 'pkg': how many separately cooked units the barcode holds. Nielsen's multi (a 5-pack of
                            macaroni and cheese boxes) times the count in the description (3'S = three pizzas) when each of
                            those is at least half the size of the module's usual single package; a tray of 6 patties (6'S,
                            each far smaller than a usual package) stays one unit. 1 for basis 'serv4'.
       sessions             cooking sessions one package makes under the MAIN rule (Anya 2026-11-18): four label servings per
                            session, at least one session per package (basis 'serv4'); one session per unit for packages that
                            are cooked whole (basis 'pkg': whole birds, roasts, cake mixes, frozen pizzas, ...)
       min_pkg              min_session x sessions
       min_pkg_serv4        the same with four label servings per session for EVERY barcode (high check)
       min_pkg_once         the same with one session per unit for EVERY barcode (low check)
       kcal_pkg             grams_pkg x the dictionary density / 100
A barcode without a package size in grams makes one session per package; its kcal_pkg is missing, so the household step leaves
it out of both the minutes and the calories of minutes per 1,000 kcal.
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

from utils.food_measures import package_grams, POWDER_MODULES, POWDER_DENSITY_MIN, GRAMS_PER_QT_PREPARED

OUT = idir / 'prep_time' / 'upc_prep_package.parquet'
SERVINGS = 4                      # label servings per cooking session
SERVING_UNITS = ['g', 'GRM', 'GM', 'ml', 'MLT']

m = pd.read_parquet(idir / 'prep_time' / 'upc_prep_minutes.parquet')
b = pd.read_parquet(idir / 'prep_time' / 'upc_base.parquet', columns=['upc', 'multi', 'size1_amount', 'size1_units', 'spending', 'quantity', 'module', 'descr'])
d = m.merge(b, on='upc', how='left')
grp = pd.read_csv(rdir / 'products.tsv', sep='\t', dtype=str, encoding='latin-1', quoting=3, usecols=['product_module_code', 'product_group_code'])
grp = grp.dropna().drop_duplicates('product_module_code').astype({'product_module_code': 'int64', 'product_group_code': 'int64'})
d = d.merge(grp, on='product_module_code', how='left')
d['upc_13'] = '0' + d['upc']
d = d.merge(pd.read_parquet(idir / 'health' / 'upc_dictionary.parquet', columns=['upc_13', 'cal_per_100g']), on='upc_13', how='left')
d['grams_pkg'] = package_grams(d)
d['kcal_pkg'] = d['grams_pkg'] * d['cal_per_100g'] / 100

lab = pd.read_parquet(idir / 'usda_nielsen_merged' / 'usda_wide.parquet', columns=['upc13', 'serving_size', 'serving_size_unit'])
lab = lab[lab['serving_size_unit'].isin(SERVING_UNITS) & (lab['serving_size'] > 0)]
lab = lab.groupby('upc13', as_index=False)['serving_size'].median().rename(columns={'upc13': 'upc_13', 'serving_size': 'serv_g'})
d = d.merge(lab, on='upc_13', how='left')
d['serv_own'] = d['serv_g'].notna()
d['serv_g'] = d['serv_g'].fillna(d.groupby('product_module_code')['serv_g'].transform('median'))
d['serv_g'] = d['serv_g'].fillna(d.groupby('product_group_code')['serv_g'].transform('median'))
# powdered drinks sized in prepared quarts with a per-drink label: 4 servings of 8 fl oz per quart (as build_module_time_price.py)
prepared = d['product_module_code'].isin(POWDER_MODULES) & (d['size1_units'].astype(str).str.strip() == 'QT') & (d['cal_per_100g'] < POWDER_DENSITY_MIN)
d.loc[prepared, 'serv_g'] = GRAMS_PER_QT_PREPARED / 4

d['servings_pkg'] = d['grams_pkg'] / d['serv_g']
# powdered drinks: a quart is four servings of 8 fl oz whatever the powder weighs (a sugar-free powder's label serving is about
# 1 g, while the package grams of a quart-sized canister are those of a sweetened powder); packages sized by weight: at most 35
# quarts a unit, the largest canisters sold
powder = d['product_module_code'].isin(POWDER_MODULES).values
qt = powder & (d['size1_units'].astype(str).str.strip() == 'QT').values
d.loc[qt, 'servings_pkg'] = (d['size1_amount'] * d['multi'].fillna(1).clip(lower=1) * 4)[qt]
d.loc[powder & ~qt, 'servings_pkg'] = np.minimum(d['servings_pkg'], 140 * d['multi'].fillna(1).clip(lower=1))[powder & ~qt]
by_serv = np.maximum(1.0, (d['servings_pkg'] / SERVINGS).fillna(1.0).values)

# units in a package that is cooked whole: multi x the count written as N'S, where each is a full-size package
pkg = (d['basis'] == 'pkg').values
multi = d['multi'].fillna(1).clip(lower=1).values
n_descr = pd.to_numeric(d['descr'].str.extract(r"\b(\d{1,2})'S\b")[0], errors='coerce').values
single = pkg & (multi == 1) & np.isnan(n_descr) & (d['grams_pkg'] > 0).values
usual = d.loc[single].groupby('product_module_code')['grams_pkg'].agg(['median', 'size'])
usual = usual.loc[usual['size'] >= 5, 'median']
usual_g = d['product_module_code'].map(usual).values
each = d['grams_pkg'].values / (multi * n_descr)
count = np.where((n_descr >= 2) & (each >= 0.5 * usual_g), n_descr, 1.0)
d['units_pkg'] = np.where(pkg, multi * count, 1.0)

d['sessions'] = np.where(pkg, d['units_pkg'].values, by_serv)
d['min_pkg'] = d['min_session'] * d['sessions']
d['min_pkg_serv4'] = d['min_session'] * by_serv
d['min_pkg_once'] = d['min_session'] * d['units_pkg']

cols = ['upc', 'product_module_code', 'min_session', 'basis', 'grams_pkg', 'serv_g', 'serv_own', 'servings_pkg', 'units_pkg', 'sessions', 'min_pkg',
        'min_pkg_serv4', 'min_pkg_once', 'kcal_pkg']
d[cols].to_parquet(OUT, index=False)

q = d['quantity'].fillna(0).values
ok = (d['kcal_pkg'] > 0).values
kc = (q * d['kcal_pkg'].fillna(0).values)[ok].sum()
print(f'{len(d):,} barcodes; packages bought with calories and a size: {100 * q[ok].sum() / q.sum():.1f}%')
print(f'own label serving: {100 * (q * d.serv_own.values).sum() / q.sum():.1f}% of packages; cooked barcodes (minutes > 0): {100 * q[d.min_session.values > 0].sum() / q.sum():.1f}% of packages')
mu = pkg & (d['units_pkg'].values > 1)
print(f"packages cooked whole that hold several units: {mu.sum():,} barcodes, {100 * q[mu].sum() / max(q[pkg].sum(), 1):.1f}% of the packages cooked whole")
for c, name in [('min_pkg', 'main rule'), ('min_pkg_serv4', 'four servings per session for every barcode'), ('min_pkg_once', 'one session per package for every barcode')]:
    print(f'minutes per 1,000 kcal, all purchases 2004-2020, {name}: {1000 * (q * d[c].values)[ok].sum() / kc:.2f}')
top = d[ok].assign(mins=(q * d['min_pkg'].values)[ok]).groupby('module')['mins'].sum().sort_values(ascending=False)
print('modules with the most minutes (% of all minutes):', '; '.join(f'{k} {100 * v / top.sum():.1f}' for k, v in top.head(15).items()))
print(f'saved {OUT}')
