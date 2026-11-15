"""Barcode health measures from each barcode's OWN Syndigo label (no values copied from related barcodes), for E2 and E3.

Reads  interim/syndigo_nielsen_merged/syndigo_wide.parquet and syndigo_added_sugar.parquet, raw/products.tsv,
       interim/health/upc_year_spending.parquet (food modules), interim/variety/jaravel_food/organic_upc.parquet.
Writes interim/variety/jaravel_food/upc_health.parquet (upc, module, group, brand, description, cal_per_100g, addsugar_1000,
       sugar_1000, hi_1000, produce, organic).
Labels are cleaned with the rules of build/health/build_upc_dictionary.py (caps; saturated fat <= total fat; sugar and
saturated fat within the calories they imply; labels far from their module's density dropped; a missing sugar / saturated
fat / cholesterol / fiber line set to zero where the module's reporting labels are near zero); its tiers that fill values
from relatives are NOT applied.  Measures per 1,000 kcal are missing when one package has 1 kcal or less
(utils/food_measures.MIN_PACKAGE_KCAL).  Health Index per 1,000 kcal: GetHealthIndex.do formula as in
analyze/what_moves/build_rms_upc_groups.py (fruit and vegetables at fixed values); it needs own sugar, saturated fat and
sodium lines (fiber and cholesterol: missing = 0).  Organic: 10_build_organic_flag.py (missing when never coded).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403
from utils.food_measures import package_grams, is_fruit, is_veg, MIN_PACKAGE_KCAL  # noqa: E402

nuts = ['cal', 'sugar', 'satfat', 'chol', 'sodium', 'fiber', 'addsugar', 'totfat']
CAPS = {'cal': 900, 'sugar': 100, 'addsugar': 100, 'satfat': 100, 'totfat': 100, 'fiber': 100, 'sodium': 40, 'chol': 3}
cols = [f'{n}_per_100g' for n in nuts]

prod = pd.read_csv(rdir / 'products.tsv', sep='\t', encoding='latin-1', dtype={'upc': str}, quoting=3,
                   usecols=['upc', 'upc_ver_uc', 'upc_descr', 'product_module_code', 'product_group_code', 'brand_descr',
                            'multi', 'size1_amount', 'size1_units'])
prod = prod.sort_values(['upc', 'upc_ver_uc']).drop_duplicates('upc', keep='last')
prod['upc_13'] = '0' + prod['upc'].str.zfill(12)
us = pd.read_parquet(idir / 'health' / 'upc_year_spending.parquet', columns=['product_module_code'])
prod = prod[prod['product_module_code'].isin(set(us['product_module_code'].dropna().astype(int)))].copy()

wide = pd.read_parquet(idir / 'syndigo_nielsen_merged' / 'syndigo_wide.parquet',
                       columns=['upc'] + [c for c in cols if c != 'addsugar_per_100g']).drop_duplicates('upc')
wide = wide.merge(pd.read_parquet(idir / 'syndigo_nielsen_merged' / 'syndigo_added_sugar.parquet', columns=['upc', 'addsugar_per_100g']),
                  on='upc', how='left').rename(columns={'upc': 'upc_13'})[['upc_13'] + cols]
wide.loc[wide['cal_per_100g'] > CAPS['cal'], cols] = np.nan
for n in nuts:
    c = f'{n}_per_100g'
    wide.loc[(wide[c] > CAPS[n]) | (wide[c] < 0), c] = np.nan
over = wide['satfat_per_100g'] > wide['totfat_per_100g']
wide.loc[over, 'satfat_per_100g'] = wide.loc[over, 'totfat_per_100g']
implied = 1.2 * wide['cal_per_100g'] + 5
wide.loc[9 * wide['satfat_per_100g'] > implied, 'satfat_per_100g'] = np.nan
wide.loc[4 * wide['sugar_per_100g'] > implied, 'sugar_per_100g'] = np.nan
wide.loc[4 * wide['addsugar_per_100g'] > implied, 'addsugar_per_100g'] = np.nan
d = prod.merge(wide, on='upc_13', how='left')
med = d.groupby('product_module_code')['cal_per_100g'].transform('median')
far = (d['cal_per_100g'] > np.maximum(5 * med, med + 200)) | ((d['cal_per_100g'] > 0) & (d['cal_per_100g'] < med / 20) & (med - d['cal_per_100g'] > 100))
d.loc[far, cols] = np.nan
labelled = d['cal_per_100g'].notna()
for n, near_zero in (('sugar', 1.0), ('satfat', 0.5), ('chol', 0.005), ('fiber', 0.5)):
    c = f'{n}_per_100g'
    zero = labelled & d[c].isna() & (d.groupby('product_module_code')[c].transform('median') <= near_zero)
    d.loc[zero, c] = 0.0
# added sugar cannot exceed total sugar by more than rounding
d.loc[d['addsugar_per_100g'] > d['sugar_per_100g'] + 0.5, 'addsugar_per_100g'] = np.nan

d['grams_pkg'] = package_grams(d)
pkg_kcal = d['grams_pkg'] * d['cal_per_100g'] / 100
ok = labelled & (pkg_kcal > MIN_PACKAGE_KCAL) & (d['cal_per_100g'] > 0)
d['addsugar_1000'] = np.where(ok, d['addsugar_per_100g'] / d['cal_per_100g'] * 1000, np.nan)
d['sugar_1000'] = np.where(ok, d['sugar_per_100g'] / d['cal_per_100g'] * 1000, np.nan)
fruit = is_fruit(d['product_module_code'], d['product_group_code']).astype(bool)
veg = is_veg(d['product_module_code'], d['product_group_code']).astype(bool)
have = d[['sugar_per_100g', 'satfat_per_100g', 'sodium_per_100g']].notna().all(axis=1)
hi_100g = np.where(fruit | veg, fruit * 100 / 320 + veg * 100 / 390,
                   d['fiber_per_100g'].fillna(0) / 29.5 - d['sugar_per_100g'] / 32.8 - d['satfat_per_100g'] / 17.2
                   - d['sodium_per_100g'] / 2.3 - d['chol_per_100g'].fillna(0) / 0.3)
d['hi_1000'] = np.where(ok & (have | fruit | veg), hi_100g / d['cal_per_100g'] * 1000, np.nan)
d['produce'] = (fruit | veg).astype(int)

org = pd.read_parquet(DATA / 'organic_upc.parquet')[['upc', 'coded', 'organic']]
d['upc'] = d['upc'].astype('int64')
d = d.merge(org, on='upc', how='left')
d['organic'] = np.where(d['coded'] == True, d['organic'], np.nan)  # noqa: E712
out = d[['upc', 'product_module_code', 'product_group_code', 'brand_descr', 'upc_descr', 'cal_per_100g', 'addsugar_1000',
         'sugar_1000', 'hi_1000', 'produce', 'organic']]
out.to_parquet(DATA / 'upc_health.parquet', index=False)
print(f"food UPCs {len(out):,}; own label calories {labelled.sum():,}; added sugar {out.addsugar_1000.notna().sum():,}; "
      f"sugar {out.sugar_1000.notna().sum():,}; HI {out.hi_1000.notna().sum():,}; organic coded {out.organic.notna().sum():,} "
      f"(organic {int(out.organic.sum()):,}); sodium missing among labelled {(labelled & d.sodium_per_100g.isna()).mean() / labelled.mean():.3f}")
print(out[['addsugar_1000', 'sugar_1000', 'hi_1000']].describe(percentiles=[.01, .25, .5, .75, .99]).T.round(2).to_string())
