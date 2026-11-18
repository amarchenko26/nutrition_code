"""Food barcodes bought by the consumer panel, with everything the preparation rules read -- one row per barcode.

Reads  interim/health/upc_year_spending.parquet (barcode x year spending and units bought, 2004-2020)
       raw/products.tsv (description, brand, module, group, size)
       interim/products_extra/products_extra_upc.parquet (Nielsen's attributes, clean/nielsen/clean_products_extra.py)
       interim/rms_variety/prep_time_scores.parquet (the module score of build_prep_time_index.py, kept for comparison)
Writes interim/prep_time/upc_base.parquet: upc (12 characters), product_module_code, module, group, descr (the abbreviated
       description), brand, multi, size1_amount, size1_units, spending and quantity (2004-2020 totals), the attributes type,
       style, form, use, product, variety, formula, flavor, container, common_consumer_name (upper case; missing if not
       stated), old_score (1-9; missing for modules the old score left out) and old_why.

A barcode bought under two module codes over the years keeps the module with more of its spending. A barcode number that was
reused for another product has several versions in raw/products.tsv, each with its own module, description and attributes
(common among store brands): the version kept is the latest one filed under the module the barcode was bought in, and the
latest version of all if none is.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import pandas as pd

OUT = idir / 'prep_time' / 'upc_base.parquet'
FIELDS = ['type', 'style', 'form', 'use', 'product', 'variety', 'formula', 'flavor', 'container', 'common_consumer_name']

s = pd.read_parquet(idir / 'health' / 'upc_year_spending.parquet', columns=['upc', 'product_module_code', 'spending', 'quantity'])
s = s.groupby(['upc', 'product_module_code'], as_index=False)[['spending', 'quantity']].sum()
tot = s.groupby('upc', as_index=False)[['spending', 'quantity']].sum()
s = s.sort_values('spending').drop_duplicates('upc', keep='last')[['upc', 'product_module_code']].merge(tot, on='upc')

p = pd.read_csv(rdir / 'products.tsv', sep='\t', dtype=str, encoding='latin-1', quoting=3,
                usecols=['upc', 'upc_ver_uc', 'upc_descr', 'product_module_code', 'product_module_descr', 'product_group_descr',
                         'brand_descr', 'multi', 'size1_amount', 'size1_units'])
names = p.drop_duplicates('product_module_code')[['product_module_code', 'product_module_descr', 'product_group_descr']]
names['product_module_code'] = pd.to_numeric(names['product_module_code'], errors='coerce')
names = names.dropna(subset=['product_module_code']).astype({'product_module_code': 'int64'})
p['upc'] = p['upc'].str.zfill(12)
p['upc_ver_uc'] = p['upc_ver_uc'].astype(int)
p['p_module'] = pd.to_numeric(p['product_module_code'], errors='coerce')
p = p.merge(s[['upc', 'product_module_code']].rename(columns={'product_module_code': 'bought_module'}), on='upc', how='inner')
p['same'] = (p['p_module'] == p['bought_module']).astype(int)
p = p.sort_values(['upc', 'same', 'upc_ver_uc']).drop_duplicates('upc', keep='last')
print(f"barcodes with a version filed under the module they were bought in: {100 * p['same'].mean():.2f}%")

d = s.merge(p[['upc', 'upc_ver_uc', 'upc_descr', 'brand_descr', 'multi', 'size1_amount', 'size1_units']], on='upc', how='left')
d = d.merge(names, on='product_module_code', how='left')
d = d.merge(pd.read_parquet(idir / 'products_extra' / 'products_extra_upc.parquet', columns=['upc', 'upc_ver_uc'] + FIELDS),
            on=['upc', 'upc_ver_uc'], how='left')
sc = pd.read_parquet(idir / 'rms_variety' / 'prep_time_scores.parquet', columns=['product_module_code', 'prep_time', 'rationale'])
d = d.merge(sc.rename(columns={'prep_time': 'old_score', 'rationale': 'old_why'}), on='product_module_code', how='left')
d = d.rename(columns={'product_module_descr': 'module', 'product_group_descr': 'group', 'upc_descr': 'descr', 'brand_descr': 'brand'})
d = d[d['module'].notna()].copy()                    # 76 unnamed module codes of 2010 only, 0.002% of spending
d['multi'] = pd.to_numeric(d['multi'], errors='coerce').fillna(1)
d['size1_amount'] = pd.to_numeric(d['size1_amount'], errors='coerce')
d['descr'] = d['descr'].str.strip().str.upper()
OUT.parent.mkdir(parents=True, exist_ok=True)
d.to_parquet(OUT, index=False)
print(f'{len(d):,} barcodes, {d.product_module_code.nunique()} modules, spending ${d.spending.sum() / 1e6:,.0f} m')
print('share of spending with each attribute stated:', {f: round(float(d.loc[d[f].notna(), 'spending'].sum() / d.spending.sum()), 3) for f in FIELDS})
print(f'share of spending with a description: {d.loc[d.descr.notna(), "spending"].sum() / d.spending.sum():.3f}')
print(f'saved {OUT}')
