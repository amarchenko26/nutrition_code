"""Classifies every new food UPC of 2008-2020 by what kind of new product it is (2026-11-05, Anya: "new products in more
sizes and more flavors").

Inputs: interim/what_moves/upc_first_year_national.parquet (first year a UPC sold anywhere in RMS; 2007 = already on the
shelf) and raw/products.tsv (brand, description, size; first version of each UPC). For a UPC first sold in year t in module m,
brand b, against the UPCs of (m, b) first sold before t:
  store brand   brand_descr starts with "CTL BR": all store brands share this code, so they cannot be classified further
  new brand     b has no earlier UPC in m
  new variant   b has earlier UPCs in m, but none with this set of description words (flavor, form, line); size_new flags
                whether the size is also new to (m, b)
  new size      same description words as an earlier UPC of (m, b), size (amount x unit x multipack) not seen before for (m, b)
  re-coded      same description words and same size as an earlier UPC of (m, b): a new code for an old product
Description words: upc_descr split on spaces, with the brand's own abbreviation words kept (they do not change within a
brand) and size-like tokens dropped. Nielsen's abbreviations are not uniform, so "new variant" includes some coding noise.
Outputs: interim/what_moves/new_upc_types.parquet (UPC-level), new_product_types_by_group.parquet (group x year x type counts)
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import gc, re
import numpy as np
import pandas as pd

TOKEN_DROP = re.compile(r'^\d+([.]\d+)?(OZ|CT|LB|PK|FL|ML|L|GAL|QT|PT|G)?$')


def wordset(s):
    if not isinstance(s, str):
        return ''
    return ' '.join(sorted({w for w in s.upper().split() if not TOKEN_DROP.match(w)}))


def main():
    fy = pd.read_parquet(idir / 'what_moves' / 'upc_first_year_national.parquet')
    groups = pd.read_parquet(idir / 'what_moves' / 'module_groups.parquet', columns=['product_module_code', 'hi_group', 'upf_group', 'prep_group'])
    fy = fy[fy['product_module_code'].isin(groups['product_module_code'])]
    print(f'{len(fy):,} food upc-module pairs; duplicate upcs across modules: {fy.upc.duplicated().sum():,}', flush=True)
    cols = ['upc', 'upc_ver_uc', 'upc_descr', 'product_module_code', 'brand_descr', 'multi', 'size1_amount', 'size1_units']
    keep = []
    for ch in pd.read_csv(rdir / 'products.tsv', sep='\t', usecols=cols, dtype=str, encoding='latin-1', chunksize=1_000_000):
        ch['product_module_code'] = pd.to_numeric(ch['product_module_code'], errors='coerce')
        ch = ch[ch['product_module_code'].isin(groups['product_module_code'])]
        keep.append(ch)
    p = pd.concat(keep, ignore_index=True)
    del keep; gc.collect()
    p['upc'] = p['upc'].str.zfill(12)
    p['upc_ver_uc'] = pd.to_numeric(p['upc_ver_uc'], errors='coerce')
    p = p.sort_values(['upc', 'product_module_code', 'upc_ver_uc']).drop_duplicates(['upc', 'product_module_code'])
    print(f'products.tsv food rows (first version): {len(p):,}', flush=True)
    d = fy.merge(p.drop(columns='upc_ver_uc'), on=['upc', 'product_module_code'], how='left')
    print(f'matched to products: {d.brand_descr.notna().mean():.4f}', flush=True)
    del p; gc.collect()
    d['words'] = d['upc_descr'].map(wordset)
    d['size'] = d['size1_amount'].fillna('') + '|' + d['size1_units'].fillna('') + '|' + d['multi'].fillna('1')
    d['store'] = d['brand_descr'].fillna('').str.startswith('CTL BR')
    d = d.sort_values(['product_module_code', 'brand_descr', 'first_year_national']).reset_index(drop=True)
    # first year each (module, brand), (module, brand, words), (module, brand, size), (module, brand, words, size) was seen
    k = ['product_module_code', 'brand_descr']
    d['fy_brand'] = d.groupby(k, dropna=False)['first_year_national'].transform('min')
    d['fy_words'] = d.groupby(k + ['words'], dropna=False)['first_year_national'].transform('min')
    d['fy_size'] = d.groupby(k + ['size'], dropna=False)['first_year_national'].transform('min')
    t = d['first_year_national']
    d['type'] = np.select([d['store'], d['fy_brand'] >= t, d['fy_words'] >= t, d['fy_size'] >= t],
                          ['store brand', 'new brand', 'new variant', 'new size'], default='re-coded')
    d['size_new'] = d['fy_size'] >= t
    new = d[t >= 2008].copy()
    print(new['type'].value_counts(normalize=True).round(3).to_dict())
    print('variant with a new size too:', round(new.loc[new.type == 'new variant', 'size_new'].mean(), 3))
    new = new.merge(groups, on='product_module_code', how='left')
    new[['upc', 'product_module_code', 'first_year_national', 'type', 'size_new', 'hi_group', 'upf_group', 'prep_group']].to_parquet(idir / 'what_moves' / 'new_upc_types.parquet', index=False)
    rows = []
    for g in ['hi_group', 'upf_group', 'prep_group']:
        x = new.dropna(subset=[g]).groupby([g, 'first_year_national', 'type']).size().rename('n').reset_index().rename(columns={g: 'group'})
        x['grouping'] = g
        rows.append(x)
    out = pd.concat(rows, ignore_index=True)
    out.to_parquet(idir / 'what_moves' / 'new_product_types_by_group.parquet', index=False)
    print(out.groupby(['grouping', 'group', 'type'])['n'].sum().unstack().pipe(lambda z: z.div(z.sum(axis=1), axis=0)).round(3).to_string())
    print(out.groupby(['grouping', 'group'])['n'].sum().to_string())
    print('examples of each type (2015):')
    for ty in ['new variant', 'new size', 'new brand', 're-coded']:
        print(ty, new[(new.type == ty) & (new.first_year_national == 2015)]['upc_descr'].head(6).tolist())


if __name__ == '__main__':
    main()
