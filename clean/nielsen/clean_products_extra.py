"""Nielsen's product attributes, one row per barcode, 2004-2020.

Reads  raw/consumer/Consumer_Panel_Data_<y>.tgz, member nielsen_extracts/HMS/<y>/Annual_Files/products_extra_<y>.tsv
       (streamed from the archive; nothing is extracted to disk)
Writes interim/products_extra/products_extra_upc.parquet: upc (12 characters, as in raw/products.tsv), n_years (years the
       barcode is in the files), and the attribute descriptions type, style, form, use, product, variety, formula, flavor,
       container, common_consumer_name (text; missing if never stated).

A barcode can be in several yearly files with different values. Each attribute takes its value from the most recent year in
which it is stated ('NOT STATED' and blank count as not stated). The files cover every department; no barcode is dropped here.

For the barcode-level preparation score (build/prep_time), where instant and steel-cut oats, or brown and white rice, must get
different cooking times.
"""
import io
import sys
import tarfile
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import pandas as pd

FIELDS = ['type', 'style', 'form', 'use', 'product', 'variety', 'formula', 'flavor', 'container', 'common_consumer_name']
OUT = idir / 'products_extra' / 'products_extra_upc.parquet'

frames = []
for y in range(2004, 2021):
    target = f'nielsen_extracts/HMS/{y}/Annual_Files/products_extra_{y}.tsv'
    d = None
    with tarfile.open(rdir / 'consumer' / f'Consumer_Panel_Data_{y}.tgz', 'r|gz') as tf:
        for member in tf:
            if member.name == target:
                d = pd.read_csv(io.BytesIO(tf.extractfile(member).read()), sep='\t', quoting=3, encoding='latin-1', dtype=str,
                                usecols=['upc', 'upc_ver_uc'] + [f'{f}_descr' for f in FIELDS])
                break
    assert d is not None, f'{target} not found'
    d['year'] = y
    frames.append(d)
    print(y, f'{len(d):,} rows', flush=True)

d = pd.concat(frames, ignore_index=True)
d['upc'] = d['upc'].str.zfill(12)
d['upc_ver_uc'] = d['upc_ver_uc'].astype(int)
for f in FIELDS:
    v = d[f'{f}_descr'].str.strip().str.upper()
    d[f] = v.where(v.notna() & (v != '') & (v != 'NOT STATED'))
d = d.sort_values(['upc', 'year', 'upc_ver_uc'])
g = d.groupby('upc', sort=False)
out = g[FIELDS].last()                       # last() skips missing values: the most recent year that states the attribute
out['n_years'] = g['year'].nunique().astype('int16')
out = out.reset_index()[['upc', 'n_years'] + FIELDS]

# how often a barcode's stated value differs between years
for f in ['type', 'style', 'form', 'product']:
    n = d.dropna(subset=[f]).groupby('upc')[f].nunique()
    print(f'{f}: stated for {len(n):,} barcodes, more than one value over the years for {100 * (n > 1).mean():.1f}%')
OUT.parent.mkdir(parents=True, exist_ok=True)
out.to_parquet(OUT, index=False)
print(f'{len(out):,} barcodes; share with each attribute stated:', {f: round(float(out[f].notna().mean()), 3) for f in FIELDS})
print(f'saved {OUT}')
