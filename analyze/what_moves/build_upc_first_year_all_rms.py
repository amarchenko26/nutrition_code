"""First year each food barcode sold at ANY RMS store -- run on OSCAR after build_store_upc_year_rms.py for 2008-2020.
The variety files (rms_upc_fips_spending_<y>.parquet) keep only stores in store_fips_cache.parquet: every RMS store to 2011 but
about 58% of stores (68% of food spending) in 2018-2020, so their first years miss barcodes first sold at the other stores.
first_year = min(first year in upc_first_year.parquet (variety files, 2006-2020), first year in store_upc/<y>/ (all stores,
2008-2020)). Writes /users/amarche4/scratch/new_products_places/upc_first_year_all.parquet (upc, first_year)."""
import time
from pathlib import Path
import numpy as np, pandas as pd, pyarrow.parquet as pq

O = Path('/users/amarche4/scratch/new_products_places')
t0 = time.time()
first = {}
for y in range(2008, 2021):
    u = np.unique(np.concatenate([pq.read_table(f, columns=['upc'])['upc'].to_numpy() for f in sorted((O / 'store_upc' / str(y)).glob('m*.parquet'))]))
    s = pd.Series(y, index=u, dtype=np.int16)
    first[y] = s
    print(y, f'{len(u):,} barcodes ({time.time() - t0:.0f}s)', flush=True)
files = pd.concat(first.values())
files = files.groupby(level=0).min().rename('fy_files')
old = pd.read_parquet(O / 'upc_first_year.parquet').set_index('upc')['first_year'].rename('fy_old')
d = pd.concat([old, files], axis=1)
d['first_year'] = d[['fy_old', 'fy_files']].min(axis=1).astype(np.int16)
ch = d[d['fy_old'].notna() & (d['first_year'] != d['fy_old'])]
print(f'barcodes: {len(d):,}; not in the variety files {d.fy_old.isna().sum():,}; first year moved earlier {len(ch):,}')
print('first year, new map:', d['first_year'].value_counts().sort_index().to_dict())
print('first year, variety files:', d['fy_old'].value_counts().sort_index().to_dict())
d['first_year'].rename_axis('upc').reset_index().to_parquet(O / 'upc_first_year_all.parquet', index=False)
