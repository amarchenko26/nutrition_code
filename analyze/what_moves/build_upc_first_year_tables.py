"""Collapses interim/rms_variety/upc_first_year_county.parquet (177M upc x module x county rows, copied from Oscar) into
a small table, one row group at a time so memory stays low (2026-11-05):
  interim/what_moves/upc_first_year_national.parquet   upc (12-digit string), product_module_code, first_year_national
The county counts it also wrote (new_upc_county_counts.parquet) fed only the county demand tables, deleted with their
code on 2026-11-10.
2007 is the file's first year, so first_year = 2007 means "already on the shelf"; new products start in 2008.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import gc
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

F = idir / 'rms_variety' / 'upc_first_year_county.parquet'
OUT = idir / 'what_moves'


def to_int(col, typ):
    """dictionary-encoded digit strings -> integers, without decoding the strings row by row"""
    return pa.chunked_array([pc.take(pc.cast(ch.dictionary, typ), ch.indices) for ch in col.chunks])


def main():
    pf = pq.ParquetFile(F, read_dictionary=['upc'])
    nat = []
    for i in range(pf.num_row_groups):
        t = pf.read_row_group(i, columns=['upc', 'product_module_code', 'first_year_national'])
        u = to_int(t['upc'], pa.int64())
        x = pa.table({'upc': u, 'm': pc.cast(t['product_module_code'], pa.int32()), 'y': pc.cast(t['first_year_national'], pa.int16())})
        del t, u; gc.collect()
        nat.append(x.group_by(['upc', 'm', 'y']).aggregate([]).to_pandas())
        del x; gc.collect()
        print(f'row group {i}: national rows {len(nat[-1]):,}', flush=True)
    n = pd.concat(nat, ignore_index=True).groupby(['upc', 'm'], as_index=False)['y'].min()
    n['upc'] = n['upc'].astype('int64').astype(str).str.zfill(12)
    n = n.rename(columns={'m': 'product_module_code', 'y': 'first_year_national'})
    n.to_parquet(OUT / 'upc_first_year_national.parquet', index=False)
    print(f'national: {len(n):,} upc-module pairs;', n['first_year_national'].value_counts().sort_index().to_dict())


if __name__ == '__main__':
    main()
