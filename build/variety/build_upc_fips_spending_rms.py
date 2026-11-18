"""Barcode x county spending and units of EVERY RMS store, one file per year -- run on OSCAR.

Replaces the per-year step of build_product_variety.py, which mapped stores to counties from the 2006 stores file only and so
left out stores that entered later: its files held 100% of RMS food spending through 2011, 94% in 2015, 90% in 2017 and 67-69%
in 2018-2020 (found 2026-11-15).

Reads   /users/amarche4/scratch/new_products_places/store_upc/<year>/m<module>.parquet  (store, upc, units, spend; every store,
        analyze/what_moves/build_store_upc_year_rms.py, which keeps store x barcode rows with positive units and spending)
        /users/amarche4/data/rms_variety/store_year_fips.parquet  (each store-year's county, build_store_year_fips.py)
Writes  /users/amarche4/data/rms_variety/rms_upc_fips_spending_<year>.parquet with the columns and types of the old file:
        upc (str, 12 digits), product_group_code, product_module_code (int64), fips (str, 5 digits), total_spending,
        total_units (float64).
The old file is moved to /users/amarche4/data/rms_variety/z_backup_2026-11-18/ before the new one is written (once; a rerun
does not touch the backup).

Checks printed per year: spending in the store files, share without a county, new total against the old file's total, modules
of the old file that the store files lack (the run stops if they hold more than 0.01% of the old file's spending: run
build_store_upc_year_rms.py for the year first), and, row by row, how far the new file is from the old one on the rows both
have (the years through 2011 should agree, apart from the 643 stores whose county code changes over the years).

Usage: python build_upc_fips_spending_rms.py <year> [check]     (check: print the comparison, write nothing)
"""
import gc, sys, time
from pathlib import Path
import numpy as np, pandas as pd
import pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq

D = Path('/users/amarche4/data/rms_variety')
S = Path('/users/amarche4/scratch/new_products_places/store_upc')
BACKUP = D / 'z_backup_2026-11-18'
MAX_MISSING_SHARE = 1e-4


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


def main(year, check=False):
    t0 = time.time()
    out_path = D / f'rms_upc_fips_spending_{year}.parquet'
    old_path = BACKUP / out_path.name if (BACKUP / out_path.name).exists() else out_path

    sf = pd.read_parquet(D / 'store_year_fips.parquet')
    sf = sf[sf['year'] == year]
    fips_of = pd.Series(sf['fips'].astype(np.int32).values, index=sf['store'].astype(np.int64).values)

    # the old file: module -> group, its totals, and its barcode format
    old = pq.read_table(old_path, columns=['upc', 'product_group_code', 'product_module_code', 'fips', 'total_spending', 'total_units']).to_pandas()
    lens = old['upc'].str.len()
    assert lens.min() == 12 and lens.max() == 12, f'old barcodes are not all 12 characters ({lens.min()}-{lens.max()})'
    old['upc'] = old['upc'].astype(np.int64)
    old['fips'] = old['fips'].astype(np.int32)
    group_of = {}
    for f in sorted(BACKUP.glob('rms_upc_fips_spending_*.parquet')) + sorted(D.glob('rms_upc_fips_spending_*.parquet')):
        g = pq.read_table(f, columns=['product_module_code', 'product_group_code']).to_pandas().drop_duplicates()
        group_of.update(dict(zip(g['product_module_code'].astype(int), g['product_group_code'].astype(int))))
    old_mod = old.groupby('product_module_code')['total_spending'].sum()

    files = sorted((S / str(year)).glob('m*.parquet'))
    have = {int(f.stem[1:]) for f in files}
    lack = old_mod[~old_mod.index.isin(have)]
    log(f'{year}: {len(files)} module files; old file {len(old):,} rows, {len(old_mod)} modules, ${old_mod.sum() / 1e9:.2f} bn; '
        f'old modules without a store file: {len(lack)} with {100 * lack.sum() / old_mod.sum():.4f}% of old spending {sorted(lack.index)[:20]}')
    if lack.sum() > MAX_MISSING_SHARE * old_mod.sum():
        raise SystemExit(f'{year}: run build_store_upc_year_rms.py {year} first, the store files lack modules of the old file')

    parts, raw, nofips, nogroup = [], 0.0, 0.0, []
    for i, f in enumerate(files):
        mod = int(f.stem[1:])
        if mod not in group_of:
            nogroup.append(mod)
            continue
        d = pd.read_parquet(f)
        spend = d['spend'].astype(np.float64)
        raw += spend.sum()
        d['fips'] = d['store'].astype(np.int64).map(fips_of)
        nofips += spend[d['fips'].isna()].sum()
        d = d.dropna(subset=['fips'])
        if d.empty:
            continue
        g = (pd.DataFrame({'upc': d['upc'].values, 'fips': d['fips'].astype(np.int32).values,
                           'total_spending': d['spend'].astype(np.float64).values, 'total_units': d['units'].astype(np.float64).values})
             .groupby(['upc', 'fips'], as_index=False, sort=False).sum())
        g['product_module_code'] = np.int64(mod)
        g['product_group_code'] = np.int64(group_of[mod])
        parts.append(g)
        del d, g
        if (i + 1) % 100 == 0:
            gc.collect()
            log(f'{year}: {i + 1} modules read ({time.time() - t0:.0f}s)')
    new = pd.concat(parts, ignore_index=True)
    del parts
    gc.collect()
    # a barcode sits in one module file a year; if not, the old file's grouping (upc, group, module, county) keeps them apart too
    log(f'{year}: store files ${raw / 1e9:.2f} bn; without a county {100 * nofips / raw:.4f}%; modules without a group code {nogroup}; '
        f'new file {len(new):,} rows, ${new.total_spending.sum() / 1e9:.2f} bn = {100 * new.total_spending.sum() / old_mod.sum():.1f}% of the old file; '
        f'old file = {100 * old_mod.sum() / raw:.1f}% of all stores')

    # row-by-row comparison with the old file
    key = ['upc', 'product_module_code', 'fips']
    m = old[key + ['total_spending']].merge(new[key + ['total_spending']], on=key, how='outer', suffixes=('_old', '_new'), indicator=True)
    both = m[m['_merge'] == 'both']
    rel = (both['total_spending_new'] / both['total_spending_old'] - 1).abs()
    log(f'{year}: rows in both {len(both):,}, only old {(m._merge == "left_only").sum():,} '
        f'(${m.loc[m._merge == "left_only", "total_spending_old"].sum() / 1e9:.3f} bn), only new {(m._merge == "right_only").sum():,} '
        f'(${m.loc[m._merge == "right_only", "total_spending_new"].sum() / 1e9:.3f} bn); on rows in both, new/old spending '
        f'{both.total_spending_new.sum() / both.total_spending_old.sum():.4f}, rows within 0.1% {100 * (rel < 1e-3).mean():.1f}%, '
        f'new at or above old {100 * (both.total_spending_new >= both.total_spending_old * (1 - 1e-6)).mean():.1f}%')
    del m, both, old
    gc.collect()
    if check:
        log(f'{year}: check only, nothing written ({time.time() - t0:.0f}s)')
        return

    def padded(values, width):
        # text in pieces of 5 million rows: one array of 60 million 12-character barcodes passes the 2 GB limit of a text column
        parts = np.array_split(values, max(1, len(values) // 5_000_000 + 1))
        return pa.chunked_array([pc.utf8_lpad(pc.cast(pa.array(v), pa.string()), width, '0') for v in parts])

    t = pa.table({'upc': padded(new['upc'].values, 12),
                  'product_group_code': pa.array(new['product_group_code'].values),
                  'product_module_code': pa.array(new['product_module_code'].values),
                  'fips': padded(new['fips'].values, 5),
                  'total_spending': pa.array(new['total_spending'].values),
                  'total_units': pa.array(new['total_units'].values)})
    BACKUP.mkdir(exist_ok=True)
    if not (BACKUP / out_path.name).exists():
        out_path.rename(BACKUP / out_path.name)
    tmp = D / f'rms_upc_fips_spending_{year}.tmp'
    pq.write_table(t, tmp)
    tmp.rename(out_path)
    log(f'{year}: saved {out_path.name}, {t.num_rows:,} rows ({time.time() - t0:.0f}s)')


if __name__ == '__main__':
    main(int(sys.argv[1]), len(sys.argv) > 2 and sys.argv[2] == 'check')
