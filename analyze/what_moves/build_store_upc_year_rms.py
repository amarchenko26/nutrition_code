"""Store x barcode units and spending for one year, one parquet file per food module -- run on OSCAR (scratch work for Anya's
2026-11-15 request: new food options offered in stores by shopper income; would be a mode of
analyze/what_moves/build_store_choice_sets_rms.py, which streams the same files).
Streams /users/amarche4/data/rms/2006-2020_Scanner_Data/SCANNER_DATA_<year>.tgz (read only), food modules of module_cells.parquet
(the 538 modules of Table 12's data) and, since 2026-11-18, every other food module of the barcode x county files
(rms_variety_module_fips_year.parquet, 554 modules), because build/variety/build_upc_fips_spending_rms.py rebuilds those files
from this output. A module whose file exists is skipped, so a rerun streams only the modules that are missing. Aggregates the weekly rows to store x UPC (units; spending = units x price / prmult, as
build_store_choice_sets_rms.py) and writes /users/amarche4/scratch/new_products_places/store_upc/<year>/m<module>.parquet
(store int32, upc int64, units float32, spend float32).
Usage: python build_store_upc_year_rms.py <year> [n_modules]   (n_modules: stop after that many food modules, a test)"""
import gc, re, sys, tarfile, time
from pathlib import Path
import numpy as np, pandas as pd
import pyarrow as pa, pyarrow.compute as pc, pyarrow.csv as pacsv, pyarrow.parquet as pq

RMS = Path('/users/amarche4/data/rms/2006-2020_Scanner_Data')
O = Path('/users/amarche4/scratch/new_products_places')
VARIETY = Path('/users/amarche4/data/rms_variety/rms_variety_module_fips_year.parquet')


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


def agg(t):
    g = t.group_by(['store', 'upc']).aggregate([('units', 'sum'), ('spend', 'sum')])
    g = g.rename_columns([{'units_sum': 'units', 'spend_sum': 'spend'}.get(c, c) for c in g.column_names])
    return g.select(['store', 'upc', 'units', 'spend'])


def read_movement(f):
    conv = pacsv.ConvertOptions(include_columns=['store_code_uc', 'upc', 'units', 'prmult', 'price'],
                                column_types={'store_code_uc': pa.int64(), 'upc': pa.int64(), 'units': pa.float64(),
                                              'prmult': pa.float64(), 'price': pa.float64()})
    reader = pacsv.open_csv(f, read_options=pacsv.ReadOptions(block_size=64 << 20), parse_options=pacsv.ParseOptions(delimiter='\t'),
                            convert_options=conv)
    parts, raw_spend = [], 0.0
    for b in reader:
        spend = pc.multiply(b.column('units'), pc.divide(b.column('price'), pc.max_element_wise(b.column('prmult'), pa.scalar(1.0))))
        raw_spend += pc.sum(spend).as_py() or 0.0
        parts.append(agg(pa.table({'store': b.column('store_code_uc'), 'upc': b.column('upc'), 'units': b.column('units'), 'spend': spend})))
        if len(parts) >= 20:
            parts = [agg(pa.concat_tables(parts))]
    if not parts:
        return None, 0.0
    return agg(pa.concat_tables(parts)).to_pandas(), raw_spend


def main(year, n_modules=None):
    t0 = time.time()
    mods = set(pd.read_parquet(O / 'module_cells.parquet')['product_module_code'].astype(int))
    mods |= set(pd.read_parquet(VARIETY, columns=['product_module_code'])['product_module_code'].astype(int))
    out = O / 'store_upc' / str(year)
    out.mkdir(parents=True, exist_ok=True)
    have = {int(f.stem[1:]) for f in out.glob('m*.parquet')}
    log(f'{year}: {len(mods)} food modules, {len(have & mods)} already written')
    mods -= have
    if not mods:
        log(f'{year}: nothing to do')
        return
    done, rows, bad = 0, 0, 0
    with tarfile.open(RMS / f'SCANNER_DATA_{year}.tgz', 'r|gz') as tf:
        for m in tf:
            hit = re.search(r'Movement_Files/\d+_\d+/(\d+)_\d+\.tsv$', m.name)
            if hit and int(hit.group(1)) in mods and (n_modules is None or done < n_modules):
                mod = int(hit.group(1))
                d, raw = read_movement(tf.extractfile(m))
                if d is not None:
                    if abs(d['spend'].sum() - raw) > 1e-6 * max(raw, 1.0):
                        bad += 1
                        log(f'module {mod}: spending {d["spend"].sum():,.0f} after the collapse vs {raw:,.0f} read')
                    d = d[(d['units'] > 0) & (d['spend'] > 0)]
                    t = pa.table({'store': pa.array(d['store'].values.astype(np.int32)), 'upc': pa.array(d['upc'].values.astype(np.int64)),
                                  'units': pa.array(d['units'].values.astype(np.float32)), 'spend': pa.array(d['spend'].values.astype(np.float32))})
                    pq.write_table(t, out / f'm{mod}.tmp')              # renamed when complete, so a killed job leaves no partial file
                    (out / f'm{mod}.tmp').rename(out / f'm{mod}.parquet')
                    rows += len(d)
                del d
                gc.collect()
                done += 1
                if done % 25 == 0:
                    log(f'{year}: {done} modules, {rows:,} store x UPC rows ({time.time() - t0:.0f}s)')
            tf.members = []
            if n_modules is not None and done >= n_modules:
                break
    log(f'{year}: done, {done} modules, {rows:,} store x UPC rows, {bad} modules with a spending mismatch ({time.time() - t0:.0f}s)')


if __name__ == '__main__':
    main(int(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) > 2 else None)
