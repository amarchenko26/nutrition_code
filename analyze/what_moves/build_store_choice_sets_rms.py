"""What each RMS store offers: healthfulness and prices of its choice set, one year per call -- run on OSCAR.

For Anya's 2026-11-10 request: access to healthy food by neighborhood income, following Allcott, Diamond and Dube (2019,
Figure II), whose store file had 5-digit ZIP codes; ours has 3-digit ZIP and county only, so the stores' neighborhood
income comes from their shoppers' home ZIP codes (build_store_shopper_income.py, local).
Streams /users/amarche4/data/rms/2006-2020_Scanner_Data/SCANNER_DATA_<year>.tgz one food module at a time
(Movement_Files/<group>_<year>/<module>_<year>.tsv: store_code_uc, upc, week_end, units, prmult, price), aggregates to
store x UPC (units; spending = units x price / prmult) and keeps the UPCs of rms_upc_groups.parquet (build_rms_upc_groups.py)
with units and spending above zero. A store's choice set = the food UPCs it sold that year.
Every measure weights a UPC by the calories in ONE package (kcal_unit, packages above 1 kcal), not by units sold, so the
measures describe access, not consumption:
  addsugar_1000   grams of added sugar per 1,000 kcal over the packages offered (UPCs with an added-sugar value)
  produce_share   share of package calories in fruit and vegetable UPCs
  whole_share     share of bread, bun and roll package calories that are whole grain
  hi_1000         mean Health Index per 1,000 kcal (GetHealthIndex.do formula)
  nova4_share     share of package calories that are ultra-processed (NOVA 4), over UPCs with a NOVA group
  claude_hi_mean  mean module healthiness score (0-10)
  price_healthy, price_unhealthy: mean of log(store unit value / national unit value) over the store's healthy (claude_hi
      4-10) or unhealthy UPCs, clipped at the UPC's 1st and 99th percentiles across stores, weighted by package calories;
      price_*_sold: the same weighted by national calories sold (the weights of their price figure)
  n_upcs, n_upcs_healthy, n_upcs_unhealthy, food_spend (all food UPCs kept)
Store attributes from the year's stores file: channel_code, store_zip3, fips (state x 1000 + county), retailer_code, parent_code.
Output: /users/amarche4/scratch/store_choice/store_choice_sets_<year>.parquet.
Usage: python build_store_choice_sets_rms.py <year> [n_modules]   (n_modules: stop after that many food modules, a test);
python build_store_choice_sets_rms.py combine writes store_choice_sets.parquet (all years), copied to interim/what_moves/.
"""
import re
import sys
import tarfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

RMS = Path('/users/amarche4/data/rms/2006-2020_Scanner_Data')
UPC = Path('/users/amarche4/data/rms_variety/rms_upc_groups.parquet')
OUT = Path('/users/amarche4/scratch/store_choice')
MIN_KCAL = 1.0
SUMS = ['n', 'n_h', 'n_u', 'spend', 'kc', 'add', 'kc_add', 'prod', 'hi', 'kc_hi', 'n4', 'kc_nova', 'bread', 'whole', 'chi',
        'ph', 'wh', 'pu', 'wu', 'phs', 'whs', 'pus', 'wus']


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


def clip_by_group(key, x, lo=0.01, hi=0.99):
    order = np.lexsort((x, key))
    ks, xs = key[order], x[order]
    starts = np.flatnonzero(np.r_[True, ks[1:] != ks[:-1]])
    n = np.r_[starts[1:], len(ks)] - starts
    lo_v = xs[starts + np.floor(lo * (n - 1)).astype(np.int64)]
    hi_v = xs[starts + np.ceil(hi * (n - 1)).astype(np.int64)]
    grp = np.repeat(np.arange(len(starts)), n)
    out = np.empty_like(x)
    out[order] = np.clip(xs, lo_v[grp], hi_v[grp])
    return out


def agg(t):
    """Sum units and spending by store x UPC; columns renamed by name (pyarrow puts the sums before the keys)."""
    g = t.group_by(['store', 'upc']).aggregate([('units', 'sum'), ('spend', 'sum')])
    g = g.rename_columns([{'units_sum': 'units', 'spend_sum': 'spend'}.get(c, c) for c in g.column_names])
    return g.select(['store', 'upc', 'units', 'spend'])


def read_movement(f):
    """store x UPC units and spending from one module's movement file (a tar member stream)."""
    conv = pacsv.ConvertOptions(include_columns=['store_code_uc', 'upc', 'units', 'prmult', 'price'],
                                column_types={'store_code_uc': pa.int64(), 'upc': pa.int64(), 'units': pa.float64(),
                                              'prmult': pa.float64(), 'price': pa.float64()})
    reader = pacsv.open_csv(f, read_options=pacsv.ReadOptions(block_size=64 << 20), parse_options=pacsv.ParseOptions(delimiter='\t'),
                            convert_options=conv)
    parts = []
    for b in reader:
        spend = pc.multiply(b.column('units'), pc.divide(b.column('price'), pc.max_element_wise(b.column('prmult'), pa.scalar(1.0))))
        parts.append(agg(pa.table({'store': b.column('store_code_uc'), 'upc': b.column('upc'), 'units': b.column('units'), 'spend': spend})))
        if len(parts) >= 20:                                       # files sorted by week would otherwise pile up
            parts = [agg(pa.concat_tables(parts))]
    if not parts:
        return None
    return agg(pa.concat_tables(parts)).to_pandas()


def module_sums(d, attr):
    """Per-store sums for one module's store x UPC rows."""
    d = d[(d['units'] > 0) & (d['spend'] > 0)].merge(attr, on='upc', how='inner')
    if d.empty:
        return None
    nat = d.groupby('upc').agg(s=('spend', 'sum'), u=('units', 'sum'))
    d['rel'] = np.log(d['spend'] / d['units']) - np.log((nat['s'] / nat['u']).reindex(d['upc']).values)
    d['rel'] = clip_by_group(d['upc'].values, d['rel'].values)
    d['wsold'] = nat['u'].reindex(d['upc']).values * d['kcal_unit'].values
    k = d['kcal_unit'].where(d['kcal_unit'] > MIN_KCAL)            # package calories, the choice-set weight
    h = d['healthy'] == 1
    x = pd.DataFrame({'store': d['store'], 'n': 1, 'n_h': h.astype(int), 'n_u': (~h).astype(int), 'spend': d['spend'],
                      'kc': k, 'add': d['addsugar_unit'].where(k.notna()), 'kc_add': k.where(d['addsugar_unit'].notna()),
                      'prod': k * d['produce'], 'hi': k * d['hi_1000'], 'kc_hi': k.where(d['hi_1000'].notna() & np.isfinite(d['hi_1000'])),
                      'n4': k * d['nova4'], 'kc_nova': k.where(d['nova4'].notna()), 'bread': k * d['bread'], 'whole': k * d['bread'] * d['whole'],
                      'chi': k * d['claude_hi'],
                      'ph': (k * d['rel']).where(h), 'wh': k.where(h), 'pu': (k * d['rel']).where(~h), 'wu': k.where(~h),
                      'phs': (d['wsold'] * d['rel']).where(h), 'whs': d['wsold'].where(h), 'pus': (d['wsold'] * d['rel']).where(~h),
                      'wus': d['wsold'].where(~h)})
    x['hi'] = x['hi'].where(x['kc_hi'].notna())
    return x.groupby('store')[SUMS].sum(min_count=1)


def main(year, n_modules=None):
    t0 = time.time()
    attr = pd.read_parquet(UPC, columns=['upc', 'product_module_code', 'claude_hi', 'healthy', 'kcal_unit', 'addsugar_unit', 'hi_1000',
                                         'produce', 'nova4', 'bread', 'whole'])
    food_modules = set(attr['product_module_code'].unique())
    attr = attr.drop(columns='product_module_code')
    acc, stores, done = [], None, 0
    with tarfile.open(RMS / f'SCANNER_DATA_{year}.tgz', 'r|gz') as tf:
        for m in tf:
            name = m.name
            if re.search(r'stores_\d{4}\.tsv$', name):
                stores = pd.read_csv(tf.extractfile(m), sep='\t', dtype=str)
                log(f'stores file {name}: {len(stores):,} rows')
            hit = re.search(r'Movement_Files/\d+_\d+/(\d+)_\d+\.tsv$', name)
            if hit and int(hit.group(1)) in food_modules and (n_modules is None or done < n_modules):
                d = read_movement(tf.extractfile(m))
                if d is not None:
                    s = module_sums(d, attr)
                    if s is not None:
                        acc.append(s)
                done += 1
                if done % 25 == 0:
                    acc = [pd.concat(acc).groupby(level=0).sum(min_count=1)]
                    log(f'{done} food modules, {len(acc[0]):,} stores ({time.time() - t0:.0f}s)')
            tf.members = []
            if n_modules is not None and done >= n_modules:
                break
    s = pd.concat(acc).groupby(level=0).sum(min_count=1)
    out = pd.DataFrame({'n_upcs': s['n'], 'n_upcs_healthy': s['n_h'], 'n_upcs_unhealthy': s['n_u'], 'food_spend': s['spend'],
                        'kcal_offered': s['kc'], 'addsugar_1000': 1000 * s['add'] / s['kc_add'], 'produce_share': s['prod'] / s['kc'],
                        'whole_share': s['whole'] / s['bread'], 'hi_1000': s['hi'] / s['kc_hi'], 'nova4_share': s['n4'] / s['kc_nova'],
                        'claude_hi_mean': s['chi'] / s['kc'], 'price_healthy': s['ph'] / s['wh'], 'price_unhealthy': s['pu'] / s['wu'],
                        'price_healthy_sold': s['phs'] / s['whs'], 'price_unhealthy_sold': s['pus'] / s['wus']})
    out.index.name = 'store_code_uc'
    out = out.reset_index()
    out['year'] = year
    if stores is not None:
        st = stores.rename(columns=str.lower)
        st['store_code_uc'] = pd.to_numeric(st['store_code_uc'], errors='coerce')
        st['fips'] = 1000 * pd.to_numeric(st['fips_state_code'], errors='coerce') + pd.to_numeric(st['fips_county_code'], errors='coerce')
        keep = [c for c in ['store_code_uc', 'channel_code', 'store_zip3', 'fips', 'retailer_code', 'parent_code'] if c in st.columns]
        out = out.merge(st[keep].drop_duplicates('store_code_uc'), on='store_code_uc', how='left')
    OUT.mkdir(parents=True, exist_ok=True)
    tag = '' if n_modules is None else f'_test{n_modules}'
    out.to_parquet(OUT / f'store_choice_sets_{year}{tag}.parquet', index=False)
    log(f'{year}: {len(out):,} stores, {done} food modules, saved ({time.time() - t0:.0f}s)')
    print(out.describe().T[['count', 'mean', '50%']].round(3).to_string())


def combine():
    parts = sorted(OUT.glob('store_choice_sets_[0-9][0-9][0-9][0-9].parquet'))
    pd.concat([pd.read_parquet(f) for f in parts], ignore_index=True).to_parquet(OUT / 'store_choice_sets.parquet', index=False)
    print(f'{len(parts)} years combined')


if __name__ == '__main__':
    if sys.argv[1] == 'combine':
        combine()
    else:
        main(int(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) > 2 else None)
