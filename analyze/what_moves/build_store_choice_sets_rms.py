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
  Prices (Anya 2026-11-10), for each side of each panel: sug_lo / sug_hi (added sugar at most / above 25 g per 1,000 kcal, 10%
  of calories), whole / refined (bread), prod / nonprod (fruit and vegetables or not), hipos / hineg (UPCs with a Health Index
  above / at or below 0, the products of hi_1000, so fruit and vegetables are in, all above 0 at the fixed score of
  GetHealthIndex.do; non-produce only until 2026-11-12, when Anya asked for the same products as the access figure),
  healthy / unhealthy (claude_hi 4-10 / 0-3):
  shelf_<side>   price of the UPCs offered per 1,000 kcal, each UPC once: 1,000 x the sum of the store's price of one package
                 (spending / units over the year) / the sum of the packages' calories (nominal dollars). Differences in what
                 stores stock and in what they charge both enter.
  rel_<side>     the same UPCs only: mean of log(store unit value / national unit value), clipped at the UPC's 1st and 99th
                 percentiles across stores, weighted by package calories. What stores charge for the same products.
  n_<side>       UPCs offered
  pk_, kc_, rl_, wr_<side>  the sums behind shelf_ and rel_ (prices, package calories, calorie-weighted log relative prices,
                 their weights), kept so that sides can be combined later without a rerun
  rel_healthy_sold, rel_unhealthy_sold: rel_ weighted by national calories sold (the weights of their price figure)
  n_upcs, food_spend (all food UPCs kept)
Store attributes from the year's stores file: channel_code, store_zip3, fips (state x 1000 + county), retailer_code, parent_code.
Output: /users/amarche4/scratch/store_choice/store_choice_sets_<year>.parquet.
Usage: python build_store_choice_sets_rms.py <year> [n_modules]   (n_modules: stop after that many food modules, a test);
python build_store_choice_sets_rms.py combine writes store_choice_sets.parquet (all years), copied to interim/what_moves/.
"""
import gc
import io
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
SIDES = ['sug_lo', 'sug_hi', 'whole', 'refined', 'prod', 'nonprod', 'hipos', 'hineg', 'healthy', 'unhealthy']
SUMS = (['n', 'spend', 'kc', 'add', 'kc_add', 'prod_kc', 'hi', 'kc_hi', 'n4', 'kc_nova', 'bread_kc', 'whole_kc', 'chi', 'rls_healthy', 'wrs_healthy',
         'rls_unhealthy', 'wrs_unhealthy'] + [f'{c}_{x}' for x in SIDES for c in ('n', 'pk', 'kc', 'rl', 'wr')])
ADDSUGAR_CUT = 25.0                     # g per 1,000 kcal = 10% of calories from added sugar


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
    price = d['spend'] / d['units']                                 # the store's price of one package
    sug = 1000 * d['addsugar_unit'] / k
    hi_ok = d['hi_1000'].notna() & np.isfinite(d['hi_1000'])
    prod, bread, whole, h = d['produce'] == 1, d['bread'] == 1, d['whole'] == 1, d['healthy'] == 1
    side = {'sug_lo': sug <= ADDSUGAR_CUT, 'sug_hi': sug > ADDSUGAR_CUT, 'whole': bread & whole, 'refined': bread & ~whole,
            'prod': prod, 'nonprod': ~prod, 'hipos': hi_ok & (d['hi_1000'] > 0), 'hineg': hi_ok & (d['hi_1000'] <= 0),
            'healthy': h, 'unhealthy': ~h}
    x = {'store': d['store'], 'n': 1, 'spend': d['spend'], 'kc': k, 'add': d['addsugar_unit'].where(k.notna()),
         'kc_add': k.where(d['addsugar_unit'].notna()), 'prod_kc': k * d['produce'], 'hi': (k * d['hi_1000']).where(hi_ok),
         'kc_hi': k.where(hi_ok), 'n4': k * d['nova4'], 'kc_nova': k.where(d['nova4'].notna()), 'bread_kc': k * d['bread'],
         'whole_kc': k * d['bread'] * d['whole'], 'chi': k * d['claude_hi'],
         'rls_healthy': (d['wsold'] * d['rel']).where(h), 'wrs_healthy': d['wsold'].where(h),
         'rls_unhealthy': (d['wsold'] * d['rel']).where(~h), 'wrs_unhealthy': d['wsold'].where(~h)}
    for nm, m in side.items():
        m = m.fillna(False) & k.notna()
        x[f'n_{nm}'] = m.astype(int)
        x[f'pk_{nm}'] = price.where(m)
        x[f'kc_{nm}'] = k.where(m)
        x[f'rl_{nm}'] = (k * d['rel']).where(m)
        x[f'wr_{nm}'] = k.where(m & d['rel'].notna())
    return pd.DataFrame(x).groupby('store')[SUMS].sum(min_count=1)


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
                try:                                       # bytes first: pandas cannot read a stream it cannot seek
                    stores = pd.read_csv(io.BytesIO(tf.extractfile(m).read()), sep='\t', dtype=str)
                    log(f'stores file {name}: {len(stores):,} rows')
                except Exception as e:
                    log(f'stores file {name} not read: {e}')
            hit = re.search(r'Movement_Files/\d+_\d+/(\d+)_\d+\.tsv$', name)
            if hit and int(hit.group(1)) in food_modules and (n_modules is None or done < n_modules):
                d = read_movement(tf.extractfile(m))
                if d is not None:
                    s = module_sums(d, attr)
                    if s is not None:
                        acc.append(s)
                del d
                gc.collect()
                done += 1
                if done % 25 == 0:
                    acc = [pd.concat(acc).groupby(level=0).sum(min_count=1)]
                    log(f'{done} food modules, {len(acc[0]):,} stores ({time.time() - t0:.0f}s)')
            tf.members = []
            if n_modules is not None and done >= n_modules:
                break
    s = pd.concat(acc).groupby(level=0).sum(min_count=1)
    cols = {'n_upcs': s['n'], 'food_spend': s['spend'], 'kcal_offered': s['kc'], 'addsugar_1000': 1000 * s['add'] / s['kc_add'],
            'produce_share': s['prod_kc'] / s['kc'], 'whole_share': s['whole_kc'] / s['bread_kc'], 'hi_1000': s['hi'] / s['kc_hi'],
            'nova4_share': s['n4'] / s['kc_nova'], 'claude_hi_mean': s['chi'] / s['kc'],
            'rel_healthy_sold': s['rls_healthy'] / s['wrs_healthy'], 'rel_unhealthy_sold': s['rls_unhealthy'] / s['wrs_unhealthy']}
    for nm in SIDES:
        cols[f'n_{nm}'] = s[f'n_{nm}']
        cols[f'shelf_{nm}'] = 1000 * s[f'pk_{nm}'] / s[f'kc_{nm}']
        cols[f'rel_{nm}'] = s[f'rl_{nm}'] / s[f'wr_{nm}']
        for sm in ('pk', 'kc', 'rl', 'wr'):
            cols[f'{sm}_{nm}'] = s[f'{sm}_{nm}']
    out = pd.DataFrame(cols)
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
