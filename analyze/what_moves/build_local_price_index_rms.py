"""County price levels of healthy and unhealthy groceries from Nielsen RMS store data, one year per call -- run on OSCAR.

For the rich-poor local price table (Anya 2026-11-10: "this needs to be RMS not HMS"; build_local_prices_by_income.py).
Reads /users/amarche4/data/rms_variety/rms_upc_fips_spending_<year>.parquet (UPC x county store spending and units, spending
= units x price / prmult, build/variety/build_product_variety.py) and rms_upc_groups.parquet (build_rms_upc_groups.py, made
locally and copied over). For every UPC x county with MIN_UNITS or more units: unit value = spending / units; national unit
value = the UPC's spending / units over all counties; log relative price = log(county / national), clipped at the UPC's 1st
and 99th percentiles across counties. A county's group level = the mean of the log relatives over the group's UPCs sold in
the county, weighted by the UPC's NATIONAL spending (level_*) or NATIONAL calories (levelcal_*, UPCs with a density), so
counties are compared on a common basket. Healthy = claude_hi 4-10, unhealthy = 0-3.
Outputs (same directory): rms_county_price_index_<year>.parquet: fips, year, level_*, levelcal_*, n_upcs_*, spending (all food
UPCs in the county); rms_national_groups_<year>.parquet: healthy, year, spending, grams, kcal (UPCs with grams; kcal over
UPCs with a density, with their spending as spend_kcal) -- national prices per kg and per 1,000 kcal from the same stores.
Usage: python build_local_price_index_rms.py <year>; then python build_local_price_index_rms.py combine, which writes
rms_county_price_index.parquet and rms_national_groups.parquet (all years), copied to interim/what_moves/.
"""
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

D = Path('/users/amarche4/data/rms_variety')
MIN_UNITS = 10


def clip_by_group(key, x, lo=0.01, hi=0.99):
    """Clip x at the lo/hi quantiles within groups of key (numpy arrays), vectorised."""
    order = np.lexsort((x, key))
    ks, xs = key[order], x[order]
    starts = np.flatnonzero(np.r_[True, ks[1:] != ks[:-1]])
    ends = np.r_[starts[1:], len(ks)]
    n = ends - starts
    lo_v = xs[starts + np.floor(lo * (n - 1)).astype(np.int64)]
    hi_v = xs[starts + np.ceil(hi * (n - 1)).astype(np.int64)]
    grp = np.repeat(np.arange(len(starts)), n)
    out = np.empty_like(x)
    out[order] = np.clip(xs, lo_v[grp], hi_v[grp])
    return out


def main(year):
    t0 = time.time()
    g = pd.read_parquet(D / 'rms_upc_groups.parquet')
    t = pq.read_table(D / f'rms_upc_fips_spending_{year}.parquet', columns=['upc', 'fips', 'total_spending', 'total_units'])
    df = pd.DataFrame({'upc': pc.cast(t['upc'], pa.int64()).to_numpy(), 'fips': pc.cast(t['fips'], pa.int32()).to_numpy(),
                       'spend': t['total_spending'].to_numpy(), 'units': t['total_units'].to_numpy()})
    del t
    df = df[(df['spend'] > 0) & (df['units'] > 0)].merge(g, on='upc', how='inner')
    print(f'{year}: {len(df):,} food UPC x county rows ({time.time() - t0:.0f}s)', flush=True)
    nat = df.groupby('upc').agg(spend_nat=('spend', 'sum'), units_nat=('units', 'sum'), healthy=('healthy', 'first'),
                                grams_unit=('grams_unit', 'first'), kcal_unit=('kcal_unit', 'first'))
    nat['uv_nat'] = nat['spend_nat'] / nat['units_nat']
    nat['kcal_nat'] = nat['units_nat'] * nat['kcal_unit']
    # national totals by group, for prices per kg and per 1,000 kcal
    ng = nat.assign(grams=nat['units_nat'] * nat['grams_unit'], kcal=nat['kcal_nat'],
                    spend_g=nat['spend_nat'].where(nat['grams_unit'].notna()), spend_kcal=nat['spend_nat'].where(nat['kcal_unit'].notna()))
    ng = ng.groupby('healthy').agg(spending=('spend_nat', 'sum'), spend_g=('spend_g', 'sum'), grams=('grams', 'sum'),
                                   spend_kcal=('spend_kcal', 'sum'), kcal=('kcal', 'sum'), n_upcs=('spend_nat', 'size')).reset_index()
    ng['year'] = year
    ng.to_parquet(D / f'rms_national_groups_{year}.parquet', index=False)
    county_spend = df.groupby('fips')['spend'].sum().rename('spending')
    df = df[df['units'] >= MIN_UNITS]
    df['rel'] = np.log(df['spend'] / df['units']) - np.log(df['upc'].map(nat['uv_nat']).values)
    df['rel'] = clip_by_group(df['upc'].values, df['rel'].values)
    df['w'] = df['upc'].map(nat['spend_nat']).values
    df['wc'] = df['upc'].map(nat['kcal_nat']).values
    out = pd.DataFrame({'spending': county_spend})
    for h, lab in [(1, 'healthy'), (0, 'unhealthy')]:
        s = df[df['healthy'] == h]
        s = s.assign(rw=s['rel'] * s['w'], rwc=(s['rel'] * s['wc']).where(s['wc'] > 0), wcp=s['wc'].where(s['wc'] > 0))
        a = s.groupby('fips').agg(rw=('rw', 'sum'), w=('w', 'sum'), rwc=('rwc', 'sum'), wc=('wcp', 'sum'), n=('rel', 'size'))
        out[f'level_{lab}'] = a['rw'] / a['w']
        out[f'levelcal_{lab}'] = a['rwc'] / a['wc']
        out[f'n_upcs_{lab}'] = a['n']
    out['year'] = year
    out = out.reset_index().rename(columns={'index': 'fips'})
    out.to_parquet(D / f'rms_county_price_index_{year}.parquet', index=False)
    big = out[out['n_upcs_healthy'] >= 500]
    print(f'{year}: {len(out):,} counties, {len(big):,} with 500+ healthy UPCs; SD of healthy minus unhealthy level there '
          f'{(big.level_healthy - big.level_unhealthy).std():.4f} ({time.time() - t0:.0f}s)', flush=True)


def combine():
    for stem in ('rms_county_price_index', 'rms_national_groups'):
        parts = sorted(D.glob(f'{stem}_*.parquet'))
        pd.concat([pd.read_parquet(f) for f in parts], ignore_index=True).to_parquet(D / f'{stem}.parquet', index=False)
        print(f'{stem}: {len(parts)} years combined')


if __name__ == '__main__':
    combine() if sys.argv[1] == 'combine' else main(int(sys.argv[1]))
