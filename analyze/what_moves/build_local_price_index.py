"""Local (3-digit-zip market) food price indices from Homescan purchases, by year.

For Anya's "what moves nutrition" review (2026-11-05): do households in markets
where healthy food is relatively cheap eat better? The county-level RMS price
index of the variety work no longer exists, so the index is built from the
panelists' own purchases (interim/purchases_food/panel_year=<y>/), one year at a
time.

Market = the store's 3-digit zip (store_zip3; 16% of rows have none and are
dropped). For every UPC x market x year: unit value = spending / units over the
rows with quantity > 0 and price > 0, after winsorizing each row's unit price at
the UPC-year's 1st and 99th percentiles; the national unit value the same way
over all markets; log relative price = log(local unit value / national unit
value). Only UPC x market cells with at least 5 purchase rows count. Modules are
grouped three ways: healthy (claude_hi 4-10) vs unhealthy (0-3)
(interim/rms_variety/claude_hi_scores.parquet, the fixed HI_CUTS of
analyze/variety/analyze_variety_price_index.py); mostly ultra-processed vs
mostly not (module share of USDA-matched UPCs that are NOVA 4, from
interim/health/upc_dictionary.parquet, split at the median across modules as in
that script's price_level_by_upf); little preparation (prep score 1) vs cooking
(4-9) (interim/rms_variety/prep_time_scores.parquet; 2-3 left out). A market's
group price level = the mean of the UPC log relatives over the group's UPCs
present in the market, weighted by each UPC's NATIONAL spending in that year, so
markets are compared on a common basket (renormalised over the UPCs the market
carries). Relative price = healthy minus unhealthy level (log points), likewise
UPF minus not, little-prep minus cooking. levelcal_* (2026-11-10, for the rich-poor table of
build_local_prices_by_income.py): the same mean weighted by each UPC's NATIONAL calories in that year
(units x utils.food_measures.package_grams x the interim/health/upc_dictionary.parquet density), over
the UPCs with a density; rel_hi_cal = levelcal_healthy - levelcal_unhealthy.

Output: interim/what_moves/local_price_index.parquet, one row per market x year:
level_*, levelcal_* and rel_* (log points), n_upcs_*, n_households (distinct purchasing
households in the market-year), n_rows.
Usage: python3 build_local_price_index.py [--years 2004 2008 ...]
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import argparse, gc, time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from utils.food_measures import package_grams

PURCH = idir / 'purchases_food'
OUT = idir / 'what_moves' / 'local_price_index.parquet'
MIN_ROWS = 5          # purchase rows per UPC x market x year
COLS = ['upc', 'quantity', 'total_price_paid', 'product_module_code', 'store_zip3', 'household_code']
SIZE_COLS = ['product_module_code', 'product_group_code', 'multi', 'size1_amount', 'size1_units']


def module_groups():
    hi = pd.read_parquet(idir / 'rms_variety' / 'claude_hi_scores.parquet', columns=['product_module_code', 'claude_hi'])
    hi['product_module_code'] = pd.to_numeric(hi['product_module_code'], errors='coerce')
    hi['g_hi'] = np.where(hi['claude_hi'] >= 4, 'healthy', 'unhealthy')
    prep = pd.read_parquet(idir / 'rms_variety' / 'prep_time_scores.parquet', columns=['product_module_code', 'prep_time'])
    prep['product_module_code'] = pd.to_numeric(prep['product_module_code'], errors='coerce')
    prep['g_prep'] = np.select([prep['prep_time'] == 1, prep['prep_time'] >= 4], ['lowprep', 'cook'], default=None)
    d = pd.read_parquet(idir / 'health' / 'upc_dictionary.parquet', columns=['product_module_code', 'nova_own']).rename(columns={'nova_own': 'nova'}).dropna(subset=['nova'])
    upf = d.groupby('product_module_code')['nova'].apply(lambda s: (s == 4).mean()).rename('upf_share').reset_index()
    cut = upf['upf_share'].median()
    upf['g_upf'] = np.where(upf['upf_share'] >= cut, 'upf', 'nonupf')
    m = hi[['product_module_code', 'g_hi']].merge(prep[['product_module_code', 'g_prep']], how='outer') \
        .merge(upf[['product_module_code', 'g_upf']], how='outer')
    print(f'modules: {m.g_hi.notna().sum()} scored for healthiness, {m.g_prep.notna().sum()} in a preparation group, '
          f'{m.g_upf.notna().sum()} with an ultra-processed share (median {cut:.2f})', flush=True)
    return m


def winsorize_by_group(key, x, lo=0.01, hi=0.99):
    """Clip x at the lo/hi quantiles within groups of key (both numpy arrays), vectorised."""
    order = np.lexsort((x, key))
    ks, xs = key[order], x[order]
    starts = np.flatnonzero(np.r_[True, ks[1:] != ks[:-1]])
    ends = np.r_[starts[1:], len(ks)]
    n = ends - starts
    lo_pos = starts + np.floor(lo * (n - 1)).astype(np.int64)
    hi_pos = starts + np.ceil(hi * (n - 1)).astype(np.int64)
    lo_v, hi_v = xs[lo_pos], xs[hi_pos]
    grp = np.repeat(np.arange(len(starts)), n)
    xs = np.clip(xs, lo_v[grp], hi_v[grp])
    out = np.empty_like(x)
    out[order] = xs
    return out


def kcal_per_unit(tab, upc, dens):
    """Calories in one unit of each UPC: package grams (first purchase row of the UPC) x dictionary density."""
    _, first = np.unique(upc, return_index=True)
    u = tab.take(pa.array(first)).select(SIZE_COLS).to_pandas()
    u['upc_13'] = '0' + pd.Series(upc[first]).astype(str).str.zfill(12).values
    u['cal_per_100g'] = u['upc_13'].map(dens).values
    return pd.Series(package_grams(u) * u['cal_per_100g'].values / 100, index=upc[first])


def one_year(y, groups, dens):
    t0 = time.time()
    tab = pq.read_table(PURCH / f'panel_year={y}', columns=COLS + SIZE_COLS[1:])
    tab = tab.filter(pc.and_(pc.and_(pc.is_valid(tab['store_zip3']), pc.greater(tab['quantity'], 0)), pc.greater(tab['total_price_paid'], 0)))
    kcal_unit = kcal_per_unit(tab, pc.cast(tab['upc'], pa.int64()).to_numpy(), dens)
    df = pd.DataFrame({
        'upc': pc.cast(tab['upc'], pa.int64()).to_numpy(),
        'qty': tab['quantity'].to_numpy().astype(np.float64),
        'spend': tab['total_price_paid'].to_numpy(),
        'module': tab['product_module_code'].to_numpy(),
        'market': tab['store_zip3'].to_numpy().astype(np.int16),
        'hh': tab['household_code'].to_numpy(),
    })
    del tab
    n_raw = len(df)
    df['p'] = winsorize_by_group(df['upc'].values, (df['spend'] / df['qty']).values)
    df['spend_w'] = df['p'] * df['qty']
    hh = df.groupby('market')['hh'].nunique().rename('n_households')
    nat = df.groupby('upc').agg(spend_nat=('spend_w', 'sum'), qty_nat=('qty', 'sum'), module=('module', 'first'))
    nat['uv_nat'] = nat['spend_nat'] / nat['qty_nat']
    nat['kcal_nat'] = nat['qty_nat'] * kcal_unit.reindex(nat.index).values
    loc = df.groupby(['upc', 'market']).agg(spend=('spend_w', 'sum'), qty=('qty', 'sum'), n=('qty', 'size')).reset_index()
    n_rows_mkt = loc.groupby('market')['n'].sum().rename('n_rows')
    del df; gc.collect()
    loc = loc[loc['n'] >= MIN_ROWS]
    loc['rel'] = np.log(loc['spend'] / loc['qty']) - np.log(loc['upc'].map(nat['uv_nat']))
    loc['w'] = loc['upc'].map(nat['spend_nat'])
    loc['wc'] = loc['upc'].map(nat['kcal_nat'])
    loc['module'] = loc['upc'].map(nat['module'])
    loc = loc.merge(groups, left_on='module', right_on='product_module_code', how='left')
    out = pd.DataFrame({'n_households': hh, 'n_rows': n_rows_mkt})
    for gcol, labels in [('g_hi', ['healthy', 'unhealthy']), ('g_upf', ['upf', 'nonupf']), ('g_prep', ['lowprep', 'cook'])]:
        for lab in labels:
            s = loc[loc[gcol] == lab]
            agg = s.groupby('market').apply(lambda x: pd.Series({'level': np.average(x['rel'], weights=x['w']), 'n_upcs': len(x)}))
            out[f'level_{lab}'] = agg['level']
            out[f'n_upcs_{lab}'] = agg['n_upcs']
            sc = s[s['wc'] > 0]
            out[f'levelcal_{lab}'] = sc.groupby('market').apply(lambda x: np.average(x['rel'], weights=x['wc']))
    out['rel_hi'] = out['level_healthy'] - out['level_unhealthy']
    out['rel_hi_cal'] = out['levelcal_healthy'] - out['levelcal_unhealthy']
    out['rel_upf'] = out['level_upf'] - out['level_nonupf']
    out['rel_prep'] = out['level_lowprep'] - out['level_cook']
    out['year'] = y
    out = out.reset_index().rename(columns={'index': 'market'})
    print(f'{y}: {n_raw:,} valid rows, {len(out):,} markets, {int((out.n_households >= 30).sum())} with 30+ households; '
          f'rel_hi SD across markets {out.loc[out.n_households >= 30, "rel_hi"].std():.3f} ({time.time() - t0:.0f}s)', flush=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--years', nargs='*', type=int, default=list(range(2004, 2021)))
    args = ap.parse_args()
    groups = module_groups()
    dic = pd.read_parquet(idir / 'health' / 'upc_dictionary.parquet', columns=['upc_13', 'cal_per_100g'])
    dens = dic.set_index('upc_13')['cal_per_100g']
    parts = []
    for y in args.years:
        if not (PURCH / f'panel_year={y}').exists():
            continue
        parts.append(one_year(y, groups, dens))
        gc.collect()
    res = pd.concat(parts, ignore_index=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    if OUT.exists() and len(args.years) < 17:
        old = pd.read_parquet(OUT)
        res = pd.concat([old[~old['year'].isin(args.years)], res], ignore_index=True).sort_values(['year', 'market'])
    res.to_parquet(OUT, index=False)
    print(f'saved {len(res):,} market-years to {OUT}', flush=True)


if __name__ == '__main__':
    main()
