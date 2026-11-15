"""Per RMS store, year t and module cell: barcodes offered, barcodes new nationally, and the pieces of a count-weighted
variety-adjusted price change from t-1 to t -- run on OSCAR after build_store_upc_year_rms.py for t-1 and t.
Cells: module_cells.parquet, 100 x unhealthy (claude_hi 0-3) + 10 x preparation group (1 open and eat, 2 little, 3 cooking,
0 no score) + 1 if a chip module. New nationally: the UPC's first year sold anywhere in RMS (upc_first_year.parquet) = t.
Per store x cell: n_t (barcodes sold in t), n_new (of which new nationally in t), n_new2 (first sold nationally in t-1 or t),
n_new_nrc (n_new without barcodes typed re-coded in new_upc_types.parquet, upc_recoded.parquet), n_tm1 (barcodes sold in t-1),
n_cont (sold in both), sum_dlp (sum over continuing barcodes of log(unit value t / unit value t-1), clipped at the module's 1st and
99th percentiles), n_cont10 and sum_dlp10 (the same over continuing barcodes with 10 or more units in both years).
Writes /users/amarche4/scratch/new_products_places/store_cells_<t>.parquet.
Usage: python build_store_new_options_rms.py <t>"""
import gc, sys, time
from pathlib import Path
import numpy as np, pandas as pd

O = Path('/users/amarche4/scratch/new_products_places')


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


def read(year, mod):
    f = O / 'store_upc' / str(year) / f'm{mod}.parquet'
    if not f.exists():
        return None
    d = pd.read_parquet(f)
    d['lp'] = np.log(d['spend'].astype(np.float64) / d['units'].astype(np.float64))
    return d[['store', 'upc', 'lp', 'units']]


def main(t):
    t0 = time.time()
    cells = pd.read_parquet(O / 'module_cells.parquet')
    fyf = O / 'upc_first_year_all.parquet'            # first year at any RMS store (build_upc_first_year_all_rms.py); the variety-file map otherwise
    fy = pd.read_parquet(fyf if fyf.exists() else O / 'upc_first_year.parquet').set_index('upc')['first_year']
    log(f'first-year map: {fyf.name if fyf.exists() else "upc_first_year.parquet"}')
    rec = set(pd.read_parquet(O / 'upc_recoded.parquet')['upc'].astype(np.int64))
    acc, missing_fy, n_mod = [], 0, 0
    for mod, cell in zip(cells['product_module_code'].astype(int), cells['cell'].astype(int)):
        b = read(t, mod)
        a = read(t - 1, mod)
        if b is None and a is None:
            continue
        n_mod += 1
        parts = []
        if b is not None:
            f = b['upc'].map(fy)
            missing_fy += int(f.isna().sum())
            b['new'] = (f == t).astype(np.int32)
            b['new2'] = f.between(t - 1, t).astype(np.int32)
            b['new_nrc'] = ((f == t) & ~b['upc'].isin(rec)).astype(np.int32)
            parts.append(b.groupby('store').agg(n_t=('upc', 'size'), n_new=('new', 'sum'), n_new2=('new2', 'sum'), n_new_nrc=('new_nrc', 'sum')))
        if a is not None:
            parts.append(a.groupby('store').agg(n_tm1=('upc', 'size')))
        if a is not None and b is not None:
            c = a.merge(b[['store', 'upc', 'lp', 'units']], on=['store', 'upc'], suffixes=('_a', '_b'))
            if len(c):
                dlp = (c['lp_b'] - c['lp_a']).values
                lo, hi = np.nanpercentile(dlp, [1, 99])
                c['dlp'] = np.clip(dlp, lo, hi)
                c['big'] = ((c['units_a'] >= 10) & (c['units_b'] >= 10)).astype(np.int32)
                c['dlp10'] = c['dlp'] * c['big']
                parts.append(c.groupby('store').agg(n_cont=('upc', 'size'), sum_dlp=('dlp', 'sum'), n_cont10=('big', 'sum'), sum_dlp10=('dlp10', 'sum')))
        s = pd.concat(parts, axis=1)
        s['cell'] = cell
        acc.append(s.reset_index().rename(columns={'index': 'store'}))
        del a, b
        if n_mod % 100 == 0:
            gc.collect()
            log(f'{t}: {n_mod} modules ({time.time() - t0:.0f}s)')
    d = pd.concat(acc, ignore_index=True)
    COLS = ['n_t', 'n_new', 'n_new2', 'n_new_nrc', 'n_tm1', 'n_cont', 'sum_dlp', 'n_cont10', 'sum_dlp10']
    for c in COLS:
        if c not in d.columns:
            d[c] = np.nan
    out = d.groupby(['store', 'cell'])[COLS].sum(min_count=1).reset_index()
    out['year'] = t
    out.to_parquet(O / f'store_cells_{t}.parquet', index=False)
    log(f'{t}: {n_mod} modules, {out.store.nunique():,} stores, {len(out):,} store x cell rows; barcodes without a first year {missing_fy:,} '
        f'({time.time() - t0:.0f}s)')
    print(out.groupby('cell')[['n_t', 'n_new', 'n_new2', 'n_new_nrc', 'n_tm1', 'n_cont', 'n_cont10']].sum().to_string())


if __name__ == '__main__':
    main(int(sys.argv[1]))
