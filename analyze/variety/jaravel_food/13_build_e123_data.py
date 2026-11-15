"""Data for E1-E3 (Anya, 2026-11-15): spending growth by price decile, barcode health by price decile, and spending growth
by healthiness tercile, within food modules.

Reads  interim/variety/jaravel_food/hms_upc_year.parquet (11_), upc_health.parquet (12_), cells_2006_<END>/analysis_main.parquet
       (4_), rms_national/rms_upc_national_<y>.parquet; raw/products.tsv.
Writes interim/variety/jaravel_food/e123/: e1_hms_cells.dta, e1_rms_cells.dta, e2_upc.dta, e3_groups.dta (read by 14_);
       e3_joint.parquet, e3_coverage.parquet (read by 15_); rms_upc_year_deciles.parquet (read by 18_).
E1  cell (module x decile) spending, deciles by Jaravel's rule in each year (current prices); annualized log growth x 100.
      Homescan: 2004-06 -> 2013-15 (/9, his HMS formula in 4a) and -> 2018-20 (/14); modules with all 10 deciles in every
                year from 2004 to the end year.
      RMS:      2006-07 -> 2014-15 (/8) and -> 2019-20 (/13); the Table 5 analysis cells (nominal and real growth).
E2  each barcode's Homescan price decile in the last year it is bought (2004-2020) and its own-label measures, the per-1,000
    kcal measures winsorized at the pooled p1/p99.
E3  barcodes split within module into terciles of a measure (own labels only; modules with 30+ such barcodes; cut points by
    value, each barcode once; tercile 3 = healthiest); spending growth of each module x tercile, same windows; and growth by
    price decile x tercile, spending pooled over modules.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403

OUT = DATA / 'e123'
OUT.mkdir(exist_ok=True)
hms = pd.read_parquet(DATA / 'hms_upc_year.parquet')
hms = hms[hms['quality_rank'].notna()].copy()           # his size trim
hms['quality_rank'] = hms['quality_rank'].astype(int)
health = pd.read_parquet(DATA / 'upc_health.parquet')

WIN = {'hms': {'base': [2004, 2005, 2006], 'his': ([2013, 2014, 2015], 9), 'ext': ([2018, 2019, 2020], 14)},
       'rms': {'base': [2006, 2007], 'his': ([2014, 2015], 8), 'ext': ([2019, 2020], 13)}}


def growth(S, src, keys):
    """S: frame with keys + year + spend. Returns keys + growth_his, growth_ext, w_his, w_ext (log mean spending)."""
    w = WIN[src]
    piv = S.pivot_table(index=keys, columns='year', values='spend', aggfunc='sum')
    base = piv.reindex(columns=w['base']).mean(axis=1, skipna=False)
    out = pd.DataFrame(index=piv.index)
    for k in ('his', 'ext'):
        yrs, n = w[k]
        end = piv.reindex(columns=yrs).mean(axis=1, skipna=False)
        out[f'growth_{k}'] = 100 * (np.log(end) - np.log(base)) / n
        allyrs = [y for y in range(w['base'][0], yrs[-1] + 1)]
        out[f'w_{k}'] = np.log(piv.reindex(columns=allyrs).mean(axis=1))
    return out.reset_index()


# ---------------- E1, Homescan ----------------
cells = hms.groupby(['product_module_code', 'quality_rank', 'year'], as_index=False)['spend'].sum()
e1 = growth(cells, 'hms', ['product_module_code', 'quality_rank'])
nd = cells.groupby(['product_module_code', 'year'])['quality_rank'].nunique().unstack('year')
for k in ('his', 'ext'):
    last = WIN['hms'][k][0][-1]
    ok = (nd.reindex(columns=range(2004, last + 1)) == 10).all(axis=1)
    e1[f'bal_{k}'] = e1['product_module_code'].map(ok).fillna(False).astype(int)
e1['src'] = 'hms'
print(f"E1 Homescan: modules balanced his {e1.loc[e1.bal_his == 1, 'product_module_code'].nunique()}, "
      f"ext {e1.loc[e1.bal_ext == 1, 'product_module_code'].nunique()} of {e1.product_module_code.nunique()}")
e1.to_stata(OUT / 'e1_hms_cells.dta', write_index=False, version=118)

# ---------------- E1, RMS (Table 5 cells) ----------------
rr = []
for E, k in ((2015, 'his'), (2020, 'ext')):
    a = pd.read_parquet(DATA / f'cells_2006_{E}' / 'analysis_main.parquet')
    t = a[['product_module_code', 'quality_rank']].copy()
    t['growth_nom'] = 100 * a['RMS_avg_Dlog_spending']
    t['growth_real'] = 100 * a['RMS_avg_Dlog_realspending_s57']
    t['w'] = np.log(a['RMS_avg_spending'])
    t['window'] = k
    rr.append(t)
pd.concat(rr).to_stata(OUT / 'e1_rms_cells.dta', write_index=False, version=118)

# ---------------- E2 ----------------
last = hms.sort_values('year').groupby('upc').tail(1)[['upc', 'product_module_code', 'year', 'quality_rank', 'spend']]
e2 = last.merge(health.drop(columns=['product_module_code', 'product_group_code']), on='upc', how='inner')
for c in ['addsugar_1000', 'sugar_1000', 'hi_1000']:
    lo, hi = e2[c].quantile([0.01, 0.99])
    e2[c + '_w'] = e2[c].clip(lo, hi)
e2['organic_pct'] = 100 * e2['organic']
print(f"E2 barcodes bought in Homescan: {len(last):,}; with added sugar {e2.addsugar_1000.notna().sum():,}, sugar "
      f"{e2.sugar_1000.notna().sum():,}, HI {e2.hi_1000.notna().sum():,}, organic coded {e2.organic.notna().sum():,}")
e2[['upc', 'product_module_code', 'year', 'quality_rank', 'spend', 'addsugar_1000_w', 'sugar_1000_w', 'hi_1000_w', 'organic_pct',
    'produce']].to_stata(OUT / 'e2_upc.dta', write_index=False, version=118)


# ---------------- E3: health terciles within module ----------------
def terciles(df, col, healthy_low):
    """Tercile by value within module among barcodes with an own value; 1 = least healthy, 3 = healthiest."""
    x = df[['upc', 'product_module_code', col]].dropna()
    n = x.groupby('product_module_code')['upc'].transform('size')
    x = x[n >= 30].copy()
    q1 = x.groupby('product_module_code')[col].transform(lambda s: s.quantile(1 / 3))
    q2 = x.groupby('product_module_code')[col].transform(lambda s: s.quantile(2 / 3))
    t = np.where(x[col] <= q1, 1, np.where(x[col] <= q2, 2, 3))   # 1 = lowest values
    x['tercile'] = (4 - t) if healthy_low else t                   # 3 = healthiest
    return x[['upc', 'tercile']]


def rms_upc_year():
    size = load_products()[['upc', 'size1_amount', 'multi']]
    out = []
    for y in range(2006, 2021):
        r = pd.read_parquet(RMS_NAT / f'rms_upc_national_{y}.parquet')
        r['upc'] = r['upc'].astype('int64')
        r = r[(r['total_units'] > 0) & (r['total_spending'] > 0)].merge(size, on='upc', how='inner')
        lo, hi = stata_pctile(r['size1_amount'], 1), stata_pctile(r['size1_amount'], 99)
        r = r[(r['size1_amount'] >= lo) & (r['size1_amount'] <= hi)].copy()
        r['price_adj'] = r['total_spending'] / r['total_units'] / (r['size1_amount'] * r['multi'])
        r = assign_deciles(r, 'price_adj')
        r['year'] = y
        out.append(r[['upc', 'product_module_code', 'year', 'quality_rank']].assign(spend=r['total_spending']))
    return pd.concat(out, ignore_index=True)


rms = rms_upc_year()
rms.to_parquet(OUT / 'rms_upc_year_deciles.parquet', index=False)
grp_rows, joint_rows, cov_rows = [], [], []
for src, panel in (('hms', hms), ('rms', rms)):
    sold = panel[['upc', 'product_module_code']].drop_duplicates('upc')
    h = sold.merge(health.drop(columns=['product_module_code']), on='upc', how='left')
    for meas, healthy_low in (('addsugar_1000', True), ('hi_1000', False)):
        t = terciles(h, meas, healthy_low)
        p = panel.merge(t, on='upc', how='left')
        # coverage: share of spending on barcodes with a tercile, base and last years
        w = WIN[src]
        for yy in (w['base'][0], w['ext'][0][-1]):
            s = p[p.year == yy]
            cov_rows.append(dict(src=src, measure=meas, year=yy, share=s.loc[s.tercile.notna(), 'spend'].sum() / s['spend'].sum()))
        p = p[p['tercile'].notna()]
        g = growth(p.groupby(['product_module_code', 'tercile', 'year'], as_index=False)['spend'].sum(), src,
                   ['product_module_code', 'tercile'])
        g['src'], g['measure'] = src, meas
        grp_rows.append(g)
        # joint: price decile x tercile, spending pooled over modules
        j = growth(p.groupby(['quality_rank', 'tercile', 'year'], as_index=False)['spend'].sum(), src, ['quality_rank', 'tercile'])
        jt = growth(p.groupby(['tercile', 'year'], as_index=False)['spend'].sum(), src, ['tercile']).assign(quality_rank=0)
        jd = growth(p.groupby(['quality_rank', 'year'], as_index=False)['spend'].sum(), src, ['quality_rank']).assign(tercile=0)
        j = pd.concat([j, jt, jd], ignore_index=True)
        j['src'], j['measure'] = src, meas
        joint_rows.append(j)
        print(f"E3 {src} {meas}: modules {t.merge(sold, on='upc').product_module_code.nunique()}, barcodes {len(t):,}", flush=True)
g = pd.concat(grp_rows, ignore_index=True)
g.to_stata(OUT / 'e3_groups.dta', write_index=False, version=118)
pd.concat(joint_rows, ignore_index=True).to_parquet(OUT / 'e3_joint.parquet', index=False)
cov = pd.DataFrame(cov_rows)
cov.to_parquet(OUT / 'e3_coverage.parquet', index=False)
print(cov.round(3).to_string(index=False))
