"""Food module x price-decile cells from national RMS sales, Jaravel (2019) 1b-1e, 4a, 4e, 4f.

Usage: python 4_build_rms_cells.py <END>    (END = 2015 for his years, 2020 for the full years)
Reads  interim/variety/jaravel_food/rms_national/rms_upc_national_<y>.parquet, hms_upc_years.parquet, instrument_cells.parquet;
       raw/products.tsv.
Writes interim/variety/jaravel_food/cells_2006_<END>/analysis_main.parquet (one row per cell of the analysis sample).
Rules (his):
  * deciles: every year, the module's barcodes ranked by national unit value per size unit (spending / units /
    (size1_amount x multi)), each barcode once, ten equal-count groups, after the pooled p1/p99 trim of size1_amount.
  * new in t: not seen in Homescan 2004..t-1 or RMS 2006..t-1; exiting in t: not seen in Homescan or RMS in t+1..END.
  * continuing CES t->t+1: year-t deciles; barcodes sold in both years; pooled p1/p99 trims of the price and quantity ratios;
    Sato-Vartia weights within cell (his Stata rule: a weight is missing when the two shares are equal, so a one-barcode
    cell has index 1).
  * outcomes: modules with all 10 deciles in every year; CES index and Feenstra ratio (1-ssnp_t)/(1-ssep_{t-1})
    winsorized at p5/p95 within department x year; full inflation = log(CES x Feenstra^(1/(sigma-1))), sigma = 5.7
    (his median module sigma; 4, 5 and 11.5 also kept); RMS_lces_price_index_win = log of the mean CES index; full = mean of logs.
  * real spending growth = [log mean spending (END-1, END) - log mean spending (2006, 2007)] / (END-2007) - full inflation;
    weight RMS_avg_spending = mean cell spending over 2006..END except 2010 (his formula).
  * analysis sample: instrument and outcomes present, and all 10 deciles of the module (his 4g rule before Table 5).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403

END = int(sys.argv[1])
START = 2006
YEARS = list(range(START, END + 1))
SIGMAS = {'s57': 5.7, 's4': 4.0, 's115': 11.5, 's5': 5.0}
OUT = DATA / f'cells_{START}_{END}'
OUT.mkdir(exist_ok=True)

prod = load_products()
mod_dept = (prod.dropna(subset=['product_module_code', 'department_code']).groupby('product_module_code')['department_code']
                .agg(lambda s: s.value_counts().idxmax()).rename('department_code').reset_index())
size = prod[['upc', 'size1_amount', 'multi']]
del prod

hms = pd.read_parquet(DATA / 'hms_upc_years.parquet')
hms = hms[hms['year'] <= END]
rms, rms_upcs = {}, {}
for y in YEARS:
    r = pd.read_parquet(RMS_NAT / f'rms_upc_national_{y}.parquet')
    r['upc'] = r['upc'].astype('int64')
    r = r[(r['total_units'] > 0) & (r['total_spending'] > 0)]
    r['average_unit_price_all'] = r['total_spending'] / r['total_units']
    rms[y] = r
    rms_upcs[y] = r[['upc']].assign(year=y)
seen = pd.concat([hms] + list(rms_upcs.values()), ignore_index=True).drop_duplicates()
first_seen = seen.groupby('upc')['year'].min()


def seen_after(t):
    s = seen[(seen['year'] > t) & (seen['year'] <= END)]
    return set(s['upc'].unique())


def base_year_cells(y):
    d = rms[y].merge(size, on='upc', how='inner')
    lo, hi = stata_pctile(d['size1_amount'], 1), stata_pctile(d['size1_amount'], 99)
    d = d[(d['size1_amount'] >= lo) & (d['size1_amount'] <= hi)].copy()
    d['average_unit_price_adj'] = d['average_unit_price_all'] / (d['size1_amount'] * d['multi'])
    return assign_deciles(d, 'average_unit_price_adj')


# new and exiting barcodes by cell-year (his 1d)
newexit, deciles = [], {}
for y in YEARS:
    d = base_year_cells(y)
    deciles[y] = d
    post = seen_after(y)
    d['new'] = ~d['upc'].map(first_seen).lt(y)
    d['exit'] = ~d['upc'].isin(post)
    g = (d.assign(sn=d['total_spending'] * d['new'], se=d['total_spending'] * d['exit'])
           .groupby(['product_module_code', 'quality_rank'], as_index=False)
           .agg(product_group_code=('product_group_code', 'min'), total_spending_QM=('total_spending', 'sum'),
                total_spending_new_QM=('sn', 'sum'), total_spending_exit_QM=('se', 'sum')))
    g['ssnpQM'] = g['total_spending_new_QM'] / g['total_spending_QM']
    g['ssepQM'] = g['total_spending_exit_QM'] / g['total_spending_QM']
    g['year'] = y
    newexit.append(g)
    print(f"{y}: {len(d):,} barcodes after the size trim; new share {d.loc[d.new, 'total_spending'].sum()/d.total_spending.sum():.4f}, "
          f"exit share {d.loc[d.exit, 'total_spending'].sum()/d.total_spending.sum():.4f}", flush=True)
newexit = pd.concat(newexit, ignore_index=True).merge(mod_dept, on='product_module_code', how='left')

# continuing-product CES by cell, t -> t+1 (his 1e)
ces_rows = []
for t in YEARS[:-1]:
    d0 = deciles[t][['upc', 'product_module_code', 'product_group_code', 'quality_rank', 'total_spending', 'total_units', 'average_unit_price_all']]
    d1 = rms[t + 1][['upc', 'total_spending', 'total_units', 'average_unit_price_all']]
    m = d0.merge(d1, on='upc', suffixes=('_old', ''), how='inner')
    m['price_ratio'] = m['average_unit_price_all'] / m['average_unit_price_all_old']
    m['quantity_ratio'] = m['total_units'] / m['total_units_old']
    p1, p99 = stata_pctile(m['price_ratio'], 1), stata_pctile(m['price_ratio'], 99)
    q1, q99 = stata_pctile(m['quantity_ratio'], 1), stata_pctile(m['quantity_ratio'], 99)
    m = m[(m['price_ratio'] >= p1) & (m['price_ratio'] <= p99) & (m['quantity_ratio'] >= q1) & (m['quantity_ratio'] <= q99)].copy()
    k = ['product_module_code', 'quality_rank']
    s1 = m['total_spending'] / m.groupby(k)['total_spending'].transform('sum')
    s0 = m['total_spending_old'] / m.groupby(k)['total_spending_old'].transform('sum')
    with np.errstate(divide='ignore', invalid='ignore'):
        wn = (s1 - s0) / (np.log(s1) - np.log(s0))
    m['wn'] = wn.where(s1 != s0)                           # Stata: 0/0 is missing
    m['wden'] = m.groupby(k)['wn'].transform('sum')        # egen sum ignores missing
    m['lpr'] = np.log(m['price_ratio'])
    m['num'] = (m['wn'] / m['wden']) * m['lpr']
    m['tw'] = 0.5 * (s1 + s0) * m['lpr']                   # Tornqvist, kept for his sample rule
    g = m.groupby(k, as_index=False).agg(lces=('num', 'sum'), ltorn=('tw', 'sum'), n_cont=('upc', 'size'))
    g['ces_price_index'] = np.exp(g['lces'])
    g['tornqvist_price_index'] = np.exp(g['ltorn'])
    g['year'] = t
    ces_rows.append(g)
    print(f"{t}->{t+1}: {len(m):,} continuing barcodes after the ratio trims; cells {len(g):,}; one-barcode cells {(g.n_cont == 1).sum()}", flush=True)
ces = pd.concat(ces_rows, ignore_index=True).merge(mod_dept, on='product_module_code', how='left')


def winsor_dept_year(df, col):
    lo = df.groupby(['department_code', 'year'])[col].transform(lambda s: stata_pctile(s, 5))
    hi = df.groupby(['department_code', 'year'])[col].transform(lambda s: stata_pctile(s, 95))
    return df[col].clip(lower=lo, upper=hi)


n_years = END - START
# (A) new/exit panel (his 4a part 1)
ne = newexit.copy()
wide_S = ne.pivot_table(index=['product_module_code', 'quality_rank'], columns='year', values='total_spending_QM', aggfunc='sum').fillna(0)
wide_S.columns = [f'RMS_total_spending_QM_{c}' for c in wide_S.columns]
lag = ne[['product_module_code', 'quality_rank', 'year', 'ssepQM']].copy()
lag['year'] = lag['year'] + 1
ne = ne.drop(columns='ssepQM').merge(lag, on=['product_module_code', 'quality_rank', 'year'], how='left')
ne = ne[ne['year'] != START]
ne = ne[ne.groupby('product_module_code')['year'].transform('size') == 10 * n_years].copy()
with np.errstate(divide='ignore', invalid='ignore'):
    ne['feenstra'] = (1 - ne['ssnpQM']) / (1 - ne['ssepQM'])
ne.loc[~np.isfinite(ne['feenstra']), 'feenstra'] = np.nan
miss = ne['feenstra'].isna()
ne.loc[miss, ['ssnpQM', 'ssepQM']] = 0
ne.loc[miss, 'feenstra'] = 1
ne['feenstra_win'] = winsor_dept_year(ne, 'feenstra')
for s, sig in SIGMAS.items():
    ne[f'feenstra_win_{s}'] = ne['feenstra_win'] ** (1 / (sig - 1))
ne_cell = (ne.groupby(['product_module_code', 'quality_rank'], as_index=False)
             .agg(RMS_ssnp=('ssnpQM', 'mean'), RMS_ssep=('ssepQM', 'mean'), RMS_feenstra_win=('feenstra_win', 'mean'),
                  RMS_spend_new=('total_spending_new_QM', 'sum'), RMS_spend_all=('total_spending_QM', 'sum')))
ne_cell = ne_cell.merge(wide_S.reset_index(), on=['product_module_code', 'quality_rank'], how='left')
print(f"new/exit panel: {ne['product_module_code'].nunique()} balanced modules", flush=True)

# (B) continuing-product panel (his 4a part 2)
cp = ces[ces.groupby('product_module_code')['year'].transform('size') == 10 * n_years].copy()
cp['ces_price_index_win'] = winsor_dept_year(cp, 'ces_price_index')
cp['tornqvist_price_index_win'] = winsor_dept_year(cp, 'tornqvist_price_index')
cp['year'] = cp['year'] + 1
cp = cp.merge(ne[['product_module_code', 'quality_rank', 'year'] + [f'feenstra_win_{s}' for s in SIGMAS]],
              on=['product_module_code', 'quality_rank', 'year'], how='left')
for s in SIGMAS:
    cp[f'lfullces_pi_win_{s}'] = np.log(cp['ces_price_index_win'] * cp[f'feenstra_win_{s}'])
agg = dict(RMS_ces_price_index_win=('ces_price_index_win', 'mean'), RMS_tornqvist_price_index_win=('tornqvist_price_index_win', 'mean'))
for s in SIGMAS:
    agg[f'RMS_lfullces_pi_win_{s}'] = (f'lfullces_pi_win_{s}', lambda v: v.mean(skipna=False))
cp_cell = cp.groupby(['product_module_code', 'quality_rank'], as_index=False).agg(**agg)
cp_cell['RMS_lces_price_index_win'] = np.log(cp_cell['RMS_ces_price_index_win'])
print(f"continuing panel: {cp['product_module_code'].nunique()} balanced modules", flush=True)

# (C) spending growth (his 4e) and analysis sample (his 4f, 4g)
out = ne_cell.merge(cp_cell, on=['product_module_code', 'quality_rank'], how='outer')
out = out.merge(newexit.groupby('product_module_code')['product_group_code'].min().reset_index(), on='product_module_code', how='left')
out = out.merge(mod_dept, on='product_module_code', how='left')
S = lambda y: out[f'RMS_total_spending_QM_{y}']  # noqa: E731
wy = [y for y in YEARS if y != 2010]
out['RMS_avg_spending'] = sum(S(y) for y in wy) / len(wy)
out['RMS_avg_Dlog_spending'] = (np.log((S(END - 1) + S(END)) / 2) - np.log((S(START) + S(START + 1)) / 2)) / (END - START - 1)
for s in SIGMAS:
    out[f'RMS_avg_Dlog_realspending_{s}'] = out['RMS_avg_Dlog_spending'] - out[f'RMS_lfullces_pi_win_{s}']
out = out.merge(pd.read_parquet(DATA / 'instrument_cells.parquet'), on=['product_module_code', 'quality_rank'], how='left')
need = ['inst_raw', 'RMS_avg_spending', 'RMS_avg_Dlog_realspending_s57', 'RMS_feenstra_win', 'RMS_tornqvist_price_index_win', 'RMS_lfullces_pi_win_s57']
a = out[out[need].notna().all(axis=1) & np.isfinite(out['RMS_avg_Dlog_realspending_s57'])].copy()
a = a[a.groupby('product_module_code')['quality_rank'].transform('size') == 10]
a.to_parquet(OUT / 'analysis_main.parquet', index=False)
tot = {y: newexit.loc[newexit.year == y, 'total_spending_QM'].sum() for y in YEARS}
print(f"analysis sample: {len(a):,} cells in {a['product_module_code'].nunique():,} modules; share of RMS food spending "
      f"(size-trimmed barcodes) {np.mean([a[f'RMS_total_spending_QM_{y}'].sum() / tot[y] for y in YEARS]):.3f}")
w = np.log(a['RMS_avg_spending'])
for c in ['inst_raw', 'inst_linageres', 'inst_allres']:
    m_ = np.average(a[c], weights=w)
    print(f"{c}: mean {m_:.4f} sd {np.sqrt(np.average((a[c] - m_) ** 2, weights=w) * len(a) / (len(a) - 1)):.4f}   (his Table IV B raw 0.0129 / 0.0051)")
