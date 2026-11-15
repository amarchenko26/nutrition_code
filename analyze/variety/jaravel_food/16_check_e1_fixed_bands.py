"""Check of E1 (prints only): price bands fixed at the base window instead of equal-count deciles every year.
Each barcode-year's price per size unit is divided by its module's median in that year (barcodes counted once); the band
cut points are the deciles of that relative price over the module's barcodes in the base window (Homescan 2004-06, RMS
2006-07).  Later barcodes are placed by those fixed cut points, so a band can gain or lose barcodes.  Growth as in E1;
regression on band dummies (and the band number) within module, weights log mean spending, clustered by module.

Reads interim/variety/jaravel_food/hms_upc_year.parquet, rms_national/rms_upc_national_<y>.parquet; raw/products.tsv.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403
import statsmodels.api as sm  # noqa: E402

WIN = {'hms': {'base': [2004, 2005, 2006], 'his': ([2013, 2014, 2015], 9), 'ext': ([2018, 2019, 2020], 14)},
       'rms': {'base': [2006, 2007], 'his': ([2014, 2015], 8), 'ext': ([2019, 2020], 13)}}

hms = pd.read_parquet(DATA / 'hms_upc_year.parquet')
hms = hms[hms['quality_rank'].notna()][['upc', 'product_module_code', 'year', 'spend', 'price_adj']]
size = load_products()[['upc', 'size1_amount', 'multi']]
rr = []
for y in range(2006, 2021):
    r = pd.read_parquet(RMS_NAT / f'rms_upc_national_{y}.parquet')
    r['upc'] = r['upc'].astype('int64')
    r = r[(r.total_units > 0) & (r.total_spending > 0)].merge(size, on='upc')
    lo, hi = stata_pctile(r['size1_amount'], 1), stata_pctile(r['size1_amount'], 99)
    r = r[(r.size1_amount >= lo) & (r.size1_amount <= hi)]
    rr.append(pd.DataFrame({'upc': r.upc, 'product_module_code': r.product_module_code, 'year': y, 'spend': r.total_spending,
                            'price_adj': r.total_spending / r.total_units / (r.size1_amount * r.multi)}))
rms = pd.concat(rr, ignore_index=True)

rows = []
for src, p in (('hms', hms), ('rms', rms)):
    w = WIN[src]
    p = p.copy()
    p['rel'] = p['price_adj'] / p.groupby(['product_module_code', 'year'])['price_adj'].transform('median')
    base = p[p.year.isin(w['base'])]
    cuts = base.groupby('product_module_code')['rel'].quantile([i / 10 for i in range(1, 10)]).unstack()
    p = p.merge(cuts, left_on='product_module_code', right_index=True, how='inner')
    cm = p[[i / 10 for i in range(1, 10)]].to_numpy()
    p['band'] = 1 + (p['rel'].to_numpy()[:, None] > cm).sum(axis=1)
    S = p.groupby(['product_module_code', 'band', 'year'])['spend'].sum().unstack('year')
    b0 = S.reindex(columns=w['base']).mean(axis=1, skipna=False)
    for k in ('his', 'ext'):
        yrs, n = w[k]
        g = 100 * (np.log(S.reindex(columns=yrs).mean(axis=1, skipna=False)) - np.log(b0)) / n
        wt = np.log(S.reindex(columns=range(w['base'][0], yrs[-1] + 1)).mean(axis=1))
        d = pd.DataFrame({'g': g, 'w': wt}).dropna().reset_index()
        full = d.groupby('product_module_code')['band'].transform('size') == 10
        d = d[full]
        X = pd.get_dummies(d['band'].astype(int), prefix='b', drop_first=True).astype(float)
        X = pd.concat([X, pd.get_dummies(d['product_module_code'], prefix='m', drop_first=True).astype(float)], axis=1)
        X = sm.add_constant(X)
        fit = sm.WLS(d['g'], X, weights=d['w']).fit(cov_type='cluster', cov_kwds={'groups': d['product_module_code']})
        Xl = sm.add_constant(pd.concat([d[['band']].astype(float), X.filter(like='m_')], axis=1))
        fl = sm.WLS(d['g'], Xl, weights=d['w']).fit(cov_type='cluster', cov_kwds={'groups': d['product_module_code']})
        rows.append(dict(src=src, window=k, d10=fit.params['b_10'], se10=fit.bse['b_10'], slope=fl.params['band'], se_slope=fl.bse['band'],
                         N=len(d), nmod=d.product_module_code.nunique()))
        print(src, k, f"band 10 - band 1 {fit.params['b_10']:.2f} ({fit.bse['b_10']:.2f}); per band {fl.params['band']:.3f} ({fl.bse['band']:.3f}); "
              f"cells {len(d)}, modules {d.product_module_code.nunique()}", flush=True)
    # count of barcodes per band at the end of the window vs base (mean over modules)
    cnt = p.groupby(['year', 'band'])['upc'].count().unstack('band')
    print(src, 'barcodes per band, base first year vs last year:\n', cnt.loc[[w['base'][0], w['ext'][0][-1]]].to_string())
