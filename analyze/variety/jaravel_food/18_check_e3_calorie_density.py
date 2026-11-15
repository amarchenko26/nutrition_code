"""Check of E3 (prints only).  The Health Index per 1,000 kcal falls when calories fall and sodium or cholesterol stay, so
its terciles could sort barcodes by calorie density.  Prints (1) the within-module rank correlation of the Health Index per
1,000 kcal with calories per 100 g; (2) E3 on terciles of calories per 100 g; (3) E3 on the Health Index per 100 g
(non-produce barcodes).  Healthiest (or densest) minus least healthy tercile, within module, weights log mean spending,
clustered by module.

Reads interim/variety/jaravel_food/upc_health.parquet, hms_upc_year.parquet, e123/rms_upc_year_deciles.parquet.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403
import statsmodels.api as sm  # noqa: E402

OUT = DATA / 'e123'
WIN = {'hms': {'base': [2004, 2005, 2006], 'his': ([2013, 2014, 2015], 9), 'ext': ([2018, 2019, 2020], 14)},
       'rms': {'base': [2006, 2007], 'his': ([2014, 2015], 8), 'ext': ([2019, 2020], 13)}}
health = pd.read_parquet(DATA / 'upc_health.parquet')
health['hi_100g'] = np.where(health['produce'] == 0, health['hi_1000'] * health['cal_per_100g'] / 1000, np.nan)
health['cal_100g'] = np.where(health['hi_1000'].notna(), health['cal_per_100g'], np.nan)   # same barcodes as the HI terciles

hms = pd.read_parquet(DATA / 'hms_upc_year.parquet')
hms = hms[hms['quality_rank'].notna()][['upc', 'product_module_code', 'year', 'spend', 'quality_rank']]
rms = pd.read_parquet(OUT / 'rms_upc_year_deciles.parquet')

# (1) rank correlation within module, barcodes bought in Homescan, non-produce
sold = hms[['upc', 'product_module_code']].drop_duplicates('upc')
h = sold.merge(health.drop(columns=['product_module_code']), on='upc')
h = h[(h['produce'] == 0) & h['hi_1000'].notna()]
h['r_hi'] = h.groupby('product_module_code')['hi_1000'].rank(pct=True)
h['r_cal'] = h.groupby('product_module_code')['cal_per_100g'].rank(pct=True)
h['r_hi100'] = h.groupby('product_module_code')['hi_100g'].rank(pct=True)
print(f"within-module rank correlation, HI per 1,000 kcal vs calories per 100 g: {h[['r_hi', 'r_cal']].corr().iloc[0, 1]:.3f}; "
      f"HI per 100 g vs calories per 100 g: {h[['r_hi100', 'r_cal']].corr().iloc[0, 1]:.3f}; "
      f"HI per 1,000 kcal vs HI per 100 g: {h[['r_hi', 'r_hi100']].corr().iloc[0, 1]:.3f}  ({len(h):,} barcodes)")


def growth(S, src, keys):
    w = WIN[src]
    piv = S.pivot_table(index=keys, columns='year', values='spend', aggfunc='sum')
    base = piv.reindex(columns=w['base']).mean(axis=1, skipna=False)
    out = pd.DataFrame(index=piv.index)
    for k in ('his', 'ext'):
        yrs, n = w[k]
        out[f'growth_{k}'] = 100 * (np.log(piv.reindex(columns=yrs).mean(axis=1, skipna=False)) - np.log(base)) / n
        out[f'w_{k}'] = np.log(piv.reindex(columns=range(w['base'][0], yrs[-1] + 1)).mean(axis=1))
    return out.reset_index()


def terciles(df, col, healthy_low):
    x = df[['upc', 'product_module_code', col]].dropna()
    x = x[x.groupby('product_module_code')['upc'].transform('size') >= 30].copy()
    q1 = x.groupby('product_module_code')[col].transform(lambda s: s.quantile(1 / 3))
    q2 = x.groupby('product_module_code')[col].transform(lambda s: s.quantile(2 / 3))
    t = np.where(x[col] <= q1, 1, np.where(x[col] <= q2, 2, 3))
    x['tercile'] = (4 - t) if healthy_low else t
    return x[['upc', 'tercile']]


def fe_reg(g, k):
    """Weighted within-module regression (module fixed effects partialled out, as reghdfe does), clustered by module."""
    d = g.dropna(subset=[f'growth_{k}', f'w_{k}']).copy()
    d = d[d.groupby('product_module_code')['tercile'].transform('size') > 1]
    X = pd.get_dummies(d['tercile'].astype(int), prefix='t', drop_first=True).astype(float)
    cols = list(X.columns)
    d = pd.concat([d, X], axis=1)
    w = d[f'w_{k}']
    for c in cols + [f'growth_{k}']:
        d[c] = d[c] - (d[c] * w).groupby(d['product_module_code']).transform('sum') / w.groupby(d['product_module_code']).transform('sum')
    f = sm.WLS(d[f'growth_{k}'], d[cols], weights=w).fit(cov_type='cluster', cov_kwds={'groups': d['product_module_code']})
    return f.params['t_3'], f.bse['t_3'], len(d), d['product_module_code'].nunique()


rows = []
for src, panel in (('hms', hms), ('rms', rms)):
    sold = panel[['upc', 'product_module_code']].drop_duplicates('upc')
    hh = sold.merge(health.drop(columns=['product_module_code']), on='upc', how='left')
    for meas, low, lab in (('hi_1000', False, 'HI per 1,000 kcal (as in E3)'), ('cal_100g', False, 'calories per 100 g (tercile 3 = densest)'),
                           ('hi_100g', False, 'HI per 100 g, non-produce')):
        t = terciles(hh, meas, low)
        p = panel.merge(t, on='upc', how='inner')
        g = growth(p.groupby(['product_module_code', 'tercile', 'year'], as_index=False)['spend'].sum(), src, ['product_module_code', 'tercile'])
        for k in ('his', 'ext'):
            b, se, n, nm = fe_reg(g, k)
            rows.append(dict(src=src, window=k, measure=lab, top_minus_bottom=round(b, 2), se=round(se, 2), N=n, modules=nm))
r = pd.DataFrame(rows)
print(r.to_string(index=False))
