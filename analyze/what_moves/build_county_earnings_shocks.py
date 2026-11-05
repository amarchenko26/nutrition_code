"""County earnings growth from administrative data, for the demand-shock regressions (2026-11-05).

raw/bls/qcew_<y>_filtered.parquet: private employment and average pay by county x NAICS sector (suppressed cells absent)
and nationally by sector. For each county, over the sectors disclosed in BOTH the baseline window 2009-2011 and the end
window, the wage bill = employment x pay is averaged within window: dlog_wagebill = log change in the wage bill,
dlog_pay = log change in pay per worker, dlog_emp = log change in employment. Windows: 2009-2011 to 2018-2020 (main) and
to 2017-2019 (before COVID, suffix _1719). bartik_wagebill = sum over sectors of the county's 2004-2006 wage-bill share
x the national log change in that sector's wage bill (same windows): the county's predicted earnings growth from its
pre-period industry mix. bartik_pay likewise with national pay per worker. Yearly versions for the year-on-year runs:
d_log_wagebill_y, bartik_wagebill_y (consistent-sector year-on-year changes).
Also merges the Homescan-based county series (interim/panel_dataset/county_income_shocks.parquet) as long differences of
log_income and log_income_hat, and prints how they compare. n_panelists comes from that file's county-years.
Outputs: interim/what_moves/county_earnings_longdiff.parquet, county_earnings_yearly.parquet
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import numpy as np
import pandas as pd

W0 = [2009, 2010, 2011]
ENDS = {'': [2018, 2019, 2020], '_1719': [2017, 2018, 2019]}
BASE = [2004, 2005, 2006]


def load():
    fs = sorted((rdir / 'bls').glob('qcew_*_filtered.parquet'))
    d = pd.concat([pd.read_parquet(f, columns=['area_fips', 'industry_code', 'year', 'annual_avg_emplvl', 'avg_annual_pay', 'level']) for f in fs], ignore_index=True)
    d = d[(d['annual_avg_emplvl'] > 0) & (d['avg_annual_pay'] > 0) & (d['industry_code'] != '99')].copy()   # 99 = unclassified
    d['year'] = d['year'].astype(int)
    d['bill'] = d['annual_avg_emplvl'].astype(float) * d['avg_annual_pay'].astype(float)
    return d[d['level'] == 'county'].rename(columns={'area_fips': 'fips'}), d[d['level'] == 'national']


def window(d, years, keys):
    x = d[d['year'].isin(years)].groupby(keys).agg(bill=('bill', 'mean'), emp=('annual_avg_emplvl', 'mean'), n=('year', 'size')).reset_index()
    return x[x['n'] == len(years)]


def main():
    c, n = load()
    print(f'county-sector-years {len(c):,}, counties {c.fips.nunique()}, sectors {c.industry_code.nunique()}')
    base = window(c, BASE, ['fips', 'industry_code'])
    base['s'] = base['bill'] / base.groupby('fips')['bill'].transform('sum')
    out = None
    for suf, W1 in ENDS.items():
        a0 = window(c, W0, ['fips', 'industry_code']); a1 = window(c, W1, ['fips', 'industry_code'])
        m = a0.merge(a1, on=['fips', 'industry_code'], suffixes=('0', '1'))
        g = m.groupby('fips')[['bill0', 'bill1', 'emp0', 'emp1']].sum()
        r = pd.DataFrame({f'dlog_wagebill{suf}': np.log(g['bill1'] / g['bill0']), f'dlog_emp{suf}': np.log(g['emp1'] / g['emp0'])})
        r[f'dlog_pay{suf}'] = r[f'dlog_wagebill{suf}'] - r[f'dlog_emp{suf}']
        n0 = window(n, W0, ['industry_code']); n1 = window(n, W1, ['industry_code'])
        nm = n0.merge(n1, on='industry_code', suffixes=('0', '1'))
        nm['g_bill'] = np.log(nm['bill1'] / nm['bill0'])
        nm['g_pay'] = nm['g_bill'] - np.log(nm['emp1'] / nm['emp0'])
        b = base.merge(nm[['industry_code', 'g_bill', 'g_pay']], on='industry_code')
        b[f'bartik_wagebill{suf}'] = b['s'] * b['g_bill']
        b[f'bartik_pay{suf}'] = b['s'] * b['g_pay']
        bb = b.groupby('fips')[[f'bartik_wagebill{suf}', f'bartik_pay{suf}']].sum()
        r = r.join(bb, how='left')
        r[f'wagebill0{suf}'] = g['bill0']
        out = r if out is None else out.join(r, how='outer')
    # Homescan-based series
    hs = pd.read_parquet(idir / 'panel_dataset' / 'county_income_shocks.parquet')
    hs['fips'] = hs['fips'].astype(str).str.zfill(5)
    for suf, W1 in ENDS.items():
        h0 = hs[hs['year'].isin(W0)].groupby('fips')[['log_income', 'log_income_hat']].mean()
        h1 = hs[hs['year'].isin(W1)].groupby('fips')[['log_income', 'log_income_hat']].mean()
        out[f'dlog_income_hs{suf}'] = h1['log_income'] - h0['log_income']
        out[f'dlog_income_hat_hs{suf}'] = h1['log_income_hat'] - h0['log_income_hat']
    out = out.reset_index()
    out['fips_n'] = out['fips'].astype(int)
    out.to_parquet(idir / 'what_moves' / 'county_earnings_longdiff.parquet', index=False)
    cols = ['dlog_wagebill', 'dlog_pay', 'dlog_emp', 'bartik_wagebill', 'bartik_pay', 'dlog_income_hs', 'dlog_income_hat_hs']
    print(out[cols].describe().T[['count', 'mean', 'std', 'min', 'max']].round(3).to_string())
    print('unweighted correlations:'); print(out[cols].corr().round(2).to_string())
    big = out[out['wagebill0'] >= out['wagebill0'].quantile(0.75)]
    print('top-quartile counties by wage bill, correlations:'); print(big[cols].corr().round(2).to_string())
    # yearly: consistent sectors between t-1 and t
    y = c.sort_values(['fips', 'industry_code', 'year']).copy()
    y['bill_lag'] = y.groupby(['fips', 'industry_code'])['bill'].shift(1)
    y['yr_lag'] = y.groupby(['fips', 'industry_code'])['year'].shift(1)
    y = y[y['yr_lag'] == y['year'] - 1]
    g = y.groupby(['fips', 'year'])[['bill', 'bill_lag']].sum()
    yy = np.log(g['bill'] / g['bill_lag']).rename('d_log_wagebill_y').reset_index()
    nn = n.sort_values(['industry_code', 'year']).copy()
    nn['g'] = np.log(nn['bill'] / nn.groupby('industry_code')['bill'].shift(1))
    bb = base[['fips', 'industry_code', 's']].merge(nn[['industry_code', 'year', 'g']], on='industry_code')
    bb['b'] = bb['s'] * bb['g']
    yb = bb.groupby(['fips', 'year'])['b'].sum().rename('bartik_wagebill_y').reset_index()
    yy = yy.merge(yb, on=['fips', 'year'], how='left')
    yy.to_parquet(idir / 'what_moves' / 'county_earnings_yearly.parquet', index=False)
    print(yy.groupby('year')[['d_log_wagebill_y', 'bartik_wagebill_y']].agg(['mean', 'std']).round(3).tail(13).to_string())
    print('saved county_earnings_longdiff.parquet, county_earnings_yearly.parquet')


if __name__ == '__main__':
    main()
