"""Which product modules move the shift-share instrument for local relative prices? (2026-11-06)

z_rel_<pair> in build_county_price_index.py is, for each county, the sum over modules of the county's 2008 share (within its
group) times the module's national cumulative log price change since 2008, positive for one group and negative for the
other. Here each module's piece is computed on its own (without the small leave-one-county-out adjustment), and its share of
the instrument's variation net of county and year means is reported (spending-weighted). The instrument with the top
1, 3, 5, 10 modules removed goes to build_local_price_timing.do.
Output: interim/what_moves/local_price_top_modules.dta (county x year: z_<pair>_ex<k>) and local_price_top_modules.csv
(module, name, group, share of variation, 2008 mean share, cumulative national log price change 2008-2020).
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
sys.path.insert(0, str(_r / 'analyze' / 'what_moves'))
from paths import *

import numpy as np
import pandas as pd
from build_county_price_index import module_groups, MAX_ABS, GROUPS, PAIRS

RMS = idir / 'rms_variety'
WM = idir / 'what_moves'
YEARS = list(range(2009, 2021))


def within(x, fips, year, w):
    """Net of county and year means, weighted, by alternating projections."""
    d = pd.DataFrame({'x': x, 'f': fips, 'y': year, 'w': w})
    r = d['x'].astype(float).copy()
    for _ in range(50):
        for k in ('f', 'y'):
            m = (r * d['w']).groupby(d[k]).transform('sum') / d['w'].groupby(d[k]).transform('sum')
            r = r - m
        if r.abs().max() < 1e8 and abs(float((r * d['w']).sum())) < 1e-8:
            break
    return r.values


def main():
    g = module_groups().rename(columns={'product_module_code': 'module'})
    d = pd.read_parquet(RMS / 'price_index_module_fips_year.parquet', columns=['product_module_code', 'fips', 'year', 'log_pi_ces', 'total_spending'])
    d = d.rename(columns={'product_module_code': 'module'})
    d['fips'] = d['fips'].astype('int32'); d['module'] = d['module'].astype('int32'); d['year'] = d['year'].astype('int16')
    b = d.loc[(d['year'] == 2008) & (d['total_spending'] > 0), ['fips', 'module', 'total_spending']].copy()
    ok = d['log_pi_ces'].abs() <= MAX_ABS
    nat = pd.DataFrame({'s': d['total_spending'].where(ok, 0.0), 'sx': (d['total_spending'] * d['log_pi_ces']).where(ok, 0.0),
                        'module': d['module'], 'year': d['year']}).groupby(['module', 'year'])[['s', 'sx']].sum()
    nat = (nat['sx'] / nat['s']).rename('nat').reset_index()
    nat = nat[nat['year'].isin(YEARS)].copy()
    nat['G'] = nat.sort_values('year').groupby('module')['nat'].cumsum()
    names = None
    for f in [idir / 'rms_variety' / 'claude_hi_scores.parquet']:
        c = pd.read_parquet(f)
        col = [x for x in c.columns if 'descr' in x.lower()]
        if col:
            names = c.drop_duplicates('product_module_code').set_index('product_module_code')[col[0]]
    idx = pd.read_parquet(WM / 'county_price_index.parquet', columns=['fips', 'year', 'spend_total', 'z_rel_hi', 'z_rel_upf'])
    idx = idx[idx['year'].isin(YEARS)].dropna(subset=['spend_total'])
    res_rows = []
    out = idx[['fips', 'year']].copy()
    for pair, col in [('hi', 'g_hi'), ('upf', 'g_upf')]:
        a_, b_ = PAIRS[pair]
        pieces = []
        for lab, sign in [(a_, 1.0), (b_, -1.0)]:
            mods = g.loc[g[col] == lab, 'module']
            w = b[b['module'].isin(mods)].copy()
            w['wn'] = w['total_spending'] / w.groupby('fips')['total_spending'].transform('sum')
            w['sign'] = sign
            pieces.append(w[['fips', 'module', 'wn', 'sign']])
        w = pd.concat(pieces, ignore_index=True)
        p = w.merge(nat[['module', 'year', 'G']], on='module')
        p['piece'] = p['wn'] * p['sign'] * p['G']
        tot = p.groupby(['fips', 'year'])['piece'].sum().rename('zz').reset_index()
        m = idx.merge(tot, on=['fips', 'year'], how='left')
        print(f'{pair}: corr(piece sum, z_rel) = {m["zz"].corr(m["z_rel_" + pair]):.3f}')
        zr = within(m['zz'].fillna(0).values, m['fips'].values, m['year'].values, m['spend_total'].values)
        wt = m['spend_total'].values
        var = float((wt * zr * zr).sum())
        keys = m[['fips', 'year']].assign(zr=zr, wt=wt)
        p2 = p.merge(keys, on=['fips', 'year'])
        # covariance of each module's piece with the residualized instrument = its share of the variation (the residual is orthogonal to the fixed effects)
        sh = (p2['wt'] * p2['piece'] * p2['zr']).groupby(p2['module']).sum() / var
        t = pd.DataFrame({'share': sh}).reset_index()
        t['group'] = t['module'].map(w.drop_duplicates('module').set_index('module')['sign']).map({1.0: a_, -1.0: b_})
        t['mean_share_2008'] = t['module'].map(w.groupby('module')['wn'].mean())
        t['G_2020'] = t['module'].map(nat[nat['year'] == 2020].set_index('module')['G'])
        if names is not None:
            t['name'] = t['module'].map(names)
        t['pair'] = pair
        t['abs_share'] = t['share'].abs()
        t = t.sort_values('abs_share', ascending=False)
        res_rows.append(t)
        print(t.head(12).round(3).to_string(index=False))
        print(f'sum of shares {t["share"].sum():.3f}; top 5 by absolute share {t["abs_share"].head(5).sum():.3f}')
        for k in [1, 3, 5, 10]:
            top = set(t['module'].head(k))
            drop = p[p['module'].isin(top)].groupby(['fips', 'year'])['piece'].sum().rename('drop').reset_index()
            mm = idx.merge(drop, on=['fips', 'year'], how='left')
            out[f'z_{pair}_ex{k}'] = mm['z_rel_' + pair].values - mm['drop'].fillna(0).values
    y08 = out.groupby('fips').head(1)[['fips']].assign(year=2008)
    for c in out.columns:
        if c.startswith('z_'):
            y08[c] = 0.0
    out = pd.concat([out, y08], ignore_index=True).rename(columns={'fips': 'county'})
    out['county'] = out['county'].astype('int32'); out['year'] = out['year'].astype('int16')
    out.to_stata(WM / 'local_price_top_modules.dta', write_index=False, version=118)
    pd.concat(res_rows).to_csv(WM / 'local_price_top_modules.csv', index=False)
    print('saved')


if __name__ == '__main__':
    main()
