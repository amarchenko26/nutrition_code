"""How much each food module is bought by rich households: the Jaravel object "things rich people buy" (2026-11-05).

For panel years 2008-2010 (the baseline, before the windows used in the regressions): Homescan spending by household x
module from interim/purchases_food/panel_year=<y>/ (one year at a time), merged to household income and projection
weights from interim/panel_dataset/panel_hh_year.parquet; households ranked into within-year weighted income quartiles.
Per module: top_share and bot_share = share of the module's weighted spending from the top and bottom income quartile;
rich = log(top_share / bot_share), the tilt of the module's buyers toward rich households; buyer_income = spending-weighted
mean of real income among the module's buyers ($000s); rich_z = rich standardized over food modules with spending weights.
Output: interim/what_moves/module_buyer_income.parquet
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import gc, glob
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.dataset as ds

YEARS = [2008, 2009, 2010]
OUT = idir / 'what_moves' / 'module_buyer_income.parquet'


def weighted_quartile(g):
    g = g.sort_values('real_income')
    # households in the same income bracket share a quartile: rank on the bracket, not the row
    by = g.groupby('real_income', sort=True)['projection_factor'].sum()
    cdf = (by.cumsum() - 0.5 * by) / by.sum()
    q = np.minimum(np.floor(cdf * 4).astype(int) + 1, 4)
    return g['real_income'].map(q)


def main():
    hh = pd.read_parquet(idir / 'panel_dataset' / 'panel_hh_year.parquet', columns=['household_code', 'panel_year', 'real_income', 'projection_factor'])
    hh = hh[hh['panel_year'].isin(YEARS) & (hh['real_income'] > 0) & hh['projection_factor'].notna()].copy()
    hh['q'] = hh.groupby('panel_year', group_keys=False).apply(weighted_quartile)
    print(hh.groupby(['panel_year', 'q'])['projection_factor'].sum().unstack().div(hh.groupby('panel_year')['projection_factor'].sum(), axis=0).round(3).to_string())
    parts = []
    for y in YEARS:
        fs = glob.glob(str(idir / 'purchases_food' / f'panel_year={y}' / '*.parquet'))
        t = ds.dataset(fs).to_table(columns=['household_code', 'product_module_code', 'total_price_paid'], filter=ds.field('department_descr') != 'MAGNET DATA')
        a = t.group_by(['household_code', 'product_module_code']).aggregate([('total_price_paid', 'sum')]).to_pandas()
        del t; gc.collect()
        a = a.rename(columns={'total_price_paid_sum': 'spend'})
        a = a.merge(hh[hh['panel_year'] == y], on='household_code', how='inner')
        a['w'] = a['spend'] * a['projection_factor']
        a['w_inc'] = a['w'] * a['real_income']
        a['w_loginc'] = a['w'] * np.log(a['real_income'])
        for k in (1, 4):
            a[f'w_q{k}'] = a['w'].where(a['q'] == k, 0.0)
        b = a.groupby('product_module_code')[['w', 'w_inc', 'w_loginc', 'w_q1', 'w_q4']].sum().reset_index()
        b['panel_year'] = y
        parts.append(b)
        print(f'{y}: {len(a):,} household-module rows, {b.product_module_code.nunique()} modules', flush=True)
        del a; gc.collect()
    m = pd.concat(parts).groupby('product_module_code')[['w', 'w_inc', 'w_loginc', 'w_q1', 'w_q4']].sum().reset_index()
    m['top_share'] = m['w_q4'] / m['w']
    m['bot_share'] = m['w_q1'] / m['w']
    m['rich'] = np.log(m['top_share'].clip(lower=1e-4) / m['bot_share'].clip(lower=1e-4))
    m['buyer_income'] = m['w_inc'] / m['w']
    mu = np.average(m['rich'], weights=m['w'])
    sd = np.sqrt(np.average((m['rich'] - mu) ** 2, weights=m['w']))
    m['rich_z'] = (m['rich'] - mu) / sd
    print(f"all food: top-quartile share {m['w_q4'].sum() / m['w'].sum():.3f}, bottom {m['w_q1'].sum() / m['w'].sum():.3f}; rich mean {mu:.3f} sd {sd:.3f}")
    d = m.rename(columns={'w': 'base_weighted_spend'})[['product_module_code', 'base_weighted_spend', 'top_share', 'bot_share', 'rich', 'rich_z', 'buyer_income']]
    d.to_parquet(OUT, index=False)
    g = pd.read_parquet(idir / 'what_moves' / 'module_groups.parquet', columns=['product_module_code', 'product_module_descr', 'hi_group', 'upf_group', 'prep_group', 'eta_iv', 'claude_hi', 'upf_share'])
    x = d.merge(g, on='product_module_code')
    print('top 12 rich:'); print(x.sort_values('rich_z', ascending=False).query('base_weighted_spend > 1e9')[['product_module_descr', 'rich_z', 'top_share', 'bot_share', 'claude_hi', 'upf_share']].head(12).round(2).to_string(index=False))
    print('bottom 12:'); print(x.sort_values('rich_z').query('base_weighted_spend > 1e9')[['product_module_descr', 'rich_z', 'top_share', 'bot_share', 'claude_hi', 'upf_share']].head(12).round(2).to_string(index=False))
    for gr in ['hi_group', 'upf_group', 'prep_group']:
        print(gr, x.groupby(gr).apply(lambda z: pd.Series({'rich_z': np.average(z.rich_z, weights=z.base_weighted_spend), 'top_share': np.average(z.top_share, weights=z.base_weighted_spend), 'n': len(z)})).round(3).to_dict('index'))
    print('corr rich_z with eta_iv, claude_hi, upf_share (spending-unweighted):', x[['rich_z', 'eta_iv', 'claude_hi', 'upf_share']].corr().round(2).iloc[0].to_dict())
    print(f'saved {OUT} ({len(d)} modules)')


if __name__ == '__main__':
    main()
