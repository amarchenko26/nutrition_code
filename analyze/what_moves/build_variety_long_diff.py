"""County x module long differences for the Jaravel-by-healthiness regressions (2026-11-05).

All RMS counties, not only those with enough Homescan panelists. Windows: W0 = 2009-2011 to W1 = 2018-2020; suffix _1719
= W1 2017-2019 (before COVID). A county-module needs at least 2 of the 3 years in each window and mean spending of
$1,000 a year in W0.
From interim/rms_variety/rms_variety_module_fips_year.parquet: dlog_spend = log change in mean spending; dlog_nupcs = log
change in the mean number of UPCs sold in the county; d_ssnp = change in the share of spending on products new to the
nation that year. d_new_county = change in the share of the county's UPCs that are new to the county (new_upc_county_counts).
From price_index_module_fips_year.parquet (kept when n_cont >= 10, |log_pi_ces| <= 0.5, |log_pi_va| <= 0.5):
dlog_ces, dlog_va = cumulative log change in the price of continuing products and in the variety-adjusted price from the
midpoint of W0 to the midpoint of W1: the mean annual change over 2011-2019 (at least 7 of the 9 years; 2011-2018 and 6 of 8 for
_1719) times the number of years. dlog_real = dlog_spend - dlog_ces (quantity at local prices).
breadth0 = the county's mean number of UPCs in W0 / the module's national number in W0; launch_z = breadth0 x the module's
national log change in UPCs (the national-launches-times-stocking-breadth instrument for deliverable 4).
Module columns: hi_group, upf_group, prep_group, eta_iv, rich_z. County columns: county_earnings_longdiff.parquet.
Output: interim/what_moves/variety_longdiff.dta
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import gc
import numpy as np
import pandas as pd
import pyreadstat

W0 = [2009, 2010, 2011]
ENDS = {'': ([2018, 2019, 2020], list(range(2011, 2020)), 7), '_1719': ([2017, 2018, 2019], list(range(2011, 2019)), 6)}


def wmean_window(d, years, cols):
    x = d[d['year'].isin(years)].groupby(['product_module_code', 'fips'], observed=True).agg(n=('year', 'size'), **{c: (c, 'mean') for c in cols})
    return x[x['n'] >= 2].drop(columns='n')


def main():
    v = pd.read_parquet(idir / 'rms_variety' / 'rms_variety_module_fips_year.parquet',
                        columns=['product_module_code', 'fips', 'year', 'total_spending', 'ssnp', 'n_upcs'])
    v = v[v['year'] >= 2009].copy()
    v['fips'] = v['fips'].astype(str).str.zfill(5).astype('category')
    new = pd.read_parquet(idir / 'what_moves' / 'new_upc_county_counts.parquet')
    new['fips'] = pd.Categorical(new['fips'], categories=v['fips'].cat.categories)
    new = new[new['fips'].notna() & (new['year'] >= 2009)]
    v = v.merge(new, on=['product_module_code', 'fips', 'year'], how='left')
    del new; gc.collect()
    v['n_new_county'] = v['n_new_county'].fillna(0)
    v['new_county'] = (v['n_new_county'] / v['n_upcs'].clip(lower=1)).astype('float32')
    for c in ['total_spending', 'ssnp', 'n_upcs']:
        v[c] = v[c].astype('float32')
    print(f'{len(v):,} module-county-years, {v.fips.nunique()} counties, {v.product_module_code.nunique()} modules; total spending by year $bn:', (v.groupby('year')['total_spending'].sum() / 1e9).round(1).to_dict(), flush=True)
    pi = pd.read_parquet(idir / 'rms_variety' / 'price_index_module_fips_year.parquet', columns=['product_module_code', 'fips', 'year', 'log_pi_ces', 'log_pi_va', 'n_cont'])
    pi = pi[(pi['n_cont'] >= 10) & (pi['log_pi_ces'].abs() <= 0.5) & (pi['log_pi_va'].abs() <= 0.5) & (pi['year'] >= 2011)].copy()
    pi['fips'] = pd.Categorical(pi['fips'].astype(str).str.zfill(5), categories=v['fips'].cat.categories)
    pi = pi[pi['fips'].notna()]
    print(f'price rows kept {len(pi):,}', flush=True)
    # national UPC counts
    nat, _ = pyreadstat.read_dta(str(idir / 'panel_dataset' / 'innovation_reg_data.dta'), usecols=['module_code', 'year', 'n_upcs'])
    nat = nat.rename(columns={'module_code': 'product_module_code', 'n_upcs': 'n_upcs_nat'})
    nat['year'] = nat['year'].astype(int); nat['product_module_code'] = nat['product_module_code'].astype(int)
    a0 = wmean_window(v, W0, ['total_spending', 'ssnp', 'n_upcs', 'new_county'])
    a0.columns = [c + '0' for c in a0.columns]
    out = a0[a0['total_spending0'] >= 1000].copy()
    n0 = nat[nat['year'].isin(W0)].groupby('product_module_code')['n_upcs_nat'].mean()
    for suf, (W1, pyears, minyrs) in ENDS.items():
        a1 = wmean_window(v, W1, ['total_spending', 'ssnp', 'n_upcs', 'new_county'])
        m = out[['total_spending0', 'ssnp0', 'n_upcs0', 'new_county0']].join(a1, how='inner')
        r = pd.DataFrame(index=m.index)
        r[f'dlog_spend{suf}'] = np.log(m['total_spending'] / m['total_spending0'])
        r[f'dlog_nupcs{suf}'] = np.log(m['n_upcs'] / m['n_upcs0'])
        r[f'd_ssnp{suf}'] = m['ssnp'] - m['ssnp0']
        r[f'd_new_county{suf}'] = m['new_county'] - m['new_county0']
        x = pi[pi['year'].isin(pyears)].groupby(['product_module_code', 'fips'], observed=True).agg(n=('year', 'size'), a=('log_pi_ces', 'mean'), b=('log_pi_va', 'mean'))
        x = x[x['n'] >= minyrs]
        r[f'dlog_ces{suf}'] = (x['a'] * len(pyears)).reindex(r.index)
        r[f'dlog_va{suf}'] = (x['b'] * len(pyears)).reindex(r.index)
        r[f'dlog_real{suf}'] = r[f'dlog_spend{suf}'] - r[f'dlog_ces{suf}']
        n1 = nat[nat['year'].isin(W1)].groupby('product_module_code')['n_upcs_nat'].mean()
        gm = np.log(n1 / n0).rename('g')
        r[f'launch_g{suf}'] = gm.reindex(r.index.get_level_values('product_module_code')).values
        out = out.join(r, how='left')
    out = out.reset_index()
    del v, pi, a0; gc.collect()
    out['fips'] = out['fips'].astype(str)
    out['breadth0'] = out['n_upcs0'] / out['product_module_code'].map(n0)
    for suf in ENDS:
        out[f'launch_z{suf}'] = out['breadth0'] * out[f'launch_g{suf}']
    g = pd.read_parquet(idir / 'what_moves' / 'module_groups.parquet', columns=['product_module_code', 'hi_group', 'upf_group', 'prep_group', 'eta_iv'])
    r = pd.read_parquet(idir / 'what_moves' / 'module_buyer_income.parquet', columns=['product_module_code', 'rich_z'])
    c = pd.read_parquet(idir / 'what_moves' / 'county_earnings_longdiff.parquet').drop(columns=['fips_n'])
    out = out.merge(g, on='product_module_code', how='left').merge(r, on='product_module_code', how='left').merge(c, on='fips', how='left')
    out['fips_n'] = out['fips'].astype(int)
    out = out.drop(columns=['fips', 'launch_g', 'launch_g_1719', 'wagebill0_1719'])
    for col in out.columns:
        if out[col].dtype == 'float64':
            out[col] = out[col].astype('float32')
    print(out[['total_spending0', 'dlog_spend', 'dlog_nupcs', 'd_ssnp', 'd_new_county', 'dlog_ces', 'dlog_va', 'dlog_real', 'breadth0', 'launch_z', 'rich_z', 'eta_iv', 'dlog_wagebill', 'bartik_wagebill', 'dlog_income_hat_hs']].describe().T[['count', 'mean', 'std', 'min', 'max']].round(3).to_string())
    print(f'{len(out):,} county-modules, {out.fips_n.nunique()} counties, {out.product_module_code.nunique()} modules; with QCEW {out.dlog_wagebill.notna().mean():.3f}, with Homescan shock {out.dlog_income_hat_hs.notna().mean():.3f}; spending share with Homescan shock {out.loc[out.dlog_income_hat_hs.notna(), "total_spending0"].sum() / out.total_spending0.sum():.3f}')
    out.to_stata(str(idir / 'what_moves' / 'variety_longdiff.dta'), write_index=False, version=118)
    print('saved variety_longdiff.dta')


if __name__ == '__main__':
    main()
