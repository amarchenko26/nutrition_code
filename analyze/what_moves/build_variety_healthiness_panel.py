"""County x module x year panel for the Jaravel-by-healthiness exercise (2026-11-05, Anya's ask: do demand
shocks raise variety and lower prices more in ultra-processed / little-preparation / unhealthy modules?).

Inputs: interim/panel_dataset/innovation_reg_data_county.dta (module x county x year 2008-2020 from
build/innovation/build_innovation_reg_data.py: ssnp = share of spending on nationally new UPCs, ssep, spending,
n_upcs (in the county), n_new_upcs and share_new_upcs (new to the county), d_spending = real expenditure growth,
iv_income = the county's leave-one-out predicted income growth d_log_income_hat), interim/rms_variety/
price_index_module_fips_year.parquet (log_pi_ces, log_pi_va year-on-year, level_ces, level_va; kept when
n_cont >= 10 and |log_pi_ces| <= 0.5), interim/panel_dataset/county_income_shocks.parquet (d_log_income, the
realized county income growth), interim/panel_dataset/module_income_elasticity.dta (eta_iv, the module's income
elasticity from build_module_income_elasticity.py), and three module groupings: claude_hi (0-3 / 4-5 / 6-10),
the module's ultra-processed share (share of its UPCs with an ingredient list that are NOVA 4, from
interim/health/upc_dictionary.parquet; halves at the spending-weighted median), preparation time
(interim/rms_variety/prep_time_scores.parquet: 1 / 2-3 / 4-9).
Outputs: interim/what_moves/variety_county_panel.dta, interim/what_moves/module_groups.parquet
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

OUT_DIR = idir / 'what_moves'


def module_groups():
    hi = pd.read_parquet(idir / 'rms_variety' / 'claude_hi_scores.parquet', columns=['product_module_code', 'product_module_descr', 'product_group_descr', 'claude_hi'])
    hi['product_module_code'] = pd.to_numeric(hi['product_module_code'], errors='coerce')
    hi['hi_group'] = pd.cut(hi['claude_hi'], [-1, 3, 5, 10], labels=[1, 2, 3]).astype(float)   # 1 unhealthy 0-3, 2 middle 4-5, 3 healthy 6-10
    pt = pd.read_parquet(idir / 'rms_variety' / 'prep_time_scores.parquet', columns=['product_module_code', 'prep_time'])
    pt['product_module_code'] = pd.to_numeric(pt['product_module_code'], errors='coerce')
    pt['prep_group'] = pd.cut(pt['prep_time'], [0, 1, 3, 9], labels=[1, 2, 3]).astype(float)      # 1 open-and-eat, 2 little prep, 3 cooking
    dic = pd.read_parquet(idir / 'health' / 'upc_dictionary.parquet', columns=['product_module_code', 'nova'])
    dic = dic[dic['nova'].notna()]
    upf = dic.groupby('product_module_code').agg(upf_share=('nova', lambda s: (s == 4).mean()), n_nova=('nova', 'size')).reset_index()
    el, _ = pyreadstat.read_dta(str(idir / 'panel_dataset' / 'module_income_elasticity.dta'))
    el = el[['product_module_code', 'eta_iv']]
    g = hi.merge(pt, on='product_module_code', how='left').merge(upf, on='product_module_code', how='left').merge(el, on='product_module_code', how='left')
    return g


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    groups = module_groups()
    cty, _ = pyreadstat.read_dta(str(idir / 'panel_dataset' / 'innovation_reg_data_county.dta'))
    cty = cty.rename(columns={'module_code': 'product_module_code'})
    cty['fips'] = cty['fips'].astype(str).str.zfill(5)
    cty['year'] = cty['year'].astype(int)
    for c in ['ssnp', 'ssep', 'spending', 'n_upcs', 'n_new_upcs', 'share_new_upcs', 'd_spending', 'iv_income']:
        cty[c] = cty[c].astype('float32')
    print(f'county panel {len(cty):,} rows, years {cty.year.min()}-{cty.year.max()}, modules {cty.product_module_code.nunique()}, counties {cty.fips.nunique()}')
    # spending-weighted median of the module UPF share, over the modules in the panel
    msp = cty.groupby('product_module_code')['spending'].sum().rename('spend_tot').reset_index()
    gm = groups.merge(msp, on='product_module_code', how='inner').dropna(subset=['upf_share'])
    gm = gm.sort_values('upf_share'); cum = gm['spend_tot'].cumsum() / gm['spend_tot'].sum()
    cut = gm.loc[cum >= 0.5, 'upf_share'].iloc[0]
    groups['upf_group'] = np.where(groups['upf_share'].isna(), np.nan, np.where(groups['upf_share'] >= cut, 2.0, 1.0))  # 2 mostly ultra-processed
    print(f'UPF cut (spending-weighted median module share of NOVA-4 UPCs): {cut:.3f}')
    groups.to_parquet(OUT_DIR / 'module_groups.parquet', index=False)
    # price index
    pi = pd.read_parquet(idir / 'rms_variety' / 'price_index_module_fips_year.parquet',
                         columns=['product_module_code', 'fips', 'year', 'log_pi_ces', 'log_pi_va', 'level_ces', 'level_va', 'n_cont'])
    pi = pi[(pi['n_cont'] >= 10) & (pi['log_pi_ces'].abs() <= 0.5) & (pi['log_pi_va'].abs() <= 0.5)]
    pi['fips'] = pi['fips'].astype(str).str.zfill(5); pi['year'] = pi['year'].astype(int)
    for c in ['log_pi_ces', 'log_pi_va', 'level_ces', 'level_va']:
        pi[c] = pi[c].astype('float32')
    pi['log_level_va'] = np.log(pi['level_va'].clip(lower=0.05)).astype('float32')
    pi['log_level_ces'] = np.log(pi['level_ces'].clip(lower=0.05)).astype('float32')
    pi = pi.drop(columns=['level_ces', 'level_va', 'n_cont'])
    print(f'price index kept {len(pi):,} module-county-years')
    d = cty.merge(pi, on=['product_module_code', 'fips', 'year'], how='left')
    del cty, pi; gc.collect()
    sh = pd.read_parquet(idir / 'panel_dataset' / 'county_income_shocks.parquet', columns=['fips', 'year', 'd_log_income'])
    sh['fips'] = sh['fips'].astype(str).str.zfill(5); sh['year'] = sh['year'].astype(int)
    d = d.merge(sh, on=['fips', 'year'], how='left')
    d = d.merge(groups[['product_module_code', 'hi_group', 'upf_group', 'prep_group', 'eta_iv', 'claude_hi', 'upf_share']], on='product_module_code', how='left')
    d['log_n_upcs'] = np.log(d['n_upcs'].clip(lower=1)).astype('float32')
    d['log_spending'] = np.log(d['spending'].clip(lower=1)).astype('float32')
    d = d.sort_values(['product_module_code', 'fips', 'year'])
    g = d.groupby(['product_module_code', 'fips'])
    d['d_log_n_upcs'] = (d['log_n_upcs'] - g['log_n_upcs'].shift(1)).where(g['year'].diff() == 1).astype('float32')
    d['fips_n'] = d['fips'].astype(int)
    d['shock_eta'] = (d['iv_income'] * d['eta_iv']).astype('float32')
    print(d[['ssnp', 'share_new_upcs', 'd_log_n_upcs', 'log_pi_va', 'log_pi_ces', 'd_spending', 'iv_income', 'd_log_income', 'shock_eta']].describe().T[['count', 'mean', 'std']].round(4).to_string())
    print('modules by group: hi', d.drop_duplicates('product_module_code')['hi_group'].value_counts().to_dict(),
          'upf', d.drop_duplicates('product_module_code')['upf_group'].value_counts().to_dict(),
          'prep', d.drop_duplicates('product_module_code')['prep_group'].value_counts().to_dict())
    keep = ['product_module_code', 'fips_n', 'year', 'spending', 'log_spending', 'ssnp', 'ssep', 'n_upcs', 'log_n_upcs', 'd_log_n_upcs', 'share_new_upcs',
            'd_spending', 'iv_income', 'd_log_income', 'shock_eta', 'eta_iv', 'log_pi_ces', 'log_pi_va', 'log_level_ces', 'log_level_va',
            'hi_group', 'upf_group', 'prep_group']
    out = d[keep].copy()
    del d; gc.collect()
    out.to_stata(str(OUT_DIR / 'variety_county_panel.dta'), write_index=False, version=118)
    print(f'saved {OUT_DIR / "variety_county_panel.dta"} ({len(out):,} rows)')


if __name__ == '__main__':
    main()
