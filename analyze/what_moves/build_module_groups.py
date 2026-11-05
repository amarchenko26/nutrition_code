"""Module groups for the Jaravel-by-healthiness exercise (2026-11-05): claude_hi (hi_group 1 unhealthy 0-3, 2 middle 4-5, 3 healthy
6-10), preparation time (prep_group 1 open and eat = score 1, 2 little preparation = 2-3, 3 cooking = 4-9,
interim/rms_variety/prep_time_scores.parquet), the module's ultra-processed share (share of its UPCs with an ingredient list that
are NOVA 4, interim/health/upc_dictionary.parquet; upf_group 2 at or above the spending-weighted median module, spending from
interim/panel_dataset/innovation_reg_data_county.dta), and the module's income elasticity eta_iv
(interim/panel_dataset/module_income_elasticity.dta). Output: interim/what_moves/module_groups.parquet
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
import pyreadstat

OUT_DIR = idir / 'what_moves'


def module_groups():
    hi = pd.read_parquet(idir / 'rms_variety' / 'claude_hi_scores.parquet', columns=['product_module_code', 'product_module_descr', 'product_group_descr', 'claude_hi'])
    hi['product_module_code'] = pd.to_numeric(hi['product_module_code'], errors='coerce')
    hi['hi_group'] = pd.cut(hi['claude_hi'], [-1, 3, 5, 10], labels=[1, 2, 3]).astype(float)   # 1 unhealthy 0-3, 2 middle 4-5, 3 healthy 6-10
    pt = pd.read_parquet(idir / 'rms_variety' / 'prep_time_scores.parquet', columns=['product_module_code', 'prep_time'])
    pt['product_module_code'] = pd.to_numeric(pt['product_module_code'], errors='coerce')
    pt['prep_group'] = pd.cut(pt['prep_time'], [0, 1, 3, 9], labels=[1, 2, 3]).astype(float)      # 1 open-and-eat, 2 little prep, 3 cooking
    dic = pd.read_parquet(idir / 'health' / 'upc_dictionary.parquet', columns=['product_module_code', 'nova_own']).rename(columns={'nova_own': 'nova'})   # products with their own ingredient list
    dic = dic[dic['nova'].notna()]
    upf = dic.groupby('product_module_code').agg(upf_share=('nova', lambda s: (s == 4).mean()), n_nova=('nova', 'size')).reset_index()
    el, _ = pyreadstat.read_dta(str(idir / 'panel_dataset' / 'module_income_elasticity.dta'))
    el = el[['product_module_code', 'eta_iv']]
    g = hi.merge(pt, on='product_module_code', how='left').merge(upf, on='product_module_code', how='left').merge(el, on='product_module_code', how='left')
    return g


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    groups = module_groups()
    cty, _ = pyreadstat.read_dta(str(idir / 'panel_dataset' / 'innovation_reg_data_county.dta'), usecols=['module_code', 'spending'])
    msp = cty.groupby('module_code')['spending'].sum().rename('spend_tot').reset_index().rename(columns={'module_code': 'product_module_code'})
    gm = groups.merge(msp, on='product_module_code', how='inner').dropna(subset=['upf_share']).sort_values('upf_share')
    cum = gm['spend_tot'].cumsum() / gm['spend_tot'].sum()
    cut = gm.loc[cum >= 0.5, 'upf_share'].iloc[0]
    groups['upf_group'] = np.where(groups['upf_share'].isna(), np.nan, np.where(groups['upf_share'] >= cut, 2.0, 1.0))
    print(f'UPF cut (spending-weighted median module share of NOVA-4 UPCs): {cut:.3f}')
    groups.to_parquet(OUT_DIR / 'module_groups.parquet', index=False)
    print(groups[['hi_group', 'upf_group', 'prep_group']].apply(lambda c: c.value_counts().to_dict()).to_dict())


if __name__ == '__main__':
    main()
