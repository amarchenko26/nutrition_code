"""County food price indices by module group from the RMS price index, plus a
shift-share instrument (Anya's "what moves nutrition" review, 2026-11-05).

Input: interim/rms_variety/price_index_module_fips_year.parquet (build/variety/
build_price_index.py on Oscar: module x county x year 2008-2020, year-on-year
Sato-Vartia CES log price change log_pi_ces over continuing UPCs, variety-
adjusted log_pi_va, n_cont continuing UPCs, total_spending) and
price_index_module_year.parquet (the national module x year version).

A county's group price index is a chained Laspeyres-type index: the year-on-year
log change of group g in county c = the mean of the module log changes over the
group's modules passing the filters (n_cont >= 10 continuing UPCs and
|log_pi_ces| <= 0.5), weighted by the county's FIXED module spending shares
(the module's mean share of the county's spending over 2008-2020), then
cumulated from 2007 = 0. Cumulating the filtered changes (rather than using the
file's level_ces) keeps the extreme year-on-year values out of later levels.
Groups: healthy (claude_hi 4-10) vs unhealthy (0-3); mostly ultra-processed vs
not (module share of USDA-matched UPCs that are NOVA 4, median split); little
preparation (prep score 1) vs cooking (4-9). Relative price = healthy minus
unhealthy log level, etc. A county-year group level needs 20+ valid modules.

Shift-share instrument: the same construction with the NATIONAL module log
change in place of the county's own, i.e. the county's exposure (fixed module
shares) to national module price trends: z_level_<group>, z_rel_<pair>.

Output: interim/what_moves/county_price_index.parquet and .dta, one row per
county x year 2008-2020: level_ces_<g>, level_va_<g>, rel_<pair>_ces, rel_<pair>_va,
z_rel_<pair>, n_mod_<g>, spend_total.
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

RMS = idir / 'rms_variety'
OUT = idir / 'what_moves' / 'county_price_index'
MIN_CONT, MAX_ABS, MIN_MOD = 10, 0.5, 20
GROUPS = {'g_hi': ('healthy', 'unhealthy'), 'g_upf': ('upf', 'nonupf'), 'g_prep': ('lowprep', 'cook')}


def module_groups():
    hi = pd.read_parquet(RMS / 'claude_hi_scores.parquet', columns=['product_module_code', 'claude_hi'])
    hi['product_module_code'] = pd.to_numeric(hi['product_module_code'], errors='coerce')
    hi['g_hi'] = np.where(hi['claude_hi'] >= 4, 'healthy', 'unhealthy')
    prep = pd.read_parquet(RMS / 'prep_time_scores.parquet', columns=['product_module_code', 'prep_time'])
    prep['product_module_code'] = pd.to_numeric(prep['product_module_code'], errors='coerce')
    prep['g_prep'] = np.select([prep['prep_time'] == 1, prep['prep_time'] >= 4], ['lowprep', 'cook'], default=None)
    d = pd.read_parquet(idir / 'health' / 'upc_dictionary.parquet', columns=['product_module_code', 'nova']).dropna(subset=['nova'])
    upf = d.groupby('product_module_code')['nova'].apply(lambda s: (s == 4).mean()).rename('upf_share').reset_index()
    upf['g_upf'] = np.where(upf['upf_share'] >= upf['upf_share'].median(), 'upf', 'nonupf')
    return hi[['product_module_code', 'g_hi']].merge(prep[['product_module_code', 'g_prep']], how='outer') \
        .merge(upf[['product_module_code', 'g_upf']], how='outer')


def main():
    g = module_groups()
    d = pd.read_parquet(RMS / 'price_index_module_fips_year.parquet',
                        columns=['product_module_code', 'fips', 'year', 'log_pi_ces', 'log_pi_va', 'total_spending', 'n_cont'])
    d['fips'] = d['fips'].astype(int)
    d = d.merge(g, on='product_module_code', how='left')
    # fixed county module weights: the module's mean share of county spending over all years
    tot = d.groupby(['fips', 'year'])['total_spending'].transform('sum')
    d['share'] = d['total_spending'] / tot
    w = d.groupby(['fips', 'product_module_code'])['share'].mean().rename('w').reset_index()
    d = d.merge(w, on=['fips', 'product_module_code'])
    nat = pd.read_parquet(RMS / 'price_index_module_year.parquet', columns=['product_module_code', 'year', 'pi_ces', 'pi_va'])
    nat['nat_dlog_ces'] = np.log(nat['pi_ces']); nat['nat_dlog_va'] = np.log(nat['pi_va'])
    d = d.merge(nat[['product_module_code', 'year', 'nat_dlog_ces', 'nat_dlog_va']], on=['product_module_code', 'year'], how='left')
    valid = (d['n_cont'] >= MIN_CONT) & (d['log_pi_ces'].abs() <= MAX_ABS) & d['log_pi_va'].notna()
    print(f'{len(d):,} module-county-years, {valid.mean():.3f} pass the filters; {d.fips.nunique()} counties')
    d = d[valid].copy()
    spend = d.groupby(['fips', 'year'])['total_spending'].sum().rename('spend_total')
    pieces = [spend]
    for gcol, labels in GROUPS.items():
        for lab in labels:
            s = d[d[gcol] == lab]
            a = s.groupby(['fips', 'year']).apply(lambda x: pd.Series({
                'dlog_ces': np.average(x['log_pi_ces'], weights=x['w']), 'dlog_va': np.average(x['log_pi_va'], weights=x['w']),
                'z_dlog_ces': np.average(x['nat_dlog_ces'], weights=x['w']), 'n_mod': len(x)}))
            a.loc[a['n_mod'] < MIN_MOD, ['dlog_ces', 'dlog_va', 'z_dlog_ces']] = np.nan
            # cumulate within county from 2007 = 0; a missing year breaks the chain for that county-group
            a = a.sort_index()
            for c in ['dlog_ces', 'dlog_va', 'z_dlog_ces']:
                a[c.replace('dlog', 'level')] = a.groupby(level=0)[c].cumsum()
            a = a.rename(columns={'level_ces': f'level_ces_{lab}', 'level_va': f'level_va_{lab}', 'z_level_ces': f'z_level_{lab}', 'n_mod': f'n_mod_{lab}'})
            pieces.append(a[[f'level_ces_{lab}', f'level_va_{lab}', f'z_level_{lab}', f'n_mod_{lab}']])
    out = pd.concat(pieces, axis=1).reset_index()
    for pair, (a_, b_) in {'hi': ('healthy', 'unhealthy'), 'upf': ('upf', 'nonupf'), 'prep': ('lowprep', 'cook')}.items():
        out[f'rel_{pair}_ces'] = out[f'level_ces_{a_}'] - out[f'level_ces_{b_}']
        out[f'rel_{pair}_va'] = out[f'level_va_{a_}'] - out[f'level_va_{b_}']
        out[f'z_rel_{pair}'] = out[f'z_level_{a_}'] - out[f'z_level_{b_}']
    out.to_parquet(OUT.with_suffix('.parquet'), index=False)
    out.to_stata(OUT.with_suffix('.dta'), write_index=False, version=118)
    ok = out.dropna(subset=['rel_hi_ces'])
    print(f'saved {len(out):,} county-years ({len(ok):,} with the healthy-unhealthy relative price); counties {ok.fips.nunique()}')
    for y in [2010, 2015, 2020]:
        s = ok[ok.year == y]
        print(f'{y}: rel_hi_ces SD across counties {s.rel_hi_ces.std():.3f}, mean {s.rel_hi_ces.mean():+.3f}; z_rel_hi SD {s.z_rel_hi.std():.3f}; corr(rel, z) {s[["rel_hi_ces", "z_rel_hi"]].corr().iloc[0, 1]:.2f}')
    m = ok.groupby('fips')['rel_hi_ces'].transform('mean')
    print(f'within-county SD of rel_hi_ces over time {(ok.rel_hi_ces - m).std():.3f}')


if __name__ == '__main__':
    main()
