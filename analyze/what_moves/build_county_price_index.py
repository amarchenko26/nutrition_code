"""County food price indices by module group from the RMS price index, plus a
shift-share instrument, and the household outcome file for the local-price
regressions (Anya's "what moves nutrition" review, 2026-11-05).

Input: interim/rms_variety/price_index_module_fips_year.parquet (build/variety/
build_price_index.py on Oscar: module x county x year 2008-2020, year-on-year
Sato-Vartia CES log price change log_pi_ces over continuing UPCs, variety-adjusted
log_pi_va, n_cont continuing UPCs, total_spending).

Fixed basket: a county's 2008 module spending shares. The year-on-year log change
of group g in county c is the mean of the module log changes over the group's valid
modules (n_cont >= 10 continuing UPCs, |log change| <= 0.5), weighted by those 2008
shares, renormalised over the valid modules; a county-year needs 20+ valid modules
covering at least half of the group's 2008 spending. Changes are chained from the
county's first valid year; a later gap ends the chain (the county-group is missing
afterwards), because a skipped year would drop that year's price change from the
level. The file's own level_ces is not used, since it carries the extreme year-on-year
values forward. So rel_* measures the CUMULATIVE CHANGE in a county's relative price
since 2007, not a county's price level: it says nothing about which counties are
cheap, only about where prices moved.
Groups: healthy (claude_hi 4-10) vs unhealthy (0-3); mostly ultra-processed vs not
(module share of USDA-matched UPCs that are NOVA 4, median split); little
preparation (prep score 1) vs cooking (4-9). Relative price = first minus second.

Shift-share instrument z_rel_*: the same fixed 2008 shares (all modules of the
group, no county-year filter) times the module's national log price change with the
county itself left out (spending-weighted mean over the other counties' rows with
|log change| <= 0.5), chained from 2008 = 0 so only price changes after the share
year count. A county's z moves only because its 2008 mix differs from other counties'.

lspend_* = log ratio of the county's RMS spending on the two groups (all rows).

Outputs: interim/what_moves/county_price_index.parquet/.dta (county x year:
level_ces_<g>, level_va_<g>, z_level_<g>, n_mod_<g>, spend_<g>, rel_<pair>_ces,
rel_<pair>_va, z_rel_<pair>, lspend_<pair>, spend_total);
interim/what_moves/local_price_outcomes.dta (household x year: post-fatty-acid-fix
hei_usda from hei_usda_panel.parquet, since final/labor_supply_reg_data.dta still holds
the pre-fix HEI with its 3-point step in 2009; calories per adult equivalent per day,
winsorized p1/p99, ultra-processed share, produce share from nova_calorie_panel).
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
from scipy import sparse

RMS = idir / 'rms_variety'
WM = idir / 'what_moves'
OUT = WM / 'county_price_index'
MIN_CONT, MAX_ABS, MIN_MOD, MIN_COVER = 10, 0.5, 20, 0.5
GROUPS = {'g_hi': ('healthy', 'unhealthy'), 'g_upf': ('upf', 'nonupf'), 'g_prep': ('lowprep', 'cook')}
PAIRS = {'hi': ('healthy', 'unhealthy'), 'upf': ('upf', 'nonupf'), 'prep': ('lowprep', 'cook')}
YEARS = list(range(2008, 2021))


def module_groups():
    hi = pd.read_parquet(RMS / 'claude_hi_scores.parquet', columns=['product_module_code', 'claude_hi'])
    hi['product_module_code'] = pd.to_numeric(hi['product_module_code'], errors='coerce')
    hi['g_hi'] = np.where(hi['claude_hi'] >= 4, 'healthy', 'unhealthy')
    prep = pd.read_parquet(RMS / 'prep_time_scores.parquet', columns=['product_module_code', 'prep_time'])
    prep['product_module_code'] = pd.to_numeric(prep['product_module_code'], errors='coerce')
    prep['g_prep'] = np.select([prep['prep_time'] == 1, prep['prep_time'] >= 4], ['lowprep', 'cook'], default=None)
    d = pd.read_parquet(idir / 'health' / 'upc_dictionary.parquet', columns=['product_module_code', 'nova_own']).rename(columns={'nova_own': 'nova'}).dropna(subset=['nova'])
    upf = d.groupby('product_module_code')['nova'].apply(lambda s: (s == 4).mean()).rename('upf_share').reset_index()
    upf['g_upf'] = np.where(upf['upf_share'] >= upf['upf_share'].median(), 'upf', 'nonupf')
    return hi[['product_module_code', 'g_hi']].merge(prep[['product_module_code', 'g_prep']], how='outer') \
        .merge(upf[['product_module_code', 'g_upf']], how='outer')


def chain(dlog):
    """Series indexed (fips, year) on the full grid: cumulate from the first valid year, missing after a gap."""
    bad = dlog.isna()
    started = (~bad).groupby(level=0).cummax()
    broken = (bad & started).groupby(level=0).cummax()
    return dlog.where(started & ~broken).groupby(level=0).cumsum().where(started & ~broken)


def outcomes():
    n = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet',
                        columns=['household_code', 'panel_year', 'cals_all', 'cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4',
                                 'produce_cals_all', 'ae_total', 'ae_complete'])
    n = n[n['ae_complete'] == 1].copy()
    n['cals_ae_day'] = n['cals_all'] / n['ae_total'] / 365
    lo, hi = n['cals_ae_day'].quantile([0.01, 0.99])
    n['cals_ae_day'] = n['cals_ae_day'].clip(lo, hi)
    n['upf_share'] = 100 * n['cals_nova4'] / n[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
    n['produce_share'] = 100 * n['produce_cals_all'] / n['cals_all']
    h = pd.read_parquet(idir / 'panel_dataset' / 'hei_usda_panel.parquet', columns=['household_code', 'panel_year', 'hei_usda'])
    o = h.merge(n[['household_code', 'panel_year', 'cals_ae_day', 'upf_share', 'produce_share']], on=['household_code', 'panel_year'], how='outer')
    o = o.rename(columns={'household_code': 'hhid', 'panel_year': 'year'})
    o['hhid'] = o['hhid'].astype('int64'); o['year'] = o['year'].astype('int16')
    o.to_stata(WM / 'local_price_outcomes.dta', write_index=False, version=118)
    print(f'{len(o):,} household-years written to local_price_outcomes.dta')


def main():
    WM.mkdir(parents=True, exist_ok=True)
    outcomes()
    g = module_groups().rename(columns={'product_module_code': 'module'})
    d = pd.read_parquet(RMS / 'price_index_module_fips_year.parquet',
                        columns=['product_module_code', 'fips', 'year', 'log_pi_ces', 'log_pi_va', 'total_spending', 'n_cont'])
    d = d.rename(columns={'product_module_code': 'module'})
    d['fips'] = d['fips'].astype('int32'); d['module'] = d['module'].astype('int32'); d['year'] = d['year'].astype('int16')

    # ---- fixed county weights: 2008 module spending shares
    b = d.loc[d['year'] == 2008, ['fips', 'module', 'total_spending']].copy()
    b = b[b['total_spending'] > 0]
    b['w'] = b['total_spending'] / b.groupby('fips')['total_spending'].transform('sum')
    w_tab = b[['fips', 'module', 'w']]
    d = d.merge(w_tab, on=['fips', 'module'], how='left')

    # ---- national module change and the leave-one-county-out version
    ok = d['log_pi_ces'].abs() <= MAX_ABS
    d['_s'] = d['total_spending'].where(ok, 0.0)
    d['_sx'] = (d['total_spending'] * d['log_pi_ces']).where(ok, 0.0)
    nm = d.groupby(['module', 'year'])[['_s', '_sx']].sum().rename(columns={'_s': 'D', '_sx': 'N'}).reset_index()
    nm['nat'] = nm['N'] / nm['D']
    natfile = pd.read_parquet(RMS / 'price_index_module_year.parquet', columns=['product_module_code', 'year', 'pi_ces'])
    natfile['nf'] = np.log(natfile['pi_ces'])
    chk = nm.merge(natfile, left_on=['module', 'year'], right_on=['product_module_code', 'year'])
    print(f'national module change built here vs price_index_module_year: corr {chk["nat"].corr(chk["nf"]):.3f}, '
          f'mean abs diff {(chk["nat"] - chk["nf"]).abs().mean():.4f}, {len(chk):,} module-years')
    d = d.merge(nm, on=['module', 'year'], how='left')
    d['loo_adj'] = ((d['N'] - d['_sx']) / (d['D'] - d['_s']) - d['nat']).fillna(0.0)
    print(f'leave-one-county-out minus national, module-county-years: SD {d["loo_adj"].std():.5f}, p99 of abs {d["loo_adj"].abs().quantile(0.99):.5f}')
    d = d.drop(columns=['_s', '_sx', 'N', 'D', 'nat'])

    valid = (d['n_cont'] >= MIN_CONT) & (d['log_pi_ces'].abs() <= MAX_ABS) & d['w'].notna()
    print(f'{len(d):,} module-county-years; {valid.mean():.3f} pass the filters; {d.loc[valid, "total_spending"].sum() / d["total_spending"].sum():.3f} of spending; {d.fips.nunique()} counties')
    d = d.merge(g, on='module', how='left')

    fips_all = np.sort(d['fips'].unique())
    grid = pd.MultiIndex.from_product([fips_all, YEARS], names=['fips', 'year'])
    fpos = pd.Series(np.arange(len(fips_all)), index=fips_all)
    nat_wide = nm.pivot(index='module', columns='year', values='nat').reindex(columns=YEARS)

    out = pd.DataFrame(index=grid)
    out['spend_total'] = d.groupby(['fips', 'year'])['total_spending'].sum()
    for gcol, labels in GROUPS.items():
        for lab in labels:
            mods = set(g.loc[g[gcol] == lab, 'module'])
            wg = w_tab[w_tab['module'].isin(mods)].copy()
            wg['wn'] = wg['w'] / wg.groupby('fips')['w'].transform('sum')
            wn = wg[['fips', 'module', 'wn']]
            s = d[d['module'].isin(mods)].merge(wn, on=['fips', 'module'], how='left')
            out[f'spend_{lab}'] = s.groupby(['fips', 'year'])['total_spending'].sum()
            # actual: valid modules, renormalised
            v = s[(s['n_cont'] >= MIN_CONT) & (s['log_pi_ces'].abs() <= MAX_ABS) & s['wn'].notna()]
            a = pd.DataFrame({'W': v.groupby(['fips', 'year'])['wn'].sum(),
                              'n_mod': v.groupby(['fips', 'year']).size(),
                              'x_ces': (v['wn'] * v['log_pi_ces']).groupby([v['fips'], v['year']]).sum()}).reindex(grid)
            vv = v[v['log_pi_va'].abs() <= MAX_ABS]
            a['W_va'] = vv.groupby(['fips', 'year'])['wn'].sum()
            a['x_va'] = (vv['wn'] * vv['log_pi_va']).groupby([vv['fips'], vv['year']]).sum()
            enough = (a['n_mod'] >= MIN_MOD) & (a['W'] >= MIN_COVER)
            out[f'level_ces_{lab}'] = chain((a['x_ces'] / a['W']).where(enough))
            out[f'level_va_{lab}'] = chain((a['x_va'] / a['W_va']).where(enough))
            out[f'n_mod_{lab}'] = a['n_mod']
            out[f'cover_{lab}'] = a['W']
            # instrument: 2008 shares x national change, all modules, plus the county's own leave-out adjustment
            mods_nat = nat_wide.index.intersection(wn['module'].unique())
            mpos = pd.Series(np.arange(len(mods_nat)), index=mods_nat)
            wn2 = wn[wn['module'].isin(mods_nat)]
            W = sparse.csr_matrix((wn2['wn'].values, (fpos.loc[wn2['fips']].values, mpos.loc[wn2['module']].values)),
                                  shape=(len(fips_all), len(mods_nat)))
            NAT = nat_wide.loc[mods_nat].values
            Zn = W @ np.nan_to_num(NAT, nan=0.0)
            Wm = W @ (~np.isnan(NAT)).astype(float)
            z = pd.DataFrame(Zn / np.where(Wm > 0, Wm, np.nan), index=fips_all, columns=YEARS).stack(dropna=False)
            z.index.names = ['fips', 'year']
            adj = (s['wn'] * s['loo_adj']).groupby([s['fips'], s['year']]).sum()
            z = z.add(adj.reindex(z.index).fillna(0.0))
            z.loc[z.index.get_level_values('year') == 2008] = 0.0     # chain starts after the share year
            out[f'z_level_{lab}'] = z.groupby(level=0).cumsum()
            print(f'  {lab}: {len(mods)} modules; county-years with a level {out[f"level_ces_{lab}"].notna().sum():,}; '
                  f'median valid modules {a["n_mod"].median():.0f}, median 2008-spending coverage {a["W"].median():.2f}', flush=True)

    for pair, (a_, b_) in PAIRS.items():
        out[f'rel_{pair}_ces'] = out[f'level_ces_{a_}'] - out[f'level_ces_{b_}']
        out[f'rel_{pair}_va'] = out[f'level_va_{a_}'] - out[f'level_va_{b_}']
        out[f'z_rel_{pair}'] = out[f'z_level_{a_}'] - out[f'z_level_{b_}']
        out[f'lspend_{pair}'] = np.log(out[f'spend_{a_}'] / out[f'spend_{b_}'])
    out = out.reset_index()
    out = out[out['spend_total'].notna()]
    out['fips'] = out['fips'].astype('int32'); out['year'] = out['year'].astype('int16')
    out.to_parquet(OUT.with_suffix('.parquet'), index=False)
    out.to_stata(OUT.with_suffix('.dta'), write_index=False, version=118)

    # ---- diagnostics for the report
    print(f'saved {len(out):,} county-years')
    for p in PAIRS:
        k = out.dropna(subset=[f'rel_{p}_ces'])
        print(f'--- rel_{p}: {len(k):,} county-years, {k.fips.nunique()} counties')
        for y in [2010, 2015, 2020]:
            s = k[k.year == y]
            print(f'  {y}: mean {s[f"rel_{p}_ces"].mean():+.3f}; SD across counties {s[f"rel_{p}_ces"].std():.3f}; z SD {s[f"z_rel_{p}"].std():.3f}; corr(rel, z) {s[[f"rel_{p}_ces", f"z_rel_{p}"]].corr().iloc[0, 1]:.2f}; '
                  f'corr(CES, variety-adjusted) {s[[f"rel_{p}_ces", f"rel_{p}_va"]].corr().iloc[0, 1]:.2f}')
        x = k[f'rel_{p}_ces']
        res = x - k.groupby('fips')[f'rel_{p}_ces'].transform('mean') - k.groupby('year')[f'rel_{p}_ces'].transform('mean') + x.mean()
        print(f'  SD net of county and year means {res.std():.3f}')
    # national relative-price path at national 2008 shares (chained from 2007 = 0), for scale
    nb = d[d['year'] == 2008].groupby('module')['total_spending'].sum()
    natl = nm.copy(); natl['s08'] = natl['module'].map(nb)
    natl = natl.merge(g, on='module', how='left')
    rows = {}
    for pair, (a_, b_) in PAIRS.items():
        gcol = [k for k, v in GROUPS.items() if a_ in v][0]
        path = {}
        for lab in (a_, b_):
            t = natl[natl[gcol] == lab].dropna(subset=['nat', 's08'])
            path[lab] = (t['nat'] * t['s08']).groupby(t['year']).sum() / t['s08'].groupby(t['year']).sum()
        rows[pair] = (path[a_] - path[b_]).cumsum()
    print('national relative price at national 2008 shares, log points since 2007:')
    print((pd.DataFrame(rows) * 100).round(1).to_string())


if __name__ == '__main__':
    main()
