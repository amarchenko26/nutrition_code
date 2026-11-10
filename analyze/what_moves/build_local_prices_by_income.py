"""Do rich and poor households live where healthy food is relatively dearer? (Anya 2026-11-10, 0_main.tex first section.)

Local prices from Nielsen RMS store data (Anya 2026-11-10: "this needs to be RMS not HMS"): interim/what_moves/
rms_county_price_index.parquet (build_local_price_index_rms.py, run on Oscar), one county-year per row: the healthy
(claude_hi 4-10) and unhealthy (0-3) price levels, log points relative to the national price of the same UPCs, with
national spending weights (level_*) and national calorie weights (levelcal_*). Counties with MIN_UPCS or more UPCs in
each group. A household-year gets the county of its home (panelists_all_years.parquet), 2006-2020.
Dollars: the national RMS price of the group (rms_national_groups.parquet, 2006-2020 pooled, 2013 dollars with the CPI-U)
times the local level: per kg with the spending-weighted level, per 1,000 kcal with the calorie-weighted level, and per
1,000 kcal plus the group's national minutes of preparation per 1,000 kcal (interim/what_moves/module_time_price.parquet,
build_module_time_price.py) valued at the household's income per head per 2,000 hours.
Income quartiles: within the year, projection-weighted, ties in the income brackets split at random (seed 0), the rule
of build_national_series.py.
Cells: projection-weighted means over household-years; ratios are healthy over unhealthy of the two means.
Output: tabs/what_moves/local_prices_by_income.tex. Prints the top-minus-bottom differences in the log levels with
standard errors clustered on county.
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
import statsmodels.formula.api as smf

OUT = tdir / 'what_moves' / 'local_prices_by_income.tex'
MIN_UPCS = 500                    # UPCs per group in the county-year
HOURS = 2000                      # hours in a working year, per head
YEARS = range(2006, 2021)         # RMS years
CPI = {2006: 201.6, 2007: 207.3, 2008: 215.3, 2009: 214.5, 2010: 218.1, 2011: 224.9, 2012: 229.6, 2013: 233.0, 2014: 236.7,
       2015: 237.0, 2016: 240.0, 2017: 245.1, 2018: 251.1, 2019: 255.7, 2020: 258.8}       # CPI-U, as in analyze_variety_price_index.py


def national():
    n = pd.read_parquet(idir / 'what_moves' / 'rms_national_groups.parquet')
    n['to_2013'] = CPI[2013] / n['year'].map(CPI)
    g = n.assign(sg=n['spend_g'] * n['to_2013'], sk=n['spend_kcal'] * n['to_2013']).groupby('healthy')[['sg', 'grams', 'sk', 'kcal']].sum()
    g.index = g.index.map({1: 'healthy', 0: 'unhealthy'})
    m = pd.read_parquet(idir / 'what_moves' / 'module_time_price.parquet')
    m['grp'] = np.where(m['claude_hi'] >= 4, 'healthy', 'unhealthy')
    t = m.assign(minutes=m['min_1000kcal'] * m['cals'] / 1000).groupby('grp')[['minutes', 'cals']].sum()
    return pd.DataFrame({'usd_kg': 1000 * g['sg'] / g['grams'], 'usd_1000kcal': 1000 * g['sk'] / g['kcal'],
                         'min_1000kcal': 1000 * t['minutes'] / t['cals']})


def households():
    p = pd.read_parquet(idir / 'panelists' / 'panelists_all_years.parquet',
                        columns=['household_code', 'panel_year', 'fips_state_code', 'fips_county_code', 'n_heads'])
    h = pd.read_parquet(idir / 'panel_dataset' / 'panel_hh_year.parquet', columns=['household_code', 'panel_year', 'projection_factor', 'real_income'])
    d = h.merge(p, on=['household_code', 'panel_year'], how='inner').dropna(subset=['real_income', 'projection_factor', 'fips_state_code', 'fips_county_code'])
    d = d[d['panel_year'].isin(YEARS)]
    d['fips'] = (1000 * pd.to_numeric(d['fips_state_code']) + pd.to_numeric(d['fips_county_code'])).astype(int)
    rng = np.random.default_rng(0)
    d = d.assign(tie=rng.random(len(d))).sort_values(['panel_year', 'real_income', 'tie'])
    d['cum'] = d.groupby('panel_year')['projection_factor'].cumsum() / d.groupby('panel_year')['projection_factor'].transform('sum')
    d['q'] = np.minimum((d['cum'] * 4).astype(int), 3) + 1
    d['vot'] = 1000 * d['real_income'] / (HOURS * d['n_heads'].clip(lower=1))     # real_income is in $1,000s of 2013
    return d


def wmean(x, col):
    return np.average(x[col], weights=x['projection_factor'])


def main():
    nat = national()
    print(nat.round(3).to_string())
    lp = pd.read_parquet(idir / 'what_moves' / 'rms_county_price_index.parquet')
    lp = lp[(lp['n_upcs_healthy'] >= MIN_UPCS) & (lp['n_upcs_unhealthy'] >= MIN_UPCS)].rename(columns={'year': 'panel_year'})
    d = households()
    n_all = len(d)
    d = d.merge(lp[['fips', 'panel_year', 'level_healthy', 'level_unhealthy', 'levelcal_healthy', 'levelcal_unhealthy']],
                on=['fips', 'panel_year'], how='inner').dropna(subset=['level_healthy', 'level_unhealthy', 'levelcal_healthy', 'levelcal_unhealthy'])
    print(f'household-years 2006-2020 with a county price: {len(d):,} of {n_all:,}')
    for g in ('healthy', 'unhealthy'):
        d[f'kg_{g}'] = nat.loc[g, 'usd_kg'] * np.exp(d[f'level_{g}'])
        d[f'kcal_{g}'] = nat.loc[g, 'usd_1000kcal'] * np.exp(d[f'levelcal_{g}'])
        d[f'full_{g}'] = d[f'kcal_{g}'] + d['vot'] / 60 * nat.loc[g, 'min_1000kcal']
    rows = {}
    for q, lab in [(4, 'Top income quartile'), (1, 'Bottom income quartile')]:
        x = d[d['q'] == q]
        r = {}
        for c in ('kg', 'kcal', 'full'):
            h, u = wmean(x, f'{c}_healthy'), wmean(x, f'{c}_unhealthy')
            r[c] = (h, u, h / u)
        r['vot'] = wmean(x, 'vot')
        r['n'] = len(x)
        rows[lab] = r
        print(lab, {k: (np.round(v, 3) if isinstance(v, tuple) else round(v, 2)) for k, v in r.items()})
    # top minus bottom in log points, clustered on county
    tb = d[d['q'].isin([1, 4])].assign(top=lambda x: (x['q'] == 4).astype(int),
                                        rel=lambda x: x['level_healthy'] - x['level_unhealthy'], relcal=lambda x: x['levelcal_healthy'] - x['levelcal_unhealthy'])
    for y in ('level_healthy', 'level_unhealthy', 'rel', 'levelcal_healthy', 'levelcal_unhealthy', 'relcal'):
        f = smf.wls(f'{y} ~ top', data=tb, weights=tb['projection_factor']).fit(cov_type='cluster', cov_kwds={'groups': tb['fips']})
        print(f'top minus bottom, {y} (log points): {f.params["top"]:.4f} ({f.bse["top"]:.4f})')
    lines = ['\\begin{tabular}{lccccccccc}', '\\toprule',
             ' & \\multicolumn{3}{c}{Per kilogram} & \\multicolumn{3}{c}{Per 1,000 calories} & \\multicolumn{3}{c}{Per 1,000 calories, with preparation time} \\\\',
             '\\cmidrule(lr){2-4} \\cmidrule(lr){5-7} \\cmidrule(lr){8-10}',
             ' & Healthy & Unhealthy & Ratio & Healthy & Unhealthy & Ratio & Healthy & Unhealthy & Ratio \\\\', '\\midrule']
    for lab, r in rows.items():
        cells = []
        for c in ('kg', 'kcal', 'full'):
            h, u, ratio = r[c]
            cells += [f'\\${h:.2f}', f'\\${u:.2f}', f'{ratio:.3f}']
        lines.append(f'{lab} & ' + ' & '.join(cells) + ' \\\\')
    lines += ['\\midrule',
              'Value of an hour & \\multicolumn{9}{l}{' + ', '.join(f'{lab.split()[0].lower()} quartile \\${r["vot"]:.2f}' for lab, r in rows.items()) + '} \\\\',
              'Household-years & \\multicolumn{9}{l}{' + ', '.join(f'{lab.split()[0].lower()} quartile {r["n"]:,}' for lab, r in rows.items()) + '} \\\\',
              '\\bottomrule', '\\end{tabular}']
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text('\n'.join(lines) + '\n')
    print(f'saved {OUT}')


if __name__ == '__main__':
    main()
