"""Do rich and poor households live where healthy food is relatively dearer? (Anya 2026-11-10, 0_main.tex first section.)

Local prices: interim/what_moves/local_price_index.parquet (build_local_price_index.py), 3-digit-zip markets with 30 or
more purchasing households, one year at a time: the healthy (claude_hi 4-10) and unhealthy (0-3) price levels, log
points relative to the national price of the same UPCs, with national spending weights (level_*) and national calorie
weights (levelcal_*). A household-year gets the market of its home zip (panelists_all_years.parquet).
Dollars: the national price of the group, consumer-panel purchases pooled over 2004-2020 (interim/what_moves/
module_time_price.parquet, build_module_time_price.py), times the local level: per kg with the spending-weighted
level, per 1,000 kcal with the calorie-weighted level, and per 1,000 kcal with the group's national minutes of
preparation per 1,000 kcal valued at the household's income per head per 2,000 hours.
Income quartiles: within the year, projection-weighted, ties in the income brackets split at random (seed 0), the rule
of build_national_series.py.
Cells: projection-weighted means over household-years; ratios are healthy over unhealthy of the two means.
Output: tabs/what_moves/local_prices_by_income.tex. Prints the top-minus-bottom differences in the log relative
price with standard errors clustered on market.
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
MIN_HH = 30                       # purchasing households per market-year
HOURS = 2000                      # hours in a working year, per head


def national():
    m = pd.read_parquet(idir / 'what_moves' / 'module_time_price.parquet')
    m['grp'] = np.where(m['claude_hi'] >= 4, 'healthy', 'unhealthy')
    m['minutes'] = m['min_1000kcal'] * m['cals'] / 1000
    g = m.groupby('grp')[['spend_g', 'grams', 'cals', 'minutes']].sum()
    return pd.DataFrame({'usd_kg': 1000 * g['spend_g'] / g['grams'], 'usd_1000kcal': 1000 * g['spend_g'] / g['cals'],
                         'min_1000kcal': 1000 * g['minutes'] / g['cals']})


def households():
    p = pd.read_parquet(idir / 'panelists' / 'panelists_all_years.parquet',
                        columns=['household_code', 'panel_year', 'panelist_zip_code', 'n_heads'])
    h = pd.read_parquet(idir / 'panel_dataset' / 'panel_hh_year.parquet', columns=['household_code', 'panel_year', 'projection_factor', 'real_income'])
    d = h.merge(p, on=['household_code', 'panel_year'], how='inner').dropna(subset=['real_income', 'projection_factor', 'panelist_zip_code'])
    d['market'] = (pd.to_numeric(d['panelist_zip_code'], errors='coerce') // 100).astype('Int64')
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
    lp = pd.read_parquet(idir / 'what_moves' / 'local_price_index.parquet')
    lp = lp[lp['n_households'] >= MIN_HH].rename(columns={'year': 'panel_year'})
    d = households()
    n_all = len(d)
    d = d.merge(lp[['market', 'panel_year', 'level_healthy', 'level_unhealthy', 'levelcal_healthy', 'levelcal_unhealthy']].astype({'market': 'Int64'}),
                on=['market', 'panel_year'], how='inner').dropna(subset=['level_healthy', 'level_unhealthy', 'levelcal_healthy', 'levelcal_unhealthy'])
    print(f'household-years with a market price: {len(d):,} of {n_all:,}')
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
    # top minus bottom, log relative price, clustered on market
    tb = d[d['q'].isin([1, 4])].assign(top=lambda x: (x['q'] == 4).astype(int),
                                        rel=lambda x: x['level_healthy'] - x['level_unhealthy'], relcal=lambda x: x['levelcal_healthy'] - x['levelcal_unhealthy'])
    for y in ('rel', 'relcal'):
        f = smf.wls(f'{y} ~ top', data=tb, weights=tb['projection_factor']).fit(cov_type='cluster', cov_kwds={'groups': tb['market'].astype(int)})
        print(f'top minus bottom, {y} (log points x 100): {100 * f.params["top"]:.2f} ({100 * f.bse["top"]:.2f})')
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
