"""Who eats better: how much of the differences between households each characteristic accounts for (2026-11-05).

One row per household: projection-weighted means over its years, geographic non-movers, in the regression data. Groups:
income (ventiles of the household's mean income), education of the heads (4 bins), birth cohort (5-year), race, Hispanic
origin, Census region, children (ever under 18 in the panel), household size. For HEI: the largest gap between bins of the
characteristic, in points and in the standard deviation that analyze/labor_supply/12_ standardizes with. For each outcome: the share
of the variance of household means from that group of dummies alone, and from all the groups together. County and ZIP dummies are
reported alone, adjusted for the number of dummies. Output: tabs/what_moves/hei_cross_section.tex.
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

REGIONS = {9: 'Northeast', 23: 'Northeast', 25: 'Northeast', 33: 'Northeast', 44: 'Northeast', 50: 'Northeast', 34: 'Northeast', 36: 'Northeast', 42: 'Northeast',
           17: 'Midwest', 18: 'Midwest', 26: 'Midwest', 39: 'Midwest', 55: 'Midwest', 19: 'Midwest', 20: 'Midwest', 27: 'Midwest', 29: 'Midwest', 31: 'Midwest', 38: 'Midwest', 46: 'Midwest',
           4: 'West', 8: 'West', 16: 'West', 30: 'West', 32: 'West', 35: 'West', 49: 'West', 56: 'West', 2: 'West', 6: 'West', 15: 'West', 41: 'West', 53: 'West'}


def dummies(s):
    return pd.get_dummies(s, drop_first=True, dtype=float).values


def r2(y, X, w):
    Xc = np.column_stack([np.ones(len(y))] + X) if X else np.ones((len(y), 1))
    sw = np.sqrt(w)
    A = Xc * sw[:, None]
    b = np.linalg.lstsq(A.T @ A, A.T @ (y * sw), rcond=None)[0]
    res = y - Xc @ b
    m = np.average(y, weights=w)
    return 1 - np.sum(w * res ** 2) / np.sum(w * (y - m) ** 2)


def between_adj(y, g, w):
    d = pd.DataFrame({'y': y, 'g': g, 'w': w}).dropna()
    m = np.average(d.y, weights=d.w)
    gm = d.groupby('g').apply(lambda x: np.average(x.y, weights=x.w))
    res = d.y - d.g.map(gm)
    n, k = len(d), d.g.nunique()
    return 1 - (np.sum(d.w * res ** 2) / (n - k)) / (np.sum(d.w * (d.y - m) ** 2) / (n - 1))


def household_means():
    """One row per household with its bins, and the standard deviation of HEI across non-mover household-years (projection-weighted, as in 12_)."""
    cols = ['hhid', 'year', 'hei_usda', 'sugar_per_1000cal_usda', 'real_income', 'hh_avg_yrsofschool', 'avg_age_hh_head', 'race', 'hisp', 'fips_state_code', 'kids', 'hh_size',
            'fips', 'zip_code', 'projection_factor', 'movers_f']
    d, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'), usecols=cols)
    d = d[d.movers_f == 0].copy()
    ok = d.hei_usda.notna() & d.projection_factor.notna()
    mu = np.average(d.hei_usda[ok], weights=d.projection_factor[ok])
    sd = np.sqrt(np.average((d.hei_usda[ok] - mu) ** 2, weights=d.projection_factor[ok]))
    n = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet', columns=['household_code', 'panel_year', 'cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4', 'cals_all', 'ae_total', 'ae_complete'])
    n['upf_share'] = 100 * n.cals_nova4 / n[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
    n = n.rename(columns={'household_code': 'hhid', 'panel_year': 'year'})[['hhid', 'year', 'upf_share']]
    d['hhid'] = d.hhid.astype('int64'); d['year'] = d.year.astype(int)
    d = d.merge(n, on=['hhid', 'year'], how='left')
    d['birth'] = d.year - d.avg_age_hh_head
    d['haskids'] = (pd.to_numeric(d.kids, errors='coerce') != 9).astype(float)
    g = d.groupby('hhid')
    h = g.agg(hei=('hei_usda', 'mean'), sugar=('sugar_per_1000cal_usda', 'mean'), upf=('upf_share', 'mean'), inc=('real_income', 'mean'), educ=('hh_avg_yrsofschool', 'mean'), birth=('birth', 'mean'),
              race=('race', 'first'), hisp=('hisp', 'first'), state=('fips_state_code', 'first'), kids=('haskids', 'max'), size=('hh_size', 'mean'), fips=('fips', 'first'), zipc=('zip_code', 'first'),
              w=('projection_factor', 'mean')).dropna().reset_index()
    print(f'{len(h):,} households')
    h['inc_b'] = pd.qcut(h.inc.rank(method='first'), 20, labels=False)
    h['educ_b'] = pd.cut(h.educ, [0, 11.9, 12.9, 15.9, np.inf], labels=False)
    h['coh_b'] = (np.floor(h.birth / 5) * 5).clip(1925, 1985)
    h['size_b'] = pd.cut(h['size'], [0, 1.5, 2.5, 4.5, np.inf], labels=False)
    h['state'] = pd.to_numeric(h.state, errors='coerce')  # a handful of rows carry '.'
    h = h[h.state.notna()].copy()
    h['region'] = h.state.astype(int).map(REGIONS).fillna('South')
    return h, sd


def main():
    h, sd = household_means()
    GROUPS = {'Income': 'inc_b', 'Education': 'educ_b', 'Birth cohort': 'coh_b', 'Race': 'race', 'Hispanic origin': 'hisp', 'Census region': 'region', 'Children': 'kids', 'Household size': 'size_b'}
    names = list(GROUPS)
    X = {k: dummies(h[v]) for k, v in GROUPS.items()}
    w = h.w.values
    res = {}
    for out, col in [('HEI', 'hei'), ('Ultra-processed share', 'upf'), ('Sugar per 1,000 kcal', 'sugar')]:
        y = h[col].values
        res[out] = {'full': r2(y, [X[k] for k in names], w), 'alone': {k: r2(y, [X[k]], w) for k in names},
                    'county': between_adj(y, h.fips.values, w), 'zip': between_adj(y, h.zipc.values, w)}
        print(out, 'all groups R2', round(res[out]['full'], 3), 'county', round(res[out]['county'], 3), 'zip', round(res[out]['zip'], 3)); print({k: round(res[out]['alone'][k], 3) for k in names})
    gaps = {}
    for k, v in GROUPS.items():
        m = h.groupby(v).apply(lambda x: np.average(x['hei'], weights=x.w))
        gaps[k] = m.max() - m.min()
    print(f'standard deviation of HEI across non-mover household-years: {sd:.3f}')
    lines = ['\\begin{tabular}{lrrrrr}', '\\toprule', ' & \\multicolumn{2}{c}{Largest gap in HEI} & \\multicolumn{3}{c}{Share of the variance of household means} \\\\',
             '\\cmidrule(lr){2-3} \\cmidrule(lr){4-6}', 'Characteristic & Points & SD & HEI & Ultra-processed share & Sugar per 1,000 kcal \\\\', '\\midrule']
    for k in names:
        lines.append(f"{k} & {gaps[k]:.1f} & {gaps[k] / sd:.2f} & " + ' & '.join(f"{res[o]['alone'][k]:.3f}" for o in res) + ' \\\\')
    lines.append('\\midrule')
    lines.append('All of the above & & & ' + ' & '.join(f"{res[o]['full']:.3f}" for o in res) + ' \\\\')
    lines.append('County dummies, alone & & & ' + ' & '.join(f"{res[o]['county']:.3f}" for o in res) + ' \\\\')
    lines.append('ZIP dummies, alone & & & ' + ' & '.join(f"{res[o]['zip']:.3f}" for o in res) + ' \\\\')
    lines += ['\\bottomrule', '\\end{tabular}']
    (tdir / 'what_moves' / 'hei_cross_section.tex').write_text('\n'.join(lines) + '\n')
    print('saved hei_cross_section.tex')


if __name__ == '__main__':
    main()
