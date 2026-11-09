"""Expected health change with HEI 0.031 SD higher (2026-11-09, Anya: Table 4 of 0_main.tex is the health outcomes only; its panels on the
components, purchases and food of the 0.55 SD income gap were deleted the same day, with their code).
Output: tabs/what_moves/hei_scale.tex; everything else is printed.

1. Health: adults' BMI, BMI of 30 or more and type 2 diabetes on the household's mean HEI in standard deviations across household-years (the
   SD that analyze/labor_supply/12_ standardizes with), from the 2016-17 Health Care Survey (weight and height brackets), unweighted like
   Allcott et al. (2019, ValidateHealthMeasures.do), without and with controls and ZIP dummies. The table: the coefficient with controls times
   the main income IV, 0.031 SD per $10k (interim/labor_supply/iv_main_estimates.csv, written by 12_); standard error by the delta method,
   the two estimates taken as independent.
2. By age: the change in HEI within a household by age of the heads, net of the calendar year. Age and year rise together, so differences across ages
   are identified and the common level is not.
3. The linear HEI (lhei_usda) against the two numbers Allcott et al. publish for theirs: correlation with HEI 0.91, top minus bottom income quartile
   0.56 standard deviations conditional on household size, age and year, 0.67 BMI and 1.9 points of diabetes.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from build_hei_cross_section import household_means
from utils.stars import star_num

import numpy as np
import pandas as pd
import pyreadstat
import statsmodels.api as sm

OUTCOMES = [('bmi', 'BMI', ''), ('obese', 'BMI of 30 or more', '(percentage points)'), ('diab', 'Type 2 diabetes', '(percentage points)')]


def wm(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok])


def write_table(health, b_iv):
    """tabs/what_moves/hei_scale.tex, one column per outcome as in iv_suboutcomes.tex."""
    tex = [r'\begin{tabular}{lccc}', r'\toprule', r' & (1) & (2) & (3) \\',
           ' & ' + ' & '.join(lab for _, lab, _ in OUTCOMES) + r' \\', ' & ' + ' & '.join(u for _, _, u in OUTCOMES) + r' \\', r'\midrule',
           f'HEI {b_iv:.3f} SD higher & ' + ' & '.join(star_num(health[y]['b'], health[y]['se'], 3) for y, _, _ in OUTCOMES) + r' \\',
           ' & ' + ' & '.join(f"({health[y]['se']:.3f})" for y, _, _ in OUTCOMES) + r' \\', r'\midrule',
           'Mean of the outcome & ' + ' & '.join(f"{health[y]['mean']:.1f}" for y, _, _ in OUTCOMES) + r' \\',
           'Adults & ' + ' & '.join(f"{health[y]['n']:,}" for y, _, _ in OUTCOMES) + r' \\', r'\bottomrule', r'\end{tabular}']
    (tdir / 'what_moves' / 'hei_scale.tex').write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))


def main():
    h, sd = household_means()
    d = pd.read_parquet(idir / 'panel_dataset' / 'hei_usda_panel.parquet', columns=['household_code', 'panel_year', 'hei_usda', 'lhei_usda', 'projection_factor'])
    iv = pd.read_csv(idir / 'labor_supply' / 'iv_main_estimates.csv').set_index('outcome').loc['hei_usda']
    b_iv, se_iv = float(iv['b']), float(iv['se'])
    print(f'SD of HEI as in 12_: {sd:.3f}; main income IV {b_iv:.4f} ({se_iv:.4f}) SD per $10k')

    # ---- 1. health
    print('\n1. Health, adults 20+, 2016-17')
    mem = pd.read_parquet(idir / 'ailments' / 'ailments_members.parquet', columns=['household_code', 'panel_year', 'member', 'age', 'gender', 'bmi', 'diabetes_type2', 'obesity', 'answered'])
    mem = mem[mem['panel_year'].isin([2016, 2017]) & (mem['age'] >= 20) & mem['bmi'].notna() & mem['bmi'].between(12, 80)].copy()
    mem['obese'] = 100 * (mem['bmi'] >= 30).astype(float)
    mem['diab'] = 100 * mem['diabetes_type2'].fillna(0).astype(float)
    lh = d.groupby('household_code').agg(lhei=('lhei_usda', 'mean')).reset_index()
    hh = h.drop(columns=['w']).merge(lh, left_on='hhid', right_on='household_code', how='left')
    x = mem.merge(hh, on='household_code', how='inner')
    x['age5m'] = (np.floor(x['age'] / 5) * 5).astype(int)
    x['agesex'] = x['age5m'].astype(str) + '_' + x['gender'].astype(str)
    lsd = np.sqrt(np.average((d.lhei_usda - wm(d.lhei_usda, d.projection_factor)) ** 2, weights=d.projection_factor))
    x['hei_z'] = x['hei'] / sd       # household mean HEI in SD across household-years
    x['lhei_z'] = x['lhei'] / lsd
    print(f"  {len(x):,} adults, {x['household_code'].nunique():,} households; BMI mean {x.bmi.mean():.1f}, SD {x.bmi.std():.1f}; BMI 30+ {x.obese.mean():.1f}%; diabetes {x.diab.mean():.1f}%; household mean HEI SD {x.hei_z.std():.2f} household-year SDs")
    ctrl = pd.get_dummies(x[['agesex', 'inc_b', 'educ_b', 'race', 'hisp', 'size_b', 'kids', 'region']].astype(str), drop_first=True, dtype=float)

    def run(y, v, X=None, zipfe=False):
        Y, V = x[y].astype(float), x[v].astype(float)
        keep = Y.notna() & V.notna()
        Z = pd.concat([V.rename('v'), X], axis=1) if X is not None else V.rename('v').to_frame()
        Yk, Zk = Y[keep].copy(), Z[keep].copy()
        if zipfe:
            zz = x.loc[keep, 'zipc']
            Yk = Yk - Yk.groupby(zz).transform('mean')
            Zk = Zk - Zk.groupby(zz).transform('mean')
        else:
            Zk = sm.add_constant(Zk, has_constant='add')
        r = sm.OLS(Yk, Zk).fit(cov_type='cluster', cov_kwds={'groups': x.loc[keep, 'household_code'].values})
        return r.params['v'], r.bse['v'], int(keep.sum()), Y[keep].mean()
    health = {}
    for y, lab, _ in OUTCOMES:
        for name, X, z in [('no controls', None, False), ('age x sex, income, education, race, Hispanic, size, children, region', ctrl, False), ('same + ZIP', ctrl, True)]:
            b, se, n, mean = run(y, 'hei_z', X, z)
            bl, sel, _, _ = run(y, 'lhei_z', X, z)
            print(f"  {lab}, {name}: per SD of HEI {b:+.3f} ({se:.3f}); per {b_iv:.3f} SD {b * b_iv:+.4f}; per 0.56 SD of the linear HEI {bl * 0.56:+.3f} ({sel * 0.56:.3f})")
            if X is not None and not z:
                health[y] = {'b': b * b_iv, 'se': np.sqrt((b_iv * se) ** 2 + (b * se_iv) ** 2), 'n': n, 'mean': mean}
    write_table(health, b_iv)

    # ---- 2. age
    print('\n2. HEI within a household by age of the heads')
    r, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'), usecols=['hhid', 'year', 'hei_usda', 'avg_age_hh_head', 'projection_factor', 'movers_f'])
    r = r.sort_values(['hhid', 'year'])
    g = r.groupby('hhid')
    r['dy'] = np.where(r['year'] - g['year'].shift(1) == 1, r['hei_usda'] - g['hei_usda'].shift(1), np.nan)
    r['age_b'] = pd.cut(r['avg_age_hh_head'], [0, 35, 45, 55, 65, 75, 200], right=False, labels=['<35', '35-44', '45-54', '55-64', '65-74', '75+'])
    p = r.dropna(subset=['dy', 'age_b', 'projection_factor'])
    X = pd.concat([pd.get_dummies(p['age_b'], dtype=float), pd.get_dummies(p['year'].astype(int), prefix='y', dtype=float).iloc[:, 1:]], axis=1)
    fit = sm.WLS(p['dy'], X, weights=p['projection_factor']).fit(cov_type='cluster', cov_kwds={'groups': p['hhid'].values})
    ages = ['<35', '35-44', '45-54', '55-64', '65-74', '75+']
    ref = fit.params['45-54']
    print('  yearly change by age, net of the calendar year, relative to 45-54:', {a_: round(10 * (fit.params[a_] - ref), 2) for a_ in ages}, '(points per 10 years)')
    V = fit.cov_params()
    for a_ in ['<35', '35-44', '55-64', '65-74', '75+']:
        se = np.sqrt(V.loc[a_, a_] + V.loc['45-54', '45-54'] - 2 * V.loc[a_, '45-54'])
        print(f"    {a_}: {10 * (fit.params[a_] - ref):+.2f} ({10 * se:.2f})")
    print(f"  mean yearly change {wm(p['dy'], p['projection_factor']) * 10:+.2f} points per 10 years; national mean per 10 years over 2004-2020: {10 * (wm(r[r.year == 2020].hei_usda, r[r.year == 2020].projection_factor) - wm(r[r.year == 2004].hei_usda, r[r.year == 2004].projection_factor)) / 16:+.2f}")
    rr = r.dropna(subset=['age_b', 'hei_usda', 'projection_factor']).copy()
    rr['wy'] = rr['hei_usda'] * rr['projection_factor']
    gy = rr.groupby('year')
    rr['yr_m'] = rr['hei_usda'] - gy['wy'].transform('sum') / gy['projection_factor'].transform('sum')
    print('  mean HEI by age, net of the year, across households:', {str(k): round(np.average(v['yr_m'], weights=v['projection_factor']), 2) for k, v in rr.groupby('age_b', observed=True)})

    # ---- 3. the linear HEI
    print('\n3. Linear HEI against Allcott et al.')
    pan = pd.read_parquet(idir / 'panel_dataset' / 'panel_hh_year.parquet', columns=['household_code', 'panel_year', 'real_income', 'household_size', 'avg_age_hh_head'])
    z = d.merge(pan, on=['household_code', 'panel_year'], how='inner').dropna(subset=['real_income', 'household_size', 'avg_age_hh_head'])
    for v, lab in [('lhei_usda', 'linear HEI'), ('hei_usda', 'HEI')]:
        mu_ = wm(z[v], z['projection_factor'])
        s_ = np.sqrt(np.average((z[v] - mu_) ** 2, weights=z['projection_factor']))
        z['zv'] = (z[v] - mu_) / s_
        z['q'] = z.groupby('panel_year')['real_income'].transform(lambda s: pd.qcut(s.rank(method='first'), 4, labels=False))
        XX = pd.concat([pd.get_dummies(z['q'].astype(int), prefix='q', dtype=float).iloc[:, 1:], pd.get_dummies(z['panel_year'].astype(int), prefix='y', dtype=float), pd.get_dummies(z['household_size'].clip(upper=9).astype(int), prefix='s', dtype=float).iloc[:, 1:], z[['avg_age_hh_head']].astype(float)], axis=1)
        ff = sm.WLS(z['zv'], XX, weights=z['projection_factor']).fit(cov_type='cluster', cov_kwds={'groups': z['household_code'].values})
        print(f"  {lab}: SD {s_:.2f}; correlation with HEI {np.corrcoef(z[v], z['hei_usda'])[0, 1]:.3f}; top minus bottom income quartile {ff.params['q_3']:.3f} SD ({ff.bse['q_3']:.3f})")


if __name__ == '__main__':
    main()
