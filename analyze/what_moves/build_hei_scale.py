"""How large is a gap of 5.4 HEI points? (2026-11-06, Anya's comments on the who-eats-better table and the cohort figure.)
Output: tabs/what_moves/hei_scale.tex (parts 1 to 3); everything is printed.

1. What the gap between the lowest- and highest-scoring income bins is made of, component by component, and cups of fruit and calories by income bin.
2. In food: the change in HEI when each household's groceries gain an apple, a whole-grain slice in place of a white one, or a can of soda
   per adult equivalent per day, recomputing every component with the rules of build_hei_panel.py. Densities are read back from the component
   scores, so a component at its cap stays at the standard before the change. A medium apple is 182 g and 95 kcal, 150 g per cup equivalent as in
   the module crosswalk; a can of soda is 140 kcal and 39 g of added sugar.
3. In health: adults' BMI, BMI of 30 or more and type 2 diabetes on the household's mean HEI, from the 2016-17 Health Care Survey (weight and height
   brackets), unweighted like Allcott et al. (2019, ValidateHealthMeasures.do), without and with controls and ZIP dummies.
4. By age: the change in HEI within a household by age of the heads, net of the calendar year. Age and year rise together, so differences across ages
   are identified and the common level is not.
5. The linear HEI (lhei_usda) against the two numbers Allcott et al. publish for theirs: correlation with HEI 0.91, top minus bottom income quartile
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
from utils.hei_units import hei_sd
from utils.food_measures import package_grams

import numpy as np
import pandas as pd
import pyreadstat
import statsmodels.api as sm

COMP = ['totfruit', 'wholfruit', 'totveg', 'greensbeans', 'wholegrain', 'dairy', 'totprot', 'seaplant', 'fattyacid', 'refgrain', 'sodium', 'addsugar', 'satfat']
GAP = 5.4


def wm(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok])


def load_panel():
    hei = pd.read_parquet(idir / 'panel_dataset' / 'hei_usda_panel.parquet')
    n = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet', columns=['household_code', 'panel_year', 'cals_all', 'ae_total', 'ae_complete'])
    n = n[n['ae_complete'] == 1]
    n['E'] = n['cals_all'] / n['ae_total'] / 365
    d = hei.merge(n[['household_code', 'panel_year', 'E']], on=['household_code', 'panel_year'], how='inner')
    lo, hi = d['E'].quantile([0.01, 0.99])
    d['E'] = d['E'].clip(lo, hi)
    return d


def densities(d):
    return {'tf': d.hei_c_totfruit / 5 * 0.8, 'wf': d.hei_c_wholfruit / 5 * 0.4, 'tv': d.hei_c_totveg / 5 * 1.1, 'gb': d.hei_c_greensbeans / 5 * 0.2, 'wg': d.hei_c_wholegrain / 10 * 1.5,
            'dy': d.hei_c_dairy / 10 * 1.3, 'pr': d.hei_c_totprot / 5 * 2.5, 'sp': d.hei_c_seaplant / 5 * 0.8, 'rg': 4.3 - d.hei_c_refgrain / 10 * 2.5, 'sod': 2.0 - d.hei_c_sodium / 10 * 0.9,
            'as': d.addsug_energy_share, 'sat': 16 - d.hei_c_satfat / 10 * 8.0}


def score(x, fa):
    ad = lambda v, std, pts: pts * np.clip(v / std, 0, 1)
    mo = lambda v, good, bad, pts: pts * np.clip((bad - v) / (bad - good), 0, 1)
    return (ad(x['tf'], 0.8, 5) + ad(x['wf'], 0.4, 5) + ad(x['tv'], 1.1, 5) + ad(x['gb'], 0.2, 5) + ad(x['wg'], 1.5, 10) + ad(x['dy'], 1.3, 10) + ad(x['pr'], 2.5, 5) + ad(x['sp'], 0.8, 5) + fa
            + mo(x['rg'], 1.8, 4.3, 10) + mo(x['sod'], 1.1, 2.0, 10) + mo(x['as'], 6.5, 26, 10) + mo(x['sat'], 8, 16, 10))


def food_scenarios(d):
    x0, E, w, fa = densities(d), d['E'], d['projection_factor'], d['hei_c_fattyacid']
    base = score(x0, fa)
    print(f"  check: recomputed minus actual HEI, mean {wm(base - d.hei_usda, w):.4f}; mean HEI {wm(d.hei_usda, w):.2f}; calories per adult equivalent per day {wm(E, w):,.0f}")

    def apple(n):
        g = 182 * n
        a = 95 * n
        k = E / (E + a)
        x = {c: v * k for c, v in x0.items()}
        x['tf'] = x['tf'] + (g / 150) / ((E + a) / 1000)
        x['wf'] = x['wf'] + (g / 150) / ((E + a) / 1000)
        return score(x, fa)

    def bread(n):
        x = dict(x0)
        x['wg'] = x['wg'] + n / (E / 1000)
        x['rg'] = x['rg'] - n / (E / 1000)
        return score(x, fa)

    def soda(n):
        a = 140 * n
        k = E / (E + a)
        x = {c: v * k for c, v in x0.items()}
        x['as'] = 100 * (x0['as'] / 100 * E + 156 * n) / (E + a)
        return score(x, fa)

    out = {}
    for name, f in [('medium apple', apple), ('whole-grain slice for a white slice', bread), ('can of soda', soda)]:
        out[name] = wm(f(1) - base, w)
        print(f"  one {name} per adult equivalent per day: {out[name]:+.2f} HEI points")
    for name, f in [('medium apples', apple), ('whole-grain slices', bread)]:
        lo, hi = 0.0, 5.0
        for _ in range(40):
            mid = (lo + hi) / 2
            if wm(f(mid) - base, w) < GAP:
                lo = mid
            else:
                hi = mid
        print(f"  {name} per adult equivalent per day for +{GAP} points: {lo:.2f}")
    return out


def write_table(m, a, b, fruit, food, health):
    """tabs/what_moves/hei_scale.tex"""
    c = lambda k: f'hei_c_{k}'
    sd = hei_sd()     # HEI in standard deviations, the paper's unit
    def row(lab, keys, dec=3):
        gap = (sum(m.loc[b, c(k)] for k in keys) - sum(m.loc[a, c(k)] for k in keys)) / sd
        return f'{lab} & & & {gap:+.{dec}f} \\\\'
    fa, fb = fruit.loc[a], fruit.loc[b]
    fruit_gap = round(fb['fruit_cups'] - fa['fruit_cups'], 2) + 0.0     # + 0.0 turns -0.0 into 0.0
    tex = [r'\begin{tabular}{lrrr}', r'\toprule', r' & Lowest bin & Highest bin & Difference \\', r'\midrule', r'\multicolumn{4}{l}{\textit{A. HEI and its components, highest minus lowest bin (SD)}} \\',
           f"Total & & & {(m.loc[b, 'hei'] - m.loc[a, 'hei']) / sd:+.3f} \\\\",
           row('Fruit (two components)', ['totfruit', 'wholfruit']), row('Added sugar', ['addsugar']), row('Dairy', ['dairy']), row('Seafood and plant protein', ['seaplant']), row('Whole grains', ['wholegrain']),
           row('Vegetables, greens and beans', ['totveg', 'greensbeans']), row('Sodium, total protein, fatty acids', ['sodium', 'totprot', 'fattyacid']), row('Refined grains', ['refgrain']), row('Saturated fat', ['satfat']),
           r'\midrule', r'\multicolumn{4}{l}{\textit{B. Purchases per adult equivalent per day}} \\',
           f"Barcoded fruit (cups) & {fa['fruit_cups']:.2f} & {fb['fruit_cups']:.2f} & {fruit_gap:+.2f} \\\\",
           f"Calories & {fa['kcal']:,.0f} & {fb['kcal']:,.0f} & {fb['kcal'] - fa['kcal']:+,.0f} \\\\",
           f"Fruit per 1,000 calories (cups) & {fa['fruit_per_1000']:.2f} & {fb['fruit_per_1000']:.2f} & {fb['fruit_per_1000'] - fa['fruit_per_1000']:+.2f} \\\\",
           r'\midrule', r'\multicolumn{4}{l}{\textit{C. One more a day per adult equivalent, by the scoring rules of HEI (SD)}} \\',
           f"Medium apple & & & {food['medium apple'] / sd:+.3f} \\\\", f"Whole-grain slice in place of a white slice & & & {food['whole-grain slice for a white slice'] / sd:+.3f} \\\\", f"Can of soda & & & {food['can of soda'] / sd:+.3f} \\\\",
           r'\midrule', rf'\multicolumn{{4}}{{l}}{{\textit{{D. Adults in households scoring {GAP / sd:.2f} SD higher, 2016--17}}}} \\',
           f"BMI & & & {health['bmi'][0]:+.2f} ({health['bmi'][1]:.2f}) \\\\", f"BMI of 30 or more (percentage points) & & & {health['obese'][0]:+.2f} ({health['obese'][1]:.2f}) \\\\",
           f"Type 2 diabetes (percentage points) & & & {health['diab'][0]:+.2f} ({health['diab'][1]:.2f}) \\\\", r'\bottomrule', r'\end{tabular}']
    (tdir / 'what_moves' / 'hei_scale.tex').write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))


def fruit_by_bin(h):
    """Cups of fruit bought per adult equivalent per day by income bin, from the purchases, so without the caps of the component scores."""
    cw = pd.read_parquet(idir / 'hei' / 'module_hei_crosswalk.parquet', columns=['product_module_code', 'hei_group', 'grams_per_equiv'])
    cw = cw[cw['hei_group'].isin(['fruit_whole', 'fruit_juice'])]
    parts = []
    for y in range(2004, 2021):
        d = pd.read_parquet(idir / 'purchases_food' / f'panel_year={y}', columns=['upc', 'household_code', 'quantity', 'multi', 'size1_amount', 'size1_units', 'department_descr', 'product_module_code', 'product_group_code'],
                            filters=[('product_module_code', 'in', cw['product_module_code'].tolist())])
        d = d[d['department_descr'] != 'MAGNET DATA'].merge(cw, on='product_module_code')
        d['upc_13'] = '0' + d['upc'].astype(str).str.zfill(12)
        d['cal_per_100g'] = np.nan
        d['eq'] = d['quantity'] * package_grams(d) / d['grams_per_equiv']
        d['whole'] = d['eq'].where(d['hei_group'] == 'fruit_whole', 0.0)
        a = d.groupby('household_code')[['eq', 'whole']].sum().reset_index()
        a['panel_year'] = y
        parts.append(a)
    f = pd.concat(parts, ignore_index=True)
    n = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet', columns=['household_code', 'panel_year', 'cals_all', 'ae_total', 'ae_complete'])
    n = n[n['ae_complete'] == 1]
    x = n.merge(f, on=['household_code', 'panel_year'], how='left').fillna({'eq': 0.0, 'whole': 0.0}).merge(h[['hhid', 'inc_b', 'inc', 'w']], left_on='household_code', right_on='hhid')
    x['cups'] = x['eq'] / x['ae_total'] / 365
    x['whole_cups'] = x['whole'] / x['ae_total'] / 365
    x['kcal'] = x['cals_all'] / x['ae_total'] / 365
    for c in ['cups', 'whole_cups', 'kcal']:
        x[c] = x[c].clip(upper=x[c].quantile(0.99))
    t = x.groupby('inc_b').apply(lambda g: pd.Series({'income': np.average(g['inc'], weights=g['w']), 'kcal': np.average(g['kcal'], weights=g['w']), 'fruit_cups': np.average(g['cups'], weights=g['w']),
                                                      'whole_cups': np.average(g['whole_cups'], weights=g['w']), 'fruit_per_1000': 1000 * np.sum(g['w'] * g['cups']) / np.sum(g['w'] * g['kcal'])}))
    return t


def main():
    h, sd = household_means()
    d = load_panel()
    print(f'household-years with a complete roster: {len(d):,}; SD of HEI as in 12_: {sd:.3f}')

    # ---- 1. the income gap by component
    print('\n1. Largest income gap, by component')
    c = d.groupby('household_code')[[f'hei_c_{k}' for k in COMP] + ['hei_usda', 'E', 'addsug_energy_share']].mean().reset_index()
    h2 = h.merge(c, left_on='hhid', right_on='household_code', how='inner')
    m = h2.groupby('inc_b').apply(lambda g: pd.Series({k: np.average(g[k], weights=g.w) for k in ['hei'] + [f'hei_c_{x}' for x in COMP] + ['E', 'inc']}))
    bin_lo, bin_hi = m['hei'].idxmin(), m['hei'].idxmax()
    print(f"  lowest-scoring income bin {bin_lo} of 0-19 (income {m.loc[bin_lo, 'inc']:.1f}k), highest {bin_hi} (income {m.loc[bin_hi, 'inc']:.1f}k), gap {m.loc[bin_hi, 'hei'] - m.loc[bin_lo, 'hei']:.2f}; sample here {len(h2):,} households")
    t = pd.DataFrame({'low': m.loc[bin_lo], 'high': m.loc[bin_hi]})
    t['gap'] = t['high'] - t['low']
    print(t.round(2).to_string())

    fruit = fruit_by_bin(h)
    print(fruit.round(3).to_string())

    # ---- 2. food
    print('\n2. Food, all household-years')
    food = food_scenarios(d)
    low = d[d['household_code'].isin(h.loc[h.inc_b == bin_lo, 'hhid'])]
    print('   households in the lowest-scoring income bin')
    food_scenarios(low)

    # ---- 3. health
    print('\n3. Health, adults 20+, 2016-17')
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
    print(f"  {len(x):,} adults, {x['household_code'].nunique():,} households; BMI mean {x.bmi.mean():.1f}, SD {x.bmi.std():.1f}; BMI 30+ {x.obese.mean():.1f}%; diabetes {x.diab.mean():.1f}%; mean of household mean HEI {x.hei.mean():.1f}, SD {x.hei.std():.1f}")
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
        return r.params['v'], r.bse['v']
    health = {}
    for y, lab in [('bmi', 'BMI'), ('obese', 'BMI 30+ (points)'), ('diab', 'Type 2 diabetes (points)')]:
        for name, X, z in [('no controls', None, False), ('age x sex, income, education, race, Hispanic, size, children, region', ctrl, False), ('same + ZIP', ctrl, True)]:
            b, se = run(y, 'hei', X, z)
            bl, sel = run(y, 'lhei', X, z)
            print(f"  {lab}, {name}: per {GAP} HEI points {b * GAP:+.3f} ({se * GAP:.3f}); per 0.56 SD of the linear HEI {bl * 0.56 * lsd:+.3f} ({sel * 0.56 * lsd:.3f})")
            if X is not None and not z:
                health[y] = (b * GAP, se * GAP)
    write_table(m, bin_lo, bin_hi, fruit, food, health)

    # ---- 4. age
    print('\n4. HEI within a household by age of the heads')
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

    # ---- 5. the linear HEI
    print('\n5. Linear HEI against Allcott et al.')
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
