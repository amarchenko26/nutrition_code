"""National path of the nutrition measures, 2004-2020, for notes/what_moves_nutrition.tex (2026-11-05).

Projection-weighted means over the household-year panel: full HEI and its added-sugar, sodium, saturated-fat,
whole-grain and vegetable components (interim/panel_dataset/hei_usda_panel.parquet), sugar per 1,000 kcal from the
Syndigo labels (panel_hh_year.sugar_per_1000cal) and from the USDA labels (final/labor_supply_reg_data.dta
sugar_per_1000cal_usda), the soda share of grocery spending (spend_soda / spend_total, reg data), the
ultra-processed share of calories, the produce share of calories and calories per adult equivalent per day
(interim/health/nova_calorie_panel.parquet, complete rosters, calories winsorized at 4,000), and real household
income (thousands of 2013 dollars, the panel's real_income). Output: tabs/what_moves/national_series.tex (every fourth year and the 2004-2020
change); the full year-by-year table is printed. Also tabs/what_moves/gaps_by_group.tex: HEI and sugar gaps between within-year income
quartiles and between education groups by year, and the within-year slope of HEI on income (final/labor_supply_reg_data.dta).
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

OUT = tdir / 'what_moves' / 'national_series.tex'


def wmean(g, c):
    ok = g[c].notna()
    return np.average(g.loc[ok, c], weights=g.loc[ok, 'projection_factor']) if ok.any() else np.nan


def main():
    p = pd.read_parquet(idir / 'panel_dataset' / 'panel_hh_year.parquet',
                        columns=['household_code', 'panel_year', 'sugar_per_1000cal', 'real_income', 'projection_factor'])
    h = pd.read_parquet(idir / 'panel_dataset' / 'hei_usda_panel.parquet',
                        columns=['household_code', 'panel_year', 'hei_usda', 'hei_c_addsugar', 'hei_c_sodium', 'hei_c_satfat',
                                 'hei_c_wholegrain', 'hei_c_totveg'])
    n = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet',
                        columns=['household_code', 'panel_year', 'cals_all', 'cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4',
                                 'produce_cals_all', 'ae_total', 'ae_complete'])
    n = n[n['ae_complete'] == 1].copy()
    n['cals_ae_day'] = (n['cals_all'] / n['ae_total'] / 365).clip(upper=4000)
    n['upf_share'] = 100 * n['cals_nova4'] / n[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
    n['produce_share'] = 100 * n['produce_cals_all'] / n['cals_all']
    r, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'),
                               usecols=['hhid', 'year', 'sugar_per_1000cal_usda', 'spend_soda', 'spend_total'])
    r = r.rename(columns={'hhid': 'household_code', 'year': 'panel_year'})
    r['household_code'] = r['household_code'].astype('int64'); r['panel_year'] = r['panel_year'].astype(int)
    r['soda_share'] = 100 * r['spend_soda'] / r['spend_total']
    d = p.merge(h, on=['household_code', 'panel_year']).merge(n, on=['household_code', 'panel_year'], how='left') \
         .merge(r, on=['household_code', 'panel_year'], how='left')
    d['income_k'] = d['real_income']          # real_income is already in thousands of 2013 dollars
    cols = [('hei_usda', 'HEI (0--100)', 1), ('hei_c_addsugar', 'HEI added-sugar component (0--10)', 2),
            ('hei_c_sodium', 'HEI sodium component', 2), ('hei_c_satfat', 'HEI saturated-fat component', 2),
            ('hei_c_wholegrain', 'HEI whole-grain component', 2), ('hei_c_totveg', 'HEI vegetable component', 2),
            ('sugar_per_1000cal', 'Sugar, g per 1,000 kcal (Syndigo labels)', 1),
            ('sugar_per_1000cal_usda', 'Sugar, g per 1,000 kcal (USDA labels)', 1),
            ('soda_share', 'Soda, \\% of grocery spending', 2), ('upf_share', 'Ultra-processed, \\% of calories', 1),
            ('produce_share', 'Produce, \\% of calories', 1), ('cals_ae_day', 'Calories per adult equivalent per day', 0),
            ('income_k', 'Real household income, \\$ thousands', 1)]
    rows = {}
    for y, g in d.groupby('panel_year'):
        rows[y] = {c: wmean(g, c) for c, _, _ in cols}
    t = pd.DataFrame(rows).T
    print(t.round(2).to_string())
    years = [2004, 2008, 2012, 2016, 2020]
    lines = ['\\begin{tabular}{l' + 'r' * len(years) + 'r}', '\\toprule',
             ' & ' + ' & '.join(str(y) for y in years) + ' & 2004--2020 \\\\', '\\midrule']
    for c, lab, dec in cols:
        vals = [t.loc[y, c] for y in years]
        chg = vals[-1] - vals[0]
        lines.append(f'{lab} & ' + ' & '.join(f'{v:,.{dec}f}' for v in vals) + f' & {chg:+,.{dec}f} \\\\')
    lines += ['\\bottomrule', '\\end{tabular}']
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text('\n'.join(lines) + '\n')
    print(f'saved {OUT}')


def gaps_table():
    d, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'), usecols=['year', 'hei_usda', 'sugar_per_1000cal_usda', 'hh_avg_yrsofschool', 'real_income', 'projection_factor'])
    d = d.dropna(subset=['hei_usda', 'sugar_per_1000cal_usda', 'real_income', 'projection_factor', 'hh_avg_yrsofschool'])
    rng = np.random.default_rng(0)
    d = d.assign(tie=rng.random(len(d))).sort_values(['year', 'real_income', 'tie'])
    d['cum'] = d.groupby('year')['projection_factor'].cumsum() / d.groupby('year')['projection_factor'].transform('sum')
    d['q'] = np.minimum((d['cum'] * 4).astype(int), 3)
    years = [2004, 2007, 2012, 2016, 2020]
    rows = {}
    for y in years:
        g = d[d['year'] == y]
        low, high = g[g['hh_avg_yrsofschool'] < 13], g[g['hh_avg_yrsofschool'] >= 16]
        bot, top = g[g['q'] == 0], g[g['q'] == 3]
        rows[y] = {'hei_q': wmean(top, 'hei_usda') - wmean(bot, 'hei_usda'),
                   'slope': np.polyfit(g['real_income'], g['hei_usda'], 1, w=np.sqrt(g['projection_factor']))[0],
                   'hei_low': wmean(low, 'hei_usda'), 'hei_high': wmean(high, 'hei_usda'), 'hei_e': wmean(high, 'hei_usda') - wmean(low, 'hei_usda'),
                   'sug_q': wmean(bot, 'sugar_per_1000cal_usda') - wmean(top, 'sugar_per_1000cal_usda'),
                   'sug_e': wmean(low, 'sugar_per_1000cal_usda') - wmean(high, 'sugar_per_1000cal_usda')}
    t = pd.DataFrame(rows)
    print(t.round(2).to_string())
    labs = [('hei_q', 'HEI, top minus bottom income quartile'), ('slope', 'HEI on income within the year, points per \\$10,000'), ('hei_low', 'HEI, 12 or fewer years of schooling'),
            ('hei_high', 'HEI, 16 or more years of schooling'), ('hei_e', 'HEI, 16 or more minus 12 or fewer'), ('sug_q', 'Sugar per 1,000 kcal, bottom minus top income quartile'),
            ('sug_e', 'Sugar per 1,000 kcal, 12 or fewer minus 16 or more')]
    lines = ['\\begin{tabular}{l' + 'r' * len(years) + '}', '\\toprule', ' & ' + ' & '.join(str(y) for y in years) + ' \\\\', '\\midrule']
    for k, lab in labs:
        lines.append(f'{lab} & ' + ' & '.join(f'{t.loc[k, y]:.2f}' for y in years) + ' \\\\')
    lines += ['\\bottomrule', '\\end{tabular}']
    out = tdir / 'what_moves' / 'gaps_by_group.tex'
    out.write_text('\n'.join(lines) + '\n')
    print(f'saved {out}')


if __name__ == '__main__':
    main()
    gaps_table()
