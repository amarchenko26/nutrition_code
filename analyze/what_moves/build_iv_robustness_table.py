"""Tables for build_iv_robustness.do (2026-11-06): tabs/what_moves/iv_robustness.tex, the income IV on HEI, the ultra-processed share,
the produce share and calories under other fixed effects, timing, samples, instruments and windows, with stars at 10, 5 and 1 percent;
tabs/what_moves/iv_size_changes.tex (Table 10, from iv_size_changes.csv of `do build_iv_robustness.do sizetab`; one regression since
2026-11-12): whether household size changes from t to t+1, on the instrument alone, per $10k of group income. Prints where the estimate comes from
(iv_unpack.csv, iv_zero_first_stage.csv) and how the price paid per calorie varies with income. Reads interim/what_moves/iv_robustness_<outcome>.csv.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.hei_units import to_sd, hei_sd, hei_mean_sd
from utils.stars import star_num

import numpy as np
import pandas as pd
import pyreadstat
from scipy.stats import norm

WM = idir / 'what_moves'
OUTS = [('hei_usda', 3), ('upf_share', 2), ('produce_share', 2), ('cals_ae_day', 1)]
# the same income change on purchases before it (Anya 2026-11-09: placebos for the main row and the two groups that do not move
# with household size, and for the same-size row so that the rows of Panel A of the summary table are all here)
PLACEBO = r'G. Placebo: purchases $t-3$ to $t-2$, before the income change'
ROWS = [
    (r'A. Fixed effects', [('main', r'Main: household and state-by-year'), ('hh_year', 'Household and year')]),
    (r'B. Purchase window, same income change', [('level_t', r'$t-1$ to $t$'), ('twoyear', r'$t-1$ to $t+1$')]),
    (r'C. Households', [('same_size', r'Same household size at $t-1$, $t$, $t+1$'), ('age60', r"Heads' average age under 60 at $t+1$"),
                        ('geo_movers', 'Keeping households that change county'),
                        ('stayers', r'Group does not change from $t-1$ to $t$'), ('switchers', r'Group changes from $t-1$ to $t$'), ('switch_noNE', 'Group changes, not into not employed')]),
    (r'D. Instrument', [('no_size_bin', 'Group without the size bin'), ('baseline_occ', 'Occupation frozen at the first panel year'), ('predicted', 'Predicted group from $t-1$ characteristics'),
                        ('acs', 'Group income from the ACS'), ('bls', 'National wage of the occupation (BLS)')]),
    (r'E. Longer changes', [('k2', r'Changes over 2 years'), ('k3', r'Changes over 3 years'), ('k5', r'Changes over 5 years'), ('k8', r'Changes over 8 years')]),
    (r'F. Quartile of income in the first panel year', [('q1', 'Bottom'), ('q2', 'Second'), ('q3', 'Third'), ('q4', 'Top')]),
    (PLACEBO, [('main', 'Main'), ('same_size', r'Same household size at $t-1$, $t$, $t+1$'), ('no_size_bin', 'Group without the size bin'),
               ('predicted', 'Predicted group from $t-1$ characteristics')]),
]


def est(x, dec):
    """Estimate with its stars, then the standard error."""
    p = 2 * (1 - norm.cdf(abs(x['b'] / x['se'])))
    st = '***' if p < 0.01 else '**' if p < 0.05 else '*' if p < 0.10 else ''
    b = round(float(x['b']), dec) + 0.0     # + 0.0 prints -0.000 as 0.000
    return f"{b:.{dec}f}" + (f'$^{{{st}}}$' if st else '') + f" ({x['se']:.{dec}f})"


def main():
    d = pd.concat([pd.read_csv(WM / f'iv_robustness_{o}.csv') for o, _ in OUTS], ignore_index=True)
    d = to_sd(d)     # HEI in standard deviations, the paper's unit

    def get(o, spec, window='main'):
        x = d[(d['outcome'] == o) & (d['spec'] == spec) & (d['window'] == window)]
        return x.iloc[0] if len(x) else None

    def cell(o, spec, dec, window='main'):
        x = get(o, spec, window)
        # calories in levels compare years across which household size changes
        if o == 'cals_ae_day' and spec == 'levels':
            return ''
        # these outcomes come from the panel of analyze/movers/1_, which holds non-movers only
        if o != 'hei_usda' and spec == 'geo_movers':
            return ''
        if x is None or x['f'] != x['f']:
            return ''
        # no estimate when the first stage is zero
        if not np.isnan(x['f']) and x['f'] < 1:
            return 'no first stage'
        return est(x, dec)

    tex = [r'\begin{tabular}{lccccccc}', r'\toprule', r' & HEI & HEI, purchases & Ultra-processed & Produce & Calories per & First- & Household- \\',
           r'Specification & (SD) & before ($t-3$ to $t-2$) & (\% of calories) & (\% of calories) & adult eq. per day & stage F & years \\']
    for panel, rows in ROWS:
        w = 'before' if panel == PLACEBO else 'main'
        tex += [r'\midrule', rf'\multicolumn{{8}}{{l}}{{\textit{{{panel}}}}} \\']
        for spec, lab in rows:
            cells = [cell('hei_usda', spec, 3, w)]
            b = get('hei_usda', spec, 'before')
            cells.append('' if w == 'before' or b is None or b['f'] < 1 else est(b, 3))
            for o, dec in OUTS[1:]:
                cells.append(cell(o, spec, dec, w))
            x = get('hei_usda', spec, w)
            cells.append('' if x is None else f"{x['f']:.0f}" if x['f'] >= 1 else '0')
            cells.append('' if x is None else f"{int(x['n']):,}")
            tex.append(f'{lab} & ' + ' & '.join(cells) + r' \\')
    tex += [r'\bottomrule', r'\end{tabular}']
    (tdir / 'what_moves' / 'iv_robustness.tex').write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))
    # purchases before the income change, any outcome, any row with a first stage
    shown = [spec for _, rows in ROWS for spec, _ in rows]
    bf = d[(d['window'] == 'before') & (d['f'] >= 1) & d['spec'].isin(shown)].copy()
    bf['p'] = 2 * (1 - norm.cdf((bf['b'] / bf['se']).abs()))
    print('tests on purchases from t-3 to t-2 with p < 0.05:', bf.loc[bf['p'] < 0.05, ['outcome', 'spec', 'b', 'se', 'p']].round(3).to_dict('records'), 'of', len(bf))

    # ---- where the estimate comes from, printed
    u = pd.read_csv(WM / 'iv_unpack.csv')
    x = u[u['table'] == 'shares'].copy()
    x.columns = ['table', 'outcome', 'window', 'group', 'N', 'instrument_var', 'own_income_link', 'outcome_link', 'ratio', 'pooled']
    print(x.drop(columns='table').round(3).to_string(index=False))
    pth = u[u['table'] == 'path'].copy()
    pth.columns = ['table', 'outcome', 'k', 'group', 'per_group_income', 'se', 'per_own_income', 'se_own', 'f', 'n']
    print(pth.drop(columns='table').round(3).to_string(index=False))
    print(pd.read_csv(WM / 'iv_zero_first_stage.csv').round(4).to_string(index=False))
    if (WM / 'iv_size_changes.csv').exists():
        size_table()
    price_slopes()


def size_table():
    r = pd.read_csv(WM / 'iv_size_changes.csv').iloc[0]
    lines = [r'\begin{tabular}{lc}', r'\toprule',
             r' & Household size changes \\', r' & from $t$ to $t+1$ (\%) \\', r'\midrule',
             rf'Instrument: $\Delta$ group income, $t-1$ to $t$ (\$10k) & {star_num(r.b, r.se, 2)} \\',
             rf' & ({r.se:.2f}) \\', r'\midrule',
             rf"Mean of outcome & {r['mean']:.1f} \\", rf'N & {int(r.n):,} \\', r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'iv_size_changes.tex'
    out.write_text('\n'.join(lines) + '\n')
    print('\n'.join(lines))
    print(f'saved {out}')


def price_slopes():
    """Real $ per 1,000 kcal on income ($10k): across households (year, age, kids, composition) and within a household (+ household), clustered on household."""
    p, _ = pyreadstat.read_dta(str(WM / 'price_per_kcal.dta'))
    r, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'), usecols=['hhid', 'year', 'real_income', 'projection_factor', 'avg_age_hh_head', 'kids', 'hh_comp'])
    d = r.merge(p, on=['hhid', 'year'], how='inner').dropna(subset=['price_1000', 'real_income'])
    d['age5'] = np.floor(d['avg_age_hh_head'] / 5) * 5
    w = d['projection_factor']

    def slope(fe):
        x, y = d['real_income'].astype(float).copy(), d['price_1000'].astype(float).copy()
        for _ in range(8):
            for f in fe:
                x -= (x * w).groupby(d[f]).transform('sum') / w.groupby(d[f]).transform('sum')
                y -= (y * w).groupby(d[f]).transform('sum') / w.groupby(d[f]).transform('sum')
        b = (w * x * y).sum() / (w * x * x).sum()
        g = (w * x * (y - b * x)).groupby(d['hhid']).sum()
        return b, np.sqrt((g ** 2).sum()) / (w * x * x).sum()
    print('price per 1,000 kcal on income, across households:', slope(['year', 'age5', 'kids', 'hh_comp']))
    print('price per 1,000 kcal on income, within a household:', slope(['hhid', 'year', 'age5', 'kids', 'hh_comp']))


if __name__ == '__main__':
    main()
