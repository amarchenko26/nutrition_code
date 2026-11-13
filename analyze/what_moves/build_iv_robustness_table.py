"""Tables for build_iv_robustness.do (2026-11-06): tabs/what_moves/iv_robustness.tex (Table 9), the income IV on HEI, the ultra-processed
share, the produce share and calories in other samples, instruments, purchase windows, longer changes and baseline income quartiles: HEI
and the linearized HEI from USDA and from Syndigo labels, each next to its placebo (purchases t-3 to t-2 on the same income change; layout
of 2026-11-12), standard errors on their own row, stars at 10, 5, 1 percent;
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
# Indices in SD, each with its placebo (purchases t-3 to t-2 on the same income change): HEI and the linearized HEI of Allcott et al.
# (their published Health Index), from USDA labels and from Syndigo labels (Syndigo columns added 2026-11-12, Anya; hei_usda is in
# points and converted by to_sd, the others are standardized in build_iv_robustness.do, each by its own SD)
INDICES = [('hei_usda', 'HEI (SD)', 'USDA labels'), ('hei_syn_sd', 'HEI (SD)', 'Syndigo labels'),
           ('lhei_sd', 'Linearized HEI, Allcott et al. (SD)', 'USDA labels'), ('lhei_syn_sd', 'Linearized HEI, Allcott et al. (SD)', 'Syndigo labels')]
PLACEBO_LHEI_USDA = True     # Anya asked for the placebos of the Syndigo indices; this gives the USDA linearized HEI one as well
OTHER = [('upf_share', 2), ('produce_share', 2), ('cals_ae_day', 1)]
OUTS = [(o, 3) for o, _, _ in INDICES] + OTHER
# outcomes built for households that change county as well (the others come from analyze/movers/1_, which holds non-movers only)
ALL_HH = {o for o, _, _ in INDICES}
# Layout of 2026-11-12 (Anya): Panel A = the samples, from every household employed at t-1 to non-movers under 60 of the same
# size; the purchase windows in words, right above the longer changes; each index has its placebo next to it, and the placebo
# panel is gone. 'Same year and the next' (purchases t-1 to t+1 on the one-year income change) deleted 2026-11-12 (Anya: the
# outcome and income changes must span the same length of time). Standard errors on their own row and long labels on two lines,
# so the table fits a landscape page. A label is a string or (estimate-row text, standard-error-row text). The ACS row since
# 2026-11-13 (Anya): the household keeps its group of t-1, income change from the ACS, clustered on that group; it replaces the
# row on the household's group in each year.
ROWS = [
    (r'A. Sample', [('geo_movers', r'All, employed at $t-1$'), ('main', r'Non-movers, employed at $t-1$'),
                    ('age60', (r'Non-movers, employed at $t-1$,', 'under 60')),
                    ('age60_sizet', (r'Non-movers, employed at $t-1$,', r'under 60, same size at $t-1$ and $t$'))]),
    (r'B. Instrument, non-movers employed at $t-1$', [('no_size_bin', 'Group without the size bin'),
                                                      ('baseline_occ', ('Occupation frozen at the', 'first panel year')),
                                                      ('predicted', ('Predicted group from', '$t-1$ characteristics')),
                                                      ('acs', ('Group at $t-1$, income', 'from the ACS')),
                                                      ('bls', ('National wage of the', 'occupation (BLS)'))]),
    (r'C. Purchase window, same income change', [('level_t', 'Same year as the income change')]),
    (r'D. Longer changes', [('k2', r'Changes over 2 years'), ('k3', r'Changes over 3 years'), ('k5', r'Changes over 5 years'), ('k8', r'Changes over 8 years')]),
    (r'E. Quartile of income in the first panel year', [('q1', 'Bottom'), ('q2', 'Second'), ('q3', 'Third'), ('q4', 'Top')]),
]


def est(x, dec):
    """Estimate with its stars, and the standard error in parentheses."""
    p = 2 * (1 - norm.cdf(abs(x['b'] / x['se'])))
    st = '***' if p < 0.01 else '**' if p < 0.05 else '*' if p < 0.10 else ''
    b = round(float(x['b']), dec) + 0.0     # + 0.0 prints -0.000 as 0.000
    return f"{b:.{dec}f}" + (f'$^{{{st}}}$' if st else ''), f"({x['se']:.{dec}f})"


def main():
    d = pd.concat([pd.read_csv(WM / f'iv_robustness_{o}.csv') for o, _ in OUTS], ignore_index=True)
    d = to_sd(d)     # HEI in standard deviations, the paper's unit

    def get(o, spec, window='main'):
        x = d[(d['outcome'] == o) & (d['spec'] == spec) & (d['window'] == window)]
        return x.iloc[0] if len(x) else None

    def cell(o, spec, dec, window='main'):
        """(estimate row, standard-error row) of one cell."""
        x = get(o, spec, window)
        # calories in levels compare years across which household size changes
        if o == 'cals_ae_day' and spec == 'levels':
            return '', ''
        if o not in ALL_HH and spec == 'geo_movers':
            return '', ''
        if x is None or x['f'] != x['f']:
            return '', ''
        # no estimate when the first stage is zero; no placebo either
        if x['f'] < 1:
            return ('no first stage' if window == 'main' else ''), ''
        return est(x, dec)

    cols = [(o, w) for o, _, _ in INDICES for w in ('main', 'before') if PLACEBO_LHEI_USDA or (o, w) != ('lhei_sd', 'before')]
    ncol = 1 + len(cols) + len(OTHER) + 2
    # header: index (two columns per label source and window), label source, estimate or placebo; the other outcomes on three lines
    grp, src = [], []
    for o, w in cols:
        name, s = next((n, s) for oo, n, s in INDICES if oo == o)
        if grp and grp[-1][0] == name:
            grp[-1][1] += 1
        else:
            grp.append([name, 1])
        if src and src[-1][0] == (name, s):
            src[-1][1] += 1
        else:
            src.append([(name, s), 1])

    def spans(groups, lab):
        out, rules, c = [], [], 2
        for g, n in groups:
            out.append(rf'\multicolumn{{{n}}}{{c}}{{{lab(g)}}}' if n > 1 else lab(g))
            rules.append(rf'\cmidrule(lr){{{c}-{c + n - 1}}}')
            c += n
        return out, ' '.join(rules)
    h1, r1 = spans(grp, lambda g: g)
    h2, r2 = spans(src, lambda g: g[1])
    h3 = ['Estimate' if w == 'main' else 'Placebo' for _, w in cols]
    right = [['Ultra-processed', 'Produce', 'Calories per', 'First-', 'Household-'],
             [r'(\% of', r'(\% of', 'adult eq.', 'stage', 'years'],
             [r'calories)', r'calories)', 'per day', 'F', '']]
    tex = [rf'\begin{{tabular}}{{l*{{{ncol - 1}}}{{c}}}}', r'\toprule',
           ' & ' + ' & '.join(h1 + right[0]) + r' \\', r1,
           ' & ' + ' & '.join(h2 + right[1]) + r' \\', r2,
           'Specification & ' + ' & '.join(h3 + right[2]) + r' \\']
    for panel, rows in ROWS:
        tex += [r'\midrule', rf'\multicolumn{{{ncol}}}{{l}}{{\textit{{{panel}}}}} \\']
        for spec, lab in rows:
            top, bot = ([lab], ['']) if isinstance(lab, str) else ([lab[0]], [lab[1]])
            for o, w in cols:
                b, s = cell(o, spec, 3, w)
                top.append(b)
                bot.append(s)
            for o, dec in OTHER:
                b, s = cell(o, spec, dec)
                top.append(b)
                bot.append(s)
            x = get('hei_usda', spec, 'main')
            top += ['' if x is None else f"{x['f']:.0f}" if x['f'] >= 1 else '0', '' if x is None else f"{int(x['n']):,}"]
            bot += ['', '']
            tex += [' & '.join(top) + r' \\', ' & '.join(bot) + r' \\']
    tex += [r'\bottomrule', r'\end{tabular}']
    # every row of the body has one cell per column
    body = [l for l in tex[7:] if l.endswith(r'\\') and not l.startswith(r'\multicolumn')]
    assert all(l.count('&') == ncol - 1 for l in body), 'cell count'
    (tdir / 'what_moves' / 'iv_robustness.tex').write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))
    # the placebo cells shown
    shown = [spec for _, rows in ROWS for spec, _ in rows]
    pl = d[(d['window'] == 'before') & (d['f'] >= 1) & d['spec'].isin(shown) & d['outcome'].isin([o for o, w in cols if w == 'before'])].copy()
    pl['p'] = 2 * (1 - norm.cdf((pl['b'] / pl['se']).abs()))
    print(f"placebo cells shown: {len(pl)}; p < 0.10: {int((pl['p'] < 0.10).sum())}; p < 0.05: {int((pl['p'] < 0.05).sum())}; smallest p {pl['p'].min():.3f}")
    print(pl.loc[pl['p'] < 0.10, ['outcome', 'spec', 'b', 'se', 'p']].round(3).to_string(index=False))
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
