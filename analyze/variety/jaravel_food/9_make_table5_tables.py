"""Table fragments for the Jaravel (2019) Table 5 replication on food cells and the healthiness split.

Usage: python 9_make_table5_tables.py [--out DIR] [--extra]
Reads  interim/variety/jaravel_food/cells_2006_<END>/results_s57.dta (6_), descriptive_regs.dta (8_), ar_checks.dta (7_).
Writes to tabs/variety/jaravel_food (or --out DIR): table5_replication.tex, table5_health.tex; with --extra also
       table5_health3.tex and decile_inflation_health_<END>.tex (not in the paper).
Stars: * 10%, ** 5%, *** 1% (utils/stars.py, normal distribution).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403
from utils.stars import stars  # noqa: E402

_, TAB, EXTRA = out_dirs(sys.argv)
TAB.mkdir(parents=True, exist_ok=True)
WIN = {2015: '2006--07 to 2014--15 (his years)', 2020: '2006--07 to 2019--20'}
HIS = {  # Jaravel (2019) Table V, all departments, N = 8,600: (b, se, KP F, Cragg-Donald F)
    'c1': (2.760, 0.556, None, None), 'c2': (-1.042, 0.201, None, None),
    'c3': (-0.378, 0.081, 24.638, 43.216), 'c4': (-0.458, 0.14, 12.964, 24.249), 'c5': (-0.418, 0.139, 13.981, 15.114),
    'c7': (-1.721, 0.308, None, None),
    'c8': (-0.624, 0.128, 24.638, 43.216), 'c9': (-0.673, 0.241, 12.964, 24.249), 'c10': (-0.618, 0.258, 13.981, 15.114)}
COLS = ['c1', 'c2', 'c3', 'c4', 'c5', 'c7', 'c8', 'c9', 'c10']
SPECS = [('ces', 'linageres', 'no'), ('ces', 'allres', 'no'), ('ces', 'allres', 'yes'),
         ('full', 'linageres', 'no'), ('full', 'allres', 'no'), ('full', 'allres', 'yes')]
HEAD6 = [r'\begin{tabular}{l' + 'c' * 6 + '}', r'\toprule',
         r' & \multicolumn{3}{c}{Inflation, continuing products} & \multicolumn{3}{c}{Inflation with new and exiting products} \\',
         r'\cmidrule(lr){2-4}\cmidrule(lr){5-7}', r' & (1) & (2) & (3) & (4) & (5) & (6) \\', r'\midrule']
FOOT6 = [r'Household-type growth net of & age & all & all & age & all & all \\',
         r'Product-group and price-decile f.e. & & & \checkmark & & & \checkmark \\', r'\bottomrule', r'\end{tabular}']


def res(E, sig='s57'):
    return pd.read_stata(DATA / f'cells_2006_{E}' / f'results_{sig}.dta')


def fmt(b, se, dec=3):
    if pd.isna(b):
        return '', ''
    return f'{round(float(b), dec) + 0.0:.{dec}f}{stars(b, se)}', f'({float(se):.{dec}f})'


def fnum(v, dec=1):
    return '' if pd.isna(v) else f'{v:,.{dec}f}'


def getter(r):
    def g(o, i, fe, block, term, sample='all'):
        x = r[(r.block == block) & (r.outcome == o) & (r.inst == i) & (r.fe == fe) & (r.term == term) & (r['sample'] == sample)]
        return x.iloc[0]
    return g


def table5():
    L = [r'\begin{tabular}{l' + 'c' * 9 + '}', r'\toprule',
         r' & Real spending & \multicolumn{4}{c}{Inflation, continuing products} & \multicolumn{4}{c}{Inflation with new and exiting products} \\',
         r'\cmidrule(lr){2-2}\cmidrule(lr){3-6}\cmidrule(lr){7-10}', r' & growth & & & & & & & & \\',
         ' & ' + ' & '.join(f'({c[1:]})' for c in COLS) + r' \\',
         ' & ' + ' & '.join(['OLS', 'OLS', 'IV', 'IV', 'IV', 'OLS', 'IV', 'IV', 'IV']) + r' \\', r'\midrule']
    for pi, E in enumerate([2015, 2020]):
        t = res(E)
        t = t[t.block == 'table5']
        g = lambda o, i, fe, term: t[(t.outcome == o) & (t.inst == i) & (t.fe == fe) & (t.term == term)].iloc[0]  # noqa: E731
        c = {'c1': g('x', 'linageres', 'no', 'first_stage'), 'c2': g('ces', 'linageres', 'no', 'reduced_form'),
             'c3': g('ces', 'linageres', 'no', 'x'), 'c4': g('ces', 'allres', 'no', 'x'), 'c5': g('ces', 'allres', 'yes', 'x'),
             'c7': g('full', 'linageres', 'no', 'reduced_form'), 'c8': g('full', 'linageres', 'no', 'x'),
             'c9': g('full', 'allres', 'no', 'x'), 'c10': g('full', 'allres', 'yes', 'x')}
        L.append(r'\multicolumn{10}{l}{\textit{Panel ' + 'AB'[pi] + ': food cells, ' + WIN[E] + r'}} \\')
        ri, ris, rx, rxs = ['Instrument (linear age)'], [''], ['Real spending growth'], ['']
        for k in COLS:
            b, se = fmt(c[k].b, c[k].se)
            if k in ('c1', 'c2', 'c7'):
                ri.append(b); ris.append(se); rx.append(''); rxs.append('')
            else:
                rx.append(b); rxs.append(se); ri.append(''); ris.append('')
        for row in (ri, ris, rx, rxs):
            L.append(' & '.join(row) + r' \\')
        L.append('First-stage F (Kleibergen--Paap) & ' + ' & '.join(fnum(c[k].F_kp, 2) if k not in ('c1', 'c2', 'c7') else '' for k in COLS) + r' \\')
        L.append('Cragg--Donald F & ' + ' & '.join(fnum(c[k].F_cd, 2) if k not in ('c1', 'c2', 'c7') else '' for k in COLS) + r' \\')
        L.append('Cells (modules) & ' + ' & '.join(f'{int(c[k].N):,} ({int(c[k].nmod)})' for k in COLS) + r' \\')
        if E == 2015:
            hb, hs = ['Jaravel (2019), all departments'], ['']
            for k in COLS:
                x = fmt(HIS[k][0], HIS[k][1])
                hb.append(x[0]); hs.append(x[1])
            L.append(' & '.join(hb) + r' \\')
            L.append(' & '.join(hs) + r' \\')
            L.append(r'\quad First-stage F & ' + ' & '.join(fnum(HIS[k][2], 2) if HIS[k][2] else '' for k in COLS) + r' \\')
            L.append(r'\quad Cells & \multicolumn{9}{c}{8,600 (860 modules)} \\')
        L.append(r'\midrule')
    L += [r'Household-type growth net of & age & age & age & all & all & age & age & all & all \\',
          r'Product-group and price-decile f.e. & & & & & \checkmark & & & & \checkmark \\', r'\bottomrule', r'\end{tabular}']
    (TAB / 'table5_replication.tex').write_text('\n'.join(L) + '\n')


def health():
    ar = pd.read_stata(DATA / 'ar_checks.dta')
    L = list(HEAD6)
    for pi, E in enumerate([2015, 2020]):
        g = getter(res(E))
        L.append(r'\multicolumn{7}{l}{\textit{Panel ' + 'AB'[pi] + ': ' + WIN[E] + r'}} \\')
        for lab, term in [('Slope, healthy modules (score 4--10)', 'healthy'), ('Slope, unhealthy modules (score 0--3)', 'unhealthy'),
                          ('Difference, unhealthy minus healthy', 'difference')]:
            bs = [fmt(g(o, i, fe, 'health2', term).b, g(o, i, fe, 'health2', term).se) for o, i, fe in SPECS]
            L.append(lab + ' & ' + ' & '.join(b for b, _ in bs) + r' \\')
            L.append(' & ' + ' & '.join(s for _, s in bs) + r' \\')
        L.append('First-stage F, both regressors (Kleibergen--Paap) & ' + ' & '.join(fnum(g(o, i, fe, 'health2', 'healthy').F_kp, 2) for o, i, fe in SPECS) + r' \\')
        L.append('First-stage F, healthy cells alone & ' + ' & '.join(fnum(g(o, i, fe, 'health_split', 'x', 'healthy').F_kp, 2) for o, i, fe in SPECS) + r' \\')
        L.append('First-stage F, unhealthy cells alone & ' + ' & '.join(fnum(g(o, i, fe, 'health_split', 'x', 'unhealthy').F_kp, 2) for o, i, fe in SPECS) + r' \\')
        for grp in ('healthy', 'unhealthy'):
            vals = []
            for o, i, fe in SPECS:
                x = ar[(ar.window == str(E)) & (ar.outcome == o) & (ar['sample'] == grp)]
                vals.append(f'{x.archi2p.iloc[0]:.3f}' if (i == 'linageres' and fe == 'no' and len(x)) else '')
            L.append(f'Anderson--Rubin p-value, slope = 0, {grp} cells & ' + ' & '.join(vals) + r' \\')
        L.append('Cells (modules) & ' + ' & '.join(f"{int(g(o, i, fe, 'health2', 'healthy').N):,} ({int(g(o, i, fe, 'health2', 'healthy').nmod)})" for o, i, fe in SPECS) + r' \\')
        L.append(r'\midrule')
    (TAB / 'table5_health.tex').write_text('\n'.join(L + FOOT6) + '\n')


def health3():
    L = list(HEAD6)
    for pi, E in enumerate([2015, 2020]):
        g = getter(res(E))
        L.append(r'\multicolumn{7}{l}{\textit{Panel ' + 'AB'[pi] + ': ' + WIN[E] + r'}} \\')
        for lab, term in [('Slope, score 0--3', 'hi_0_3'), ('Slope, score 4--5', 'hi_4_5'), ('Slope, score 6--10', 'hi_6_10')]:
            bs = [fmt(g(o, i, fe, 'health3', term).b, g(o, i, fe, 'health3', term).se) for o, i, fe in SPECS]
            L.append(lab + ' & ' + ' & '.join(b for b, _ in bs) + r' \\')
            L.append(' & ' + ' & '.join(s for _, s in bs) + r' \\')
        L.append('p-value, three slopes equal & ' + ' & '.join(f"{g(o, i, fe, 'health3', 'joint_equal').p:.2f}" for o, i, fe in SPECS) + r' \\')
        L.append('First-stage F, all regressors (Kleibergen--Paap) & ' + ' & '.join(fnum(g(o, i, fe, 'health3', 'hi_0_3').F_kp, 2) for o, i, fe in SPECS) + r' \\')
        for h, lab in [('hi1', '0--3'), ('hi2', '4--5'), ('hi3', '6--10')]:
            L.append(f'First-stage F, score {lab} cells alone & ' + ' & '.join(fnum(g(o, i, fe, 'health3_split', 'x', h).F_kp, 2) for o, i, fe in SPECS) + r' \\')
        L.append('Cells (modules) & ' + ' & '.join(f"{int(g(o, i, fe, 'health3', 'hi_0_3').N):,} ({int(g(o, i, fe, 'health3', 'hi_0_3').nmod)})" for o, i, fe in SPECS) + r' \\')
        L.append(r'\midrule')
    (TAB / 'table5_health3.tex').write_text('\n'.join(L + FOOT6) + '\n')


def decile_inflation(E):
    d = DATA / f'cells_2006_{E}'
    a = pd.read_stata(d / 'analysis_main.dta')
    a['ces'] = 100 * a['RMS_lces_price_index_win']
    a['full'] = 100 * a['RMS_lfullces_pi_win_s57']
    a['h'] = np.where(a['unhealthy'] == 1, 'u', 'h')
    wm = lambda g, c: np.average(g[c], weights=g['RMS_avg_spending'])  # noqa: E731
    rows = []
    for dec in list(range(1, 11)) + ['All']:
        s = a if dec == 'All' else a[a['quality_rank'] == dec]
        row = [str(dec)] + [f'{wm(s[s.h == h], c):.2f}' for c in ['ces', 'full'] for h in ['h', 'u']]
        row += [f"{100 * s[s.h == h]['RMS_spend_new'].sum() / s[s.h == h]['RMS_spend_all'].sum():.1f}" for h in ['h', 'u']]
        rows.append(row)
    sh = {h: a.loc[a.h == h, 'RMS_avg_spending'].sum() / a['RMS_avg_spending'].sum() for h in ['h', 'u']}
    dr = pd.read_stata(d / 'descriptive_regs.dta')
    L = [r'\begin{tabular}{lcccccc}', r'\toprule',
         r' & \multicolumn{2}{c}{Inflation, continuing} & \multicolumn{2}{c}{Inflation with new} & \multicolumn{2}{c}{New products,} \\',
         r' & \multicolumn{2}{c}{products (\%)} & \multicolumn{2}{c}{and exiting products (\%)} & \multicolumn{2}{c}{share of spending (\%)} \\',
         r'\cmidrule(lr){2-3}\cmidrule(lr){4-5}\cmidrule(lr){6-7}',
         r'Price decile & Healthy & Unhealthy & Healthy & Unhealthy & Healthy & Unhealthy \\', r'\midrule',
         r'\multicolumn{7}{l}{\textit{Panel A: means, weighted by spending}} \\']
    L += [' & '.join(row) + r' \\' for row in rows]
    L.append('Share of spending in the cells & ' + ' & '.join(f"{100 * sh[h]:.0f}" for _ in range(3) for h in ['h', 'u']) + r' \\')
    L += [r'\midrule', r' & Healthy & Unhealthy & Healthy & Unhealthy & Healthy & Unhealthy \\',
          r' &  & minus healthy &  & minus healthy &  & minus healthy \\', r'\midrule',
          r'\multicolumn{7}{l}{\textit{Panel B: difference from decile 1 within module (module fixed effects)}} \\']
    for k in range(2, 11):
        b1, b2 = [str(k)], ['']
        for o in ['ces', 'full', 'new']:
            for term in ['healthy', 'difference']:
                x = dr[(dr.outcome == o) & (dr.term == term) & (dr.decile == k)].iloc[0]
                v, s_ = fmt(x.b, x.se, 2)
                b1.append(v); b2.append(s_)
        L += [' & '.join(b1) + r' \\', ' & '.join(b2) + r' \\']
    b1, b2 = ['One decile higher (linear)'], ['']
    for o in ['ces', 'full', 'new']:
        for term in ['slope_healthy', 'slope_diff']:
            x = dr[(dr.outcome == o) & (dr.term == term)].iloc[0]
            v, s_ = fmt(x.b, x.se, 3)
            b1.append(v); b2.append(s_)
    L += [' & '.join(b1) + r' \\', ' & '.join(b2) + r' \\']
    L.append('p-value, no difference at any decile & ' + ' & '.join(
        ['', f"{dr[(dr.outcome == o) & (dr.term == 'joint_diff')].p.iloc[0]:.2f}"][j] for o in ['ces', 'full', 'new'] for j in (0, 1)) + r' \\')
    n = dr[dr.term == 'N'].iloc[0]
    L += [f"Cells (modules) & \\multicolumn{{6}}{{c}}{{{int(n.b):,} ({int(n.se)})}} \\\\", r'\bottomrule', r'\end{tabular}']
    (TAB / f'decile_inflation_health_{E}.tex').write_text('\n'.join(L) + '\n')


table5()
health()
if EXTRA:
    health3()
    for E in (2015, 2020):
        decile_inflation(E)
print('tables written to', TAB)
