"""One table of everything tried in the what-moves review (0_main.tex, 2026-11-05).

Each row is one comparison; the numbers are read from the result files, none are typed here. Columns: HEI points, sugar in grams per
1,000 kcal, ultra-processed share of calories in percentage points, calories per adult equivalent per day. A blank cell means the
design was not run for that outcome, or (family changes, income IV on all households) that calories per adult equivalent there mostly
track the change in the roster that divides them. A dagger marks an estimate whose own test on the period before the event has p < 0.05.

Reads interim/what_moves/ (iv_suboutcomes, iv_suboutcomes_same_size, soda_tax_summary, local_price_regs_county, local_price_timing, policy_shock_coefs,
policy_shock_pretests, time_events_summary, family_events_summary, diagnosis_events_summary), interim/movers/ (income_event_study_summary,
county and retailer summaries) and tabs/what_moves/dynamics_variance.tex (the scale rows).
Output: tabs/what_moves/summary.tex, printed as well.
"""
import re
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import numpy as np
import pandas as pd
from scipy.stats import norm

WM = idir / 'what_moves'
MV = idir / 'movers'
OUTCOMES = ['hei', 'sugar', 'upf', 'kcal']
DEC = {'hei': 2, 'sugar': 2, 'upf': 2, 'kcal': 1}
DAGGER = r'$^{\dagger}$'


def two_sided(b, se):
    return 2 * (1 - norm.cdf(abs(b / se)))


def cell(x, out):
    """x = (b, se) or (b, se, p)."""
    b, se = x[0], x[1]
    p = x[2] if len(x) > 2 else np.nan
    if pd.isna(b) or pd.isna(se):
        return ''
    d = DEC[out]
    return f'{b:.{d}f} ({se:.{d}f})' + (DAGGER if not pd.isna(p) and p < 0.05 else '')


def pick(df, names, b, se, p=None, **filters):
    """names: outcome key -> value of df's outcome column. Returns {outcome key: (b, se, p)}."""
    out = {}
    for k, name in names.items():
        s = df
        for col, val in filters.items():
            s = s[s[col] == val]
        s = s[s['outcome'] == name]
        if len(s):
            out[k] = (float(s[b].iloc[0]), float(s[se].iloc[0]), float(s[p].iloc[0]) if p else np.nan)
    return out


def scale_rows():
    """Mean, SD of household means and SD of annual changes, from the dynamics table."""
    txt = (tdir / 'what_moves' / 'dynamics_variance.tex').read_text()
    keys = {'hei': 'HEI (0-100)', 'sugar': 'Sugar, g per 1,000 kcal (USDA labels)', 'upf': 'Ultra-processed share of calories', 'kcal': 'Calories per adult equivalent per day'}
    vals = {}
    for k, lab in keys.items():
        line = [l for l in txt.splitlines() if l.startswith(lab)][0]
        c = [x.strip().replace(',', '') for x in line.split('&')]
        vals[k] = (float(c[1]), float(c[2]), float(c[3]))
    fmt = lambda v, k: f'{v:,.0f}' if k == 'kcal' else f'{v:.1f}'
    return [('Mean', {k: fmt(vals[k][0], k) for k in OUTCOMES}),
            ('Standard deviation of household means', {k: fmt(vals[k][1], k) for k in OUTCOMES}),
            ('Standard deviation of one-year changes', {k: fmt(vals[k][2], k) for k in OUTCOMES})]


def main():
    rows = []
    def add(panel, label, vals, sign=1.0):
        rows.append({'panel': panel, 'label': label, **{k: cell((sign * vals[k][0],) + tuple(vals[k][1:]), k) if k in vals else '' for k in OUTCOMES}})

    for lab, v in scale_rows():
        rows.append({'panel': 'Scale of the outcomes', 'label': lab, **v})

    std = {'hei': 'hei_usda', 'sugar': 'sugar_1000', 'upf': 'upf_share', 'kcal': 'cals_ae_day'}
    nokcal = {k: v for k, v in std.items() if k != 'kcal'}

    # ---- income
    P = r'A. Income (\$10,000 more income)'
    for fname, tag, names in [('iv_suboutcomes.csv', 'all households', nokcal), ('iv_suboutcomes_same_size.csv', 'household size unchanged', std)]:
        iv = pd.read_csv(WM / fname)
        pl = iv[iv['window'] == 'placebo'].copy()
        pl['p'] = two_sided(pl['b'], pl['se'])
        iv = iv.merge(pl[['outcome', 'p']], on='outcome', how='left')
        add(P, f'Income IV, change the year after, {tag}', pick(iv, names, 'b', 'se', 'p', window='main'))
        add(P, f'Income IV, change over two years, $t-1$ to $t+1$, {tag}', pick(iv, names, 'b', 'se', 'p', window='level'))
    # every change of quintile: outcome per $10k of the quintile income gap over the share of that gap in own income (delta method, estimates treated as independent)
    q = pd.read_csv(MV / 'income_event_study_summary.csv')
    first = q[(q['spec'] == 'income_all') & (q['outcome'] == 'real_income')].iloc[0]
    firsts = q[(q['spec'] == 'income_samesize') & (q['outcome'] == 'real_income')].iloc[0]
    vals = {}
    for k, spec, o, fs in [('hei', 'hei_per_inc_all', 'hei_usda', first), ('sugar', 'per_inc_all', 'sugar_1000', first), ('upf', 'per_inc_all', 'upf_share', first), ('kcal', 'per_inc_samesize', 'cals_ae_day', firsts)]:
        x = q[(q['spec'] == spec) & (q['outcome'] == o)].iloc[0]
        b = x['jump'] / fs['jump']
        se = np.sqrt((x['jump_se'] / fs['jump']) ** 2 + (x['jump'] * fs['jump_se'] / fs['jump'] ** 2) ** 2)
        vals[k] = (b, se, x['p_pretrend'])
    add(P, r'Every change of income quintile, per \$10,000 of own income, no instrument', vals)

    # ---- prices
    P = 'B. Prices'
    s = pd.read_csv(WM / 'soda_tax_summary.csv')
    soda = {'hei': 'hei_usda', 'sugar': 'sugar_per_1000cal_usda', 'upf': 'upf_share'}
    add(P, 'Philadelphia soda tax 2017: taxed households against never-taxed households in the same state', pick(s, soda, 'post', 'post_se', 'p_pre', design='philadelphia', group='treated'))
    add(P, 'Philadelphia soda tax 2017: adjacent untaxed counties against the same households', pick(s, soda, 'post', 'post_se', 'p_pre', design='philadelphia', group='adjacent'))
    r = pd.read_csv(WM / 'local_price_regs_county.csv')
    timing = pd.read_csv(WM / 'local_price_timing.csv')
    lp = {'hei': ('hei_usda', 'rel_hi_ces'), 'sugar': ('sugar_per_1000cal_usda', 'rel_hi_ces'), 'upf': ('upf_share', 'rel_upf_ces'), 'kcal': ('cals_ae_day', 'rel_hi_ces')}
    def price_p(o, p, spec):
        # each row's own test: next year's actual price for the same-household row; for the predicted-price row the diet change before 2008 on
        # the later predicted price and the future predicted prices (build_local_price_timing.do), the smaller p; the later actual price for the decade row
        if spec == 'iv':
            zp = p.replace('rel_', 'z_rel_').replace('_ces', '')
            x = r[(r['outcome'] == o) & (r['price'] == zp) & (r['spec'] == 'placebo_pre_z')]
            lead = timing[(timing['outcome'] == o) & (timing['price'] == zp) & (timing['term'] == 'p_leads')]
            pz = two_sided(float(x['b'].iloc[0]), float(x['se'].iloc[0])) if len(x) else np.nan
            return np.nanmin([pz, float(lead['b'].iloc[0]) if len(lead) else np.nan])
        else:
            x = r[(r['outcome'] == o) & (r['price'] == p) & (r['spec'] == ('dyn_lead' if spec == 'hh_year' else 'placebo_pre_x'))]
        return two_sided(float(x['b'].iloc[0]), float(x['se'].iloc[0])) if len(x) else np.nan
    for spec, lab in [('hh_year', 'same household across years'), ('iv', 'prices predicted from national module prices'), ('longdiff_hh', 'same household, 2008--10 to 2018--20')]:
        vals = {}
        for k, (o, p) in lp.items():
            x = r[(r['outcome'] == o) & (r['price'] == p) & (r['spec'] == spec)]
            if len(x):
                vals[k] = (float(-x['b'].iloc[0] / 10), float(x['se'].iloc[0] / 10), price_p(o, p, spec))
        add(P, f'County relative price 10 log points lower: {lab}', vals)
    ps = pd.read_csv(WM / 'policy_shock_coefs.csv')
    pt = pd.read_csv(WM / 'policy_shock_pretests.csv')
    wic = ps[(ps['design'] == 'wic') & (ps['term'] == 'post')].merge(pt[pt['design'] == 'wic'][['outcome', 'p_pretrend']], on='outcome', how='left')
    add(P, 'WIC food package 2009: households on WIC against low-income households with young children', pick(wic, std, 'b', 'se', 'p_pretrend'))

    # ---- time
    P = 'C. Time at home (mean of the 3 years after, against households whose employment never changes)'
    t = pd.read_csv(WM / 'time_events_summary.csv')
    for ev, lab in [('retirement', 'Retirement'), ('job loss', 'Job loss'), ('hours cut', 'Hours cut'), ('re-employment', 'Re-employment'), ('hours increase', 'Hours increase')]:
        add(P, lab, pick(t, std, 'jump', 'jump_se', 'p_pretrend', spec='main', event=ev))
    cov = ps[(ps['design'] == 'covid_white') & (ps['term'] == '2020')].merge(pt[pt['design'] == 'covid_white'][['outcome', 'p_pretrend']], on='outcome', how='left')
    add(P, 'COVID 2020: white collar against blue collar and service', pick(cov, std, 'b', 'se', 'p_pretrend'))

    # ---- health information
    P = 'D. A new diagnosis in the household (mean of years 1--3 after, against households never reporting it)'
    dg = pd.read_csv(WM / 'diagnosis_events_summary.csv')
    for cond, lab in [('diabetes', 'Type 2 diabetes'), ('hypertension', 'High blood pressure'), ('cholesterol', 'High cholesterol'), ('heart', 'Heart disease'), ('obesity', 'Obesity')]:
        add(P, lab, pick(dg, std, 'post', 'post_se', 'p_pretrend', condition=cond, regime='dated'))

    # ---- family
    P = 'E. Who lives in the household (mean of the 3 years after, against households with no such change)'
    f = pd.read_csv(WM / 'family_events_summary.csv')
    for ev, lab in [('birth', 'A young child arrives'), ('emptynest', 'The last child leaves'), ('partner', 'A second head joins'), ('split', 'A couple separates'), ('widowed', 'A head dies')]:
        add(P, lab, pick(f, nokcal, 'jump', 'jump_se', 'p_pretrend', event=ev))

    # ---- place: share of the gap
    P = 'F. Moving (share of the origin-destination gap closed, not points)'
    for fname, lab in [('county_event_study_summary.csv', 'Moving to another county'), ('retailer_event_study_summary.csv', 'Changing main store')]:
        x = pd.read_csv(MV / fname)
        add(P, lab, pick(x, {'hei': 'hei_usda', 'sugar': 'sugar_1000', 'upf': 'upf_share'}, 'jump', 'jump_se', 'p_pretrend', spec='all'))

    d = pd.DataFrame(rows)
    print(d.to_string(index=False))
    tex = [r'\begin{tabular}{p{0.52\linewidth}cccc}', r'\toprule', r' & HEI & Sugar & Ultra-processed & Calories \\', r' & (points) & (g per 1,000 kcal) & (\% of calories) & (per adult eq. per day) \\']
    for p, g in d.groupby('panel', sort=False):
        tex += [r'\midrule', rf'\multicolumn{{5}}{{l}}{{\textit{{{p}}}}} \\']
        for _, x in g.iterrows():
            tex.append(f"{x['label']} & {x['hei']} & {x['sugar']} & {x['upf']} & {x['kcal']} \\\\")
    tex += [r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'summary.tex'
    out.write_text('\n'.join(tex) + '\n')
    print(f'saved {out}')


def iv_table():
    """The income IV on the parts of the diet: tabs/what_moves/iv_suboutcomes.tex."""
    a = pd.read_csv(WM / 'iv_suboutcomes.csv')
    b = pd.read_csv(WM / 'iv_suboutcomes_same_size.csv')
    labs = [('hei_usda', 'HEI (points)', 2), ('sugar_1000', 'Sugar (g per 1,000 kcal)', 2), ('sodium_1000', 'Sodium (mg per 1,000 kcal)', 1), ('upf_share', r'Ultra-processed share of calories (\%)', 2),
            ('produce_share', r'Produce share of calories (\%)', 2), ('produce_spend_share', r'Produce share of spending (\%)', 2), ('soda_spend_share', r'Soda share of spending (\%)', 2),
            ('whole_share', r'Whole-grain share (\%)', 2), ('cals_ae_day', 'Calories per adult equivalent per day', 1)]
    def c(df, o, w, d):
        x = df[(df['outcome'] == o) & (df['window'] == w)].iloc[0]
        return f"{x['b']:.{d}f} ({x['se']:.{d}f})"
    tex = [r'\begin{tabular}{lrcccccc}', r'\toprule', r' & & \multicolumn{3}{c}{All households} & \multicolumn{3}{c}{Size unchanged} \\', r'\cmidrule(lr){3-5} \cmidrule(lr){6-8}',
           r'Outcome & Mean & $t$ to $t+1$ & $t-1$ to $t+1$ & Before & $t$ to $t+1$ & $t-1$ to $t+1$ & Before \\', r'\midrule']
    for o, lab, d in labs:
        m = a[a['outcome'] == o].iloc[0]['mean']
        mean = f'{m:,.0f}' if d == 1 else f'{m:.1f}'
        cells = [c(df, o, w, d) for df in (a, b) for w in ('main', 'level', 'placebo')]
        tex.append(f'{lab} & {mean} & ' + ' & '.join(cells) + r' \\')
    hei_a, hei_b = a[(a['outcome'] == 'hei_usda') & (a['window'] == 'main')].iloc[0], b[(b['outcome'] == 'hei_usda') & (b['window'] == 'main')].iloc[0]
    tex += [r'\midrule', rf"First-stage F & & \multicolumn{{3}}{{c}}{{{hei_a['f']:.0f}}} & \multicolumn{{3}}{{c}}{{{hei_b['f']:.0f}}} \\",
            rf"Household-years & & \multicolumn{{3}}{{c}}{{{int(hei_a['n']):,}}} & \multicolumn{{3}}{{c}}{{{int(hei_b['n']):,}}} \\", r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'iv_suboutcomes.tex'
    out.write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))
    print(f'saved {out}')


if __name__ == '__main__':
    main()
    iv_table()
