"""One table of everything tried in the what-moves review (0_main.tex, 2026-11-05).

Each row is one comparison; the numbers are read from the result files, none are typed here. Columns: HEI points, sugar in grams per
1,000 kcal, ultra-processed share of calories in percentage points, calories per adult equivalent per day. A blank cell means the
design was not run for that outcome, or (family changes, income IV on all households) that calories per adult equivalent there mostly
track the change in the roster that divides them. A dagger marks an estimate whose own test on the period before the event has p < 0.05.

Reads interim/what_moves/ (iv_suboutcomes, iv_suboutcomes_same_size, soda_tax_summary, local_price_regs_county, local_price_timing, policy_shock_coefs,
policy_shock_pretests, time_events_summary, family_events_summary, diagnosis_events_summary), interim/movers/ (county and retailer
summaries) and tabs/what_moves/dynamics_variance.tex (the scale rows).
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
from utils.hei_units import to_sd, hei_sd, hei_mean_sd

import numpy as np
import pandas as pd
from scipy.stats import norm

WM = idir / 'what_moves'
MV = idir / 'movers'
OUTCOMES = ['hei', 'sugar', 'upf', 'kcal']
DEC = {'hei': 3, 'sugar': 2, 'upf': 2, 'kcal': 1}
DAGGER = r'$^{\dagger}$'


def two_sided(b, se):
    return 2 * (1 - norm.cdf(abs(b / se)))


def cell(x, out, dec=None):
    """x = (b, se) or (b, se, p)."""
    b, se = x[0], x[1]
    p = x[2] if len(x) > 2 else np.nan
    if pd.isna(b) or pd.isna(se):
        return ''
    d = DEC[out] if dec is None else dec
    pz = two_sided(b, se)
    st = '***' if pz < 0.01 else '**' if pz < 0.05 else '*' if pz < 0.10 else ''
    return f'{b:.{d}f}' + (f'$^{{{st}}}$' if st else '') + f' ({se:.{d}f})' + (DAGGER if not pd.isna(p) and p < 0.05 else '')


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
    keys = {'hei': 'HEI (SD)', 'sugar': 'Sugar, g per 1,000 kcal (USDA labels)', 'upf': 'Ultra-processed share of calories', 'kcal': 'Calories per adult equivalent per day'}
    vals = {}
    for k, lab in keys.items():
        line = [l for l in txt.splitlines() if l.startswith(lab)][0]
        c = [x.strip().replace(',', '') for x in line.split('&')]
        vals[k] = (float(c[1]), float(c[2]), float(c[3]))
    fmt = lambda v, k: f'{v:,.0f}' if k == 'kcal' else f'{v:.2f}' if k == 'hei' else f'{v:.1f}'
    # HEI is standardized to mean 0 (projection-weighted household-years); the other means are the dynamics table's
    return [('Mean', {k: ('0' if k == 'hei' else fmt(vals[k][0], k)) for k in OUTCOMES}),
            ('Standard deviation of household means', {k: fmt(vals[k][1], k) for k in OUTCOMES}),
            ('Standard deviation of one-year changes', {k: fmt(vals[k][2], k) for k in OUTCOMES})]


def main():
    rows = []
    sd = hei_sd()
    def add(panel, label, vals, sign=1.0):
        # HEI in standard deviations, the paper's unit, except the shares of a gap in panel F, which have no unit
        share = panel.startswith('F.')
        def one(k):
            b, se = sign * vals[k][0], vals[k][1]
            if k == 'hei' and not share:
                b, se = b / sd, se / sd
            return cell((b, se) + tuple(vals[k][2:]), k, 2 if (k == 'hei' and share) else None)
        rows.append({'panel': panel, 'label': label, **{k: one(k) if k in vals else '' for k in OUTCOMES}})

    for lab, v in scale_rows():
        rows.append({'panel': 'Scale of the outcomes', 'label': lab, **v})

    std = {'hei': 'hei_usda', 'sugar': 'sugar_1000', 'upf': 'upf_share', 'kcal': 'cals_ae_day'}
    nokcal = {k: v for k, v in std.items() if k != 'kcal'}

    # ---- income
    P = r'A. Income (\$10,000 more income)'
    for fname, tag, names in [('iv_suboutcomes.csv', 'employed at $t-1$', nokcal), ('iv_suboutcomes_same_size.csv', 'employed at $t-1$, same household size at $t-1$, $t$ and $t+1$', std)]:
        iv = pd.read_csv(WM / fname)
        pl = iv[iv['window'] == 'placebo'].copy()
        pl['p'] = two_sided(pl['b'], pl['se'])
        iv = iv.merge(pl[['outcome', 'p']], on='outcome', how='left')
        add(P, f'Income IV, change the year after, {tag}', pick(iv, names, 'b', 'se', 'p', window='main'))
        add(P, f'Income IV, purchases $t-3$ to $t-2$, before the income change, {tag} (placebo)', pick(iv, names, 'b', 'se', window='placebo'))

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
    tex = [r'\begin{tabular}{p{0.52\linewidth}cccc}', r'\toprule', r' & HEI & Sugar & Ultra-processed & Calories \\', r' & (SD) & (g per 1,000 kcal) & (\% of calories) & (per adult eq. per day) \\']
    for p, g in d.groupby('panel', sort=False):
        tex += [r'\midrule', rf'\multicolumn{{5}}{{l}}{{\textit{{{p}}}}} \\']
        for _, x in g.iterrows():
            tex.append(f"{x['label']} & {x['hei']} & {x['sugar']} & {x['upf']} & {x['kcal']} \\\\")
    tex += [r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'summary.tex'
    out.write_text('\n'.join(tex) + '\n')
    print(f'saved {out}')


def iv_table():
    """Column 3 of tabs/labor_supply/hi_clean.tex for the parts of the diet: tabs/what_moves/iv_suboutcomes.tex. Standard deviations are the ones 12_ standardizes with."""
    a = pd.read_csv(WM / 'iv_suboutcomes.csv')
    b = pd.read_csv(WM / 'iv_suboutcomes_same_size.csv')
    a, b = a[a['window'] == 'main'].set_index('outcome'), b[b['window'] == 'main'].set_index('outcome')
    x = a.loc['hei_usda']
    print(f"HEI, all households: {x['b'] / x['sd_z']:.4f} ({x['se'] / x['sd_z']:.4f}) SD per $10k, {x['b']:.3f} ({x['se']:.3f}) points, F {x['f']:.0f}, N {int(x['n']):,}, SD {x['sd_z']:.3f}")
    x = a.loc['lhei_usda']
    print(f"Linear HEI, all households: {x['b'] / x['sd_z']:.4f} ({x['se'] / x['sd_z']:.4f}) SD per $10k, {x['b']:.3f} ({x['se']:.3f}) points, F {x['f']:.0f}, N {int(x['n']):,}, SD {x['sd_z']:.3f}")
    cols = [(a, 'sugar_1000', 'Sugar', r'(g per 1,000 kcal)', 1), (a, 'sodium_1000', 'Sodium', r'(mg per 1,000 kcal)', 0), (a, 'upf_share', 'Ultra-processed', r'(\% of calories)', 1),
            (a, 'produce_share', 'Produce', r'(\% of calories)', 1), (a, 'produce_spend_share', 'Produce', r'(\% of spending)', 1), (a, 'soda_spend_share', 'Soda', r'(\% of spending)', 1),
            (a, 'whole_share', 'Whole-grain', r'(\% of bread)', 1), (a, 'cals_ae_day', 'Calories', r'(per adult eq.)', 0), (b, 'cals_ae_day', 'Calories, same size', r'(per adult eq.)', 0)]
    rows = [df.loc[o] for df, o, *_ in cols]
    def star(r):
        pv = two_sided(r['b'], r['se'])
        return '***' if pv < 0.01 else '**' if pv < 0.05 else '*' if pv < 0.10 else ''
    tex = [r'\begin{tabular}{l' + 'c' * len(cols) + '}', r'\toprule',
           ' & ' + ' & '.join(f'({i + 1})' for i in range(len(cols))) + r' \\',
           ' & ' + ' & '.join(c[2] for c in cols) + r' \\',
           ' & ' + ' & '.join(c[3] for c in cols) + r' \\', r'\midrule',
           r'$\Delta$ Income (\$10k) & ' + ' & '.join(f"{r['b'] / r['sd_z']:.3f}" + (f'$^{{{star(r)}}}$' if star(r) else '') for r in rows) + r' \\',
           ' & ' + ' & '.join(f"({r['se'] / r['sd_z']:.3f})" for r in rows) + r' \\', r'\midrule',
           'First-stage F & ' + ' & '.join(f"{r['f']:.0f}" for r in rows) + r' \\',
           'N & ' + ' & '.join(f"{int(r['n']):,}" for r in rows) + r' \\',
           'Mean of the outcome & ' + ' & '.join(f"{r['mean']:,.{c[4]}f}" for r, c in zip(rows, cols)) + r' \\',
           'SD of the outcome & ' + ' & '.join(f"{r['sd_z']:,.{c[4]}f}" for r, c in zip(rows, cols)) + r' \\', r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'iv_suboutcomes.tex'
    out.write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))
    print(f'saved {out}')


if __name__ == '__main__':
    main()
    iv_table()
