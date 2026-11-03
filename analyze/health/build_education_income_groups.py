"""
build_education_income_groups.py -- who are the high-income households whose heads have 12 or
fewer years of schooling, and the low-income households whose heads have 16 or more? Two tables
for notes/obesity.tex, on the sample of figure_hei_gradients_income_education.py (household-years
of geographic non-movers 2004-2020 with an employed or not-employed occupation code, USDA-label
HEI, income inside the pooled 1st-99th percentiles; projection-weighted throughout).

Table 1 (tabs/health/education_income_groups.tex): weighted means and shares for four groups --
education bin (12 or less / 16+) x within-bin income quartile (bottom / top). Quartile cut points
are projection-weighted quantiles of real income within each education bin on the pooled sample.
Household rows (HEI, income, age, composition, employment, occupation bin, race, region, residence)
and area rows: the projection-weighted mean income, share with 16+ years of schooling and mean HEI
of the OTHER panel households in the same county and in the same 5-digit ZIP, pooled over years,
leaving out every year of the household itself.

Table 2 (tabs/health/education_income_gradient_fe.tex): the within-education-bin HEI-income
gradient (HEI points per $10k) under successive specifications: (1) age-bin, kids and year dummies
(reproduces the slopes of the figure script); (2) + county FE; (3) + ZIP FE; (4) + county-by-year
FE; (5) = (1) + the leave-out county mean HEI as a control. Fixed effects are absorbed by weighted
within-group demeaning of the outcome, income and the control dummies; statsmodels WLS, standard
errors clustered on household.

Both tables are printed to stdout; an include block for each is appended once to
tabs/claude/claude_tab.tex.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *  # noqa: E402,F401,F403

import numpy as np                      # noqa: E402
import pandas as pd                     # noqa: E402
import statsmodels.api as sm            # noqa: E402

OUT = tdir / 'health'
CLAUDE_TAB = tdir / 'claude' / 'claude_tab.tex'
W = 'projection_factor'
EDU = {2: '12 years or less', 3: '13--15 years', 4: '16+ years'}      # educ_bin 1 and 2 merged
OCC_BIN = {1: 'white', 2: 'white', 3: 'white', 4: 'white', 5: 'blue', 6: 'blue', 11: 'blue',
           8: 'service', 9: 'service', 12: 'notemp'}
REGION = {1: 'Northeast', 2: 'Northeast', 3: 'Midwest', 4: 'Midwest',
          5: 'South', 6: 'South', 7: 'South', 8: 'West', 9: 'West'}     # nine census divisions -> four regions
GROUPS = [(2, 1), (2, 4), (4, 1), (4, 4)]                               # (education bin, income quartile)


# ----------------------------------------------------------------------------- helpers
def wmean(x, w):
    m = x.notna().values
    return np.average(x.values[m], weights=w.values[m])


def wquantile(x, w, qs):
    o = np.argsort(x)
    x, w = x[o], w[o]
    cw = np.cumsum(w) / w.sum()
    return [x[np.searchsorted(cw, q)] for q in qs]


def leave_out_mean(df, area, var):
    """Weighted mean of `var` over the other households in the same `area`, pooled over years,
    excluding every household-year of the household itself. Missing when no other household."""
    d = df[[area, 'hhid', var, W]].dropna(subset=[var]).copy()
    d['_wx'] = d[var] * d[W]
    A = d.groupby(area)[['_wx', W]].sum()
    H = d.groupby([area, 'hhid'])[['_wx', W]].sum()
    A = A.reindex(H.index.get_level_values(area)).set_index(H.index)
    num, den = A['_wx'] - H['_wx'], A[W] - H[W]
    lo = (num / den).where(den > 0).rename('lo')
    return df[[area, 'hhid']].join(lo, on=[area, 'hhid'])['lo']


def dummies(df, cols):
    return pd.concat([pd.get_dummies(df[c].astype(int), prefix=c, drop_first=True).astype(float) for c in cols], axis=1)


def absorb(X, group, w):
    """Weighted within-group demeaning of every column of X (DataFrame) by `group`."""
    Xw = X.mul(w, axis=0)
    gs = Xw.groupby(group.values).transform('sum')
    ws = w.groupby(group.values).transform('sum')
    return X - gs.div(ws, axis=0)


def gradient(d, controls, fe=None, extra=None):
    """WLS of HEI on income (per $10k) + control dummies, FE absorbed by demeaning, cluster hhid.
    Returns (beta_income, se_income, N, beta_extra, se_extra)."""
    d = d.reset_index(drop=True)
    w = d[W]
    X = pd.concat([d[['real_income'] + (extra or [])].astype(float), dummies(d, controls)], axis=1)
    y = d['hei_usda'].astype(float)
    if fe is not None:
        X = absorb(X, d[fe], w)
        y = absorb(y.to_frame(), d[fe], w)['hei_usda']
    X = sm.add_constant(X)
    fit = sm.WLS(y, X, weights=w).fit(cov_type='cluster', cov_kwds={'groups': d['hhid'].values})
    be, se = (fit.params[extra[0]], fit.bse[extra[0]]) if extra else (np.nan, np.nan)
    return fit.params['real_income'], fit.bse['real_income'], int(fit.nobs), be, se


# ----------------------------------------------------------------------------- data
def load():
    mv = pd.read_parquet(idir / 'movers' / 'occupation_movers_panel.parquet',
                         columns=['hhid', 'year', 'analysis', 'hei_usda', 'real_income', 'educ_bin', 'educ_yrs',
                                  'age5', 'kids', 'hh_size', 'avg_age_hh_head', W, 'occ', 'county',
                                  'n_heads', 'marital_status'])
    sub = mv[(mv['analysis'] == 1) & mv['hei_usda'].notna() & mv['real_income'].notna() & mv['educ_bin'].notna()].copy()
    p1, p99 = sub['real_income'].quantile([0.01, 0.99])
    sub = sub[sub['real_income'].between(p1, p99)].reset_index(drop=True)
    pan = pd.read_parquet(idir / 'panelists' / 'panelists_all_years.parquet',
                          columns=['household_code', 'panel_year', 'race', 'hispanic_origin', 'type_of_residence',
                                   'region_code', 'hh_employed', 'panelist_zip_code'])
    df = sub.merge(pan, left_on=['hhid', 'year'], right_on=['household_code', 'panel_year'], how='left', validate='1:1')
    assert df['race'].notna().all() and df['region_code'].notna().all() and df['hh_employed'].notna().all()
    print(f'{len(df):,} household-years, {df.hhid.nunique():,} households; income trimmed to [{p1 * 10:.1f}, {p99 * 10:.1f}] $000s')

    nmiss = df['county'].isna().sum()
    df = df[df['county'].notna()].reset_index(drop=True)      # one household (two years) has no county or state
    print(f'  dropped {nmiss} household-years with no county')
    df['educ3'] = df['educ_bin'].clip(lower=2).astype(int)
    df['county'] = df['county'].astype(int)
    df['zip5'] = df['panelist_zip_code'].astype(int).astype(str).str.zfill(5)
    df['county_year'] = df['county'] * 10000 + df['year']
    df['income_k'] = df['real_income'] * 10
    df['edu16'] = (df['educ3'] == 4).astype(float)
    df['age65'] = (df['avg_age_hh_head'] >= 65).astype(float)
    df['haskids'] = (df['kids'] != 9).astype(float)
    df['twoheads'] = (df['n_heads'] == 2).astype(float)
    df['married'] = (df['marital_status'] == 1).astype(float)
    df['anyemp'] = (df['hh_employed'] > 0).astype(float)
    ob = df['occ'].map(OCC_BIN)
    assert ob.notna().all(), 'occupation code outside the analysis bins'
    for b in ['white', 'blue', 'service', 'notemp']:
        df[f'occ_{b}'] = (ob == b).astype(float)
    df['white'] = (df['race'] == 1).astype(float)
    df['hispanic'] = (df['hispanic_origin'] == 1).astype(float)
    reg = df['region_code'].map(REGION)
    assert reg.notna().all()
    for r in ['Northeast', 'Midwest', 'South', 'West']:
        df[f'reg_{r}'] = (reg == r).astype(float)
    df['sfh'] = (df['type_of_residence'] == 1).astype(float).where(df['type_of_residence'].notna())

    # within-education-bin income quartiles: projection-weighted quantiles on the pooled sample
    df['inc_q'] = 2
    for e, g in df.groupby('educ3'):
        q25, q75 = wquantile(g['real_income'].values, g[W].values, [0.25, 0.75])
        df.loc[g.index, 'inc_q'] = np.where(g['real_income'] <= q25, 1, np.where(g['real_income'] > q75, 4, 2))
        sh = [g.loc[df.loc[g.index, 'inc_q'] == q, W].sum() / g[W].sum() for q in (1, 4)]
        print(f'  {EDU[e]}: Q1 = income <= ${q25 * 10:.1f}k (weighted share {sh[0]:.3f}); Q4 = income > ${q75 * 10:.1f}k (share {sh[1]:.3f})')

    # leave-own-household-out area means, pooled over years
    for area, tag in [('county', 'county'), ('zip5', 'zip')]:
        for var, name in [('income_k', 'inc'), ('edu16', 'edu16'), ('hei_usda', 'hei')]:
            df[f'{tag}_{name}_lo'] = leave_out_mean(df, area, var)
    print(f'  county leave-out missing for {df.county_hei_lo.isna().sum():,} household-years; '
          f'ZIP leave-out missing for {df.zip_hei_lo.isna().sum():,} ({df.zip_hei_lo.isna().mean() * 100:.1f}%)')
    return df


# ----------------------------------------------------------------------------- table 1
ROWS = [  # (label, column, kind) kind: 'lvl' = mean to 1 decimal, 'pct' = share x 100 to 1 decimal
    ('HEI (0--100)', 'hei_usda', 'lvl'),
    ('Household income (\\$000s, 2013\\$)', 'income_k', 'lvl'),
    ("Heads' average years of schooling", 'educ_yrs', 'lvl'),
    ('Head age (years)', 'avg_age_hh_head', 'lvl'),
    ('Head age 65+ (\\%)', 'age65', 'pct'),
    ('Household size', 'hh_size', 'lvl'),
    ('Children under 18 present (\\%)', 'haskids', 'pct'),
    ('Two heads (\\%)', 'twoheads', 'pct'),
    ('Married (\\%)', 'married', 'pct'),
    ('Any employed head (\\%)', 'anyemp', 'pct'),
    ('Occupation: white collar (\\%)', 'occ_white', 'pct'),
    ('Occupation: blue collar (\\%)', 'occ_blue', 'pct'),
    ('Occupation: service (\\%)', 'occ_service', 'pct'),
    ('Occupation: not employed (\\%)', 'occ_notemp', 'pct'),
    ('White (\\%)', 'white', 'pct'),
    ('Hispanic (\\%)', 'hispanic', 'pct'),
    ('Region: Northeast (\\%)', 'reg_Northeast', 'pct'),
    ('Region: Midwest (\\%)', 'reg_Midwest', 'pct'),
    ('Region: South (\\%)', 'reg_South', 'pct'),
    ('Region: West (\\%)', 'reg_West', 'pct'),
    ('Single-family house, not condo/co-op (\\%)', 'sfh', 'pct'),
]
AREA_ROWS = [
    ('County: mean income of other households (\\$000s)', 'county_inc_lo', 'lvl'),
    ('County: share of other households with 16+ years (\\%)', 'county_edu16_lo', 'pct'),
    ('County: mean HEI of other households', 'county_hei_lo', 'lvl'),
    ('ZIP: mean income of other households (\\$000s)', 'zip_inc_lo', 'lvl'),
    ('ZIP: share of other households with 16+ years (\\%)', 'zip_edu16_lo', 'pct'),
    ('ZIP: mean HEI of other households', 'zip_hei_lo', 'lvl'),
]


def fmt(v, kind):
    return f'{v * 100:.1f}' if kind == 'pct' else f'{v:.1f}'


def table1(df):
    subs = {g: df[(df['educ3'] == g[0]) & (df['inc_q'] == g[1])] for g in GROUPS}
    num = {}   # numeric values for stdout
    lines = ['\\begin{tabular}{lcccc}', '\\toprule',
             ' & \\multicolumn{2}{c}{12 years or less} & \\multicolumn{2}{c}{16+ years} \\\\',
             '\\cmidrule(lr){2-3} \\cmidrule(lr){4-5}',
             'Within-bin income quartile & Bottom & Top & Bottom & Top \\\\',
             '\\midrule', '\\multicolumn{5}{l}{\\textit{Panel A: Household}} \\\\']
    for label, col, kind in ROWS:
        vals = [wmean(subs[g][col], subs[g][W]) for g in GROUPS]
        num[label] = vals
        lines.append(f'{label} & ' + ' & '.join(fmt(v, kind) for v in vals) + ' \\\\')
    lines += ['\\midrule', '\\multicolumn{5}{l}{\\textit{Panel B: Area, leave-own-household-out means}} \\\\']
    for label, col, kind in AREA_ROWS:
        vals = [wmean(subs[g][col], subs[g][W]) for g in GROUPS]
        num[label] = vals
        lines.append(f'{label} & ' + ' & '.join(fmt(v, kind) for v in vals) + ' \\\\')
    lines.append('\\midrule')
    share = [subs[g][W].sum() / df.loc[df['educ3'] == g[0], W].sum() for g in GROUPS]
    num["Share of the education bin's household-years (%)"] = share
    lines.append("Share of the education bin's household-years (\\%) & " + ' & '.join(f'{s * 100:.1f}' for s in share) + ' \\\\')
    nhy = [len(subs[g]) for g in GROUPS]
    nhh = [subs[g]['hhid'].nunique() for g in GROUPS]
    num['Household-years'] = nhy
    num['Households'] = nhh
    lines.append('Household-years & ' + ' & '.join(f'{n:,}' for n in nhy) + ' \\\\')
    lines.append('Households & ' + ' & '.join(f'{n:,}' for n in nhh) + ' \\\\')
    lines += ['\\bottomrule', '\\end{tabular}']
    tex = '\n'.join(lines) + '\n'
    (OUT / 'education_income_groups.tex').write_text(tex)
    print(f'\nwrote {OUT / "education_income_groups.tex"}\n')
    print(tex)
    out = pd.DataFrame(num, index=['<=12 Q1', '<=12 Q4', '16+ Q1', '16+ Q4']).T
    with pd.option_context('display.width', 200, 'display.float_format', '{:.3f}'.format):
        print(out)
    # all twelve education x quartile cells for the main variables (stdout only)
    print('\nAll education-bin x income-quartile cells (weighted means; quartile 2 = middle two quartiles):')
    cells = []
    for e in (2, 3, 4):
        for q in (1, 2, 4):
            s = df[(df['educ3'] == e) & (df['inc_q'] == q)]
            cells.append({'educ': EDU[e], 'quartile': {1: 'bottom', 2: 'middle', 4: 'top'}[q], 'N': len(s),
                          **{c: wmean(s[c], s[W]) for c in ['hei_usda', 'income_k', 'avg_age_hh_head', 'anyemp', 'occ_white',
                                                              'county_inc_lo', 'county_hei_lo', 'zip_inc_lo', 'zip_hei_lo']}})
    with pd.option_context('display.width', 250, 'display.float_format', '{:.2f}'.format):
        print(pd.DataFrame(cells).to_string(index=False))


# ----------------------------------------------------------------------------- table 2
SPECS = [  # (label, controls, fe, extra)
    ('(1) Age bins, children, year dummies', ['age5', 'kids', 'year'], None, None),
    ('(2) (1) + county fixed effects', ['age5', 'kids', 'year'], 'county', None),
    ('(3) (1) + ZIP fixed effects', ['age5', 'kids', 'year'], 'zip5', None),
    ('(4) (1) + county-by-year fixed effects', ['age5', 'kids'], 'county_year', None),
    ('(5) (1) + county mean HEI of other households', ['age5', 'kids', 'year'], None, ['county_hei_lo']),
]


def table2(df):
    res = {}
    for label, controls, fe, extra in SPECS:
        for e in (2, 3, 4):
            d = df[df['educ3'] == e]
            if extra:
                d = d.dropna(subset=extra)
            res[(label, e)] = gradient(d, controls, fe, extra)
            b, se, n, be, see = res[(label, e)]
            print(f'{label:50s} {EDU[e]:18s} slope {b:.3f} (SE {se:.3f}) N {n:,}' + (f'  county HEI coef {be:.3f} (SE {see:.3f})' if extra else ''))
    lines = ['\\begin{tabular}{lccc}', '\\toprule',
             ' & 12 years or less & 13--15 years & 16+ years \\\\', '\\midrule']
    for label, controls, fe, extra in SPECS:
        r = [res[(label, e)] for e in (2, 3, 4)]
        lines.append(f'{label} & ' + ' & '.join(f'{x[0]:.3f}' for x in r) + ' \\\\')
        lines.append(' & ' + ' & '.join(f'({x[1]:.3f})' for x in r) + ' \\\\')
        if extra:
            lines.append('\\quad Coefficient on county mean HEI of others & ' + ' & '.join(f'{x[3]:.3f}' for x in r) + ' \\\\')
            lines.append(' & ' + ' & '.join(f'({x[4]:.3f})' for x in r) + ' \\\\')
        lines.append('\\quad Household-years & ' + ' & '.join(f'{x[2]:,}' for x in r) + ' \\\\')
        lines.append('\\addlinespace')
    lines += ['\\bottomrule', '\\end{tabular}']
    tex = '\n'.join(lines) + '\n'
    (OUT / 'education_income_gradient_fe.tex').write_text(tex)
    print(f'\nwrote {OUT / "education_income_gradient_fe.tex"}\n')
    print(tex)


# ----------------------------------------------------------------------------- include blocks
BLOCKS = r"""

%% ---- education x income groups (obesity note), 2026-11-03 ----

\begin{table}[htbp]
    \centering
    \textsc{Table} \ref{tab:health_education_income_groups} --- \textsc{Who are the low- and high-income households within an education bin?}
    \vspace{.2cm}
    \\
    \maxsizebox*{1.2\linewidth}{1\textheight}{%
    \begin{threeparttable}
        \input{tabs/health/education_income_groups}
        \caption{\footnotesize Projection-weighted means and shares of household-years in four groups: households whose heads average 12 or fewer years of schooling (columns 1--2) or 16 or more (columns 3--4), in the bottom or top quartile of household income within their education bin. Quartile cut points are projection-weighted quantiles of real income (2013 dollars) within each bin on the pooled 2004--2020 sample, so bracket mass points make the quartile shares differ from 25 percent (share row). Sample: geographic non-movers with an employed or not-employed occupation code, HEI from USDA labels, income inside the pooled 1st--99th percentiles. Occupation bins by Nielsen code: white collar 1--4, blue collar 5, 6, 11, service 8, 9. Single-family house is Nielsen residence code 1 (one-family house, not a condo or co-op). Panel B: projection-weighted means over the other panel households in the same county or 5-digit ZIP, pooled over years, excluding every year of the household itself; ZIP rows omit households that are the only panel household in their ZIP. \textit{Source:} Nielsen Consumer Panel 2004--2020; USDA FoodData Central labels.}
    \label{tab:health_education_income_groups}
    \end{threeparttable}
    }
\end{table}

\begin{table}[htbp]
    \centering
    \textsc{Table} \ref{tab:health_education_income_gradient_fe} --- \textsc{The within-education income gradient of HEI with place fixed effects}
    \vspace{.2cm}
    \\
    \maxsizebox*{1.2\linewidth}{1\textheight}{%
    \begin{threeparttable}
        \input{tabs/health/education_income_gradient_fe}
        \caption{\footnotesize Coefficient on household income (HEI points per \$10,000, 2013 dollars) from projection-weighted regressions of the USDA-label HEI on income, estimated separately within each education bin (heads' average years of schooling) on the sample of Table~\ref{tab:health_education_income_groups}. Row (1) controls for 5-year head-age bins, age-and-presence-of-children categories and year dummies; rows (2)--(4) add county, 5-digit ZIP or county-by-year fixed effects, absorbed by weighted within-group demeaning of the outcome, income and the control dummies (year dummies dropped in (4)); row (5) adds to (1) the mean HEI of the other panel households in the same county (pooled over years, excluding every year of the household itself) and also reports its coefficient. Standard errors clustered on household in parentheses; household-years below each specification. \textit{Source:} Nielsen Consumer Panel 2004--2020; USDA FoodData Central labels.}
    \label{tab:health_education_income_gradient_fe}
    \end{threeparttable}
    }
\end{table}
"""


def append_blocks():
    cur = CLAUDE_TAB.read_text()
    if 'tab:health_education_income_groups' in cur:
        print(f'include blocks already in {CLAUDE_TAB}; not appended again')
        return
    with open(CLAUDE_TAB, 'a') as f:
        f.write(BLOCKS)
    print(f'appended two include blocks to {CLAUDE_TAB}')


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    df = load()
    table1(df)
    table2(df)
    append_blocks()


if __name__ == '__main__':
    main()
