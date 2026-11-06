"""
build_upf_consumers.py -- do households that buy more ultra-processed food buy more calories,
and who are they (people and places)? For notes/obesity.tex. "Ultra-processed" = NOVA group 4
of clean/usda/build_usda_nova.py (ingredient-list markers); wording changed from "NOVA 4 share"
to "ultra-processed share" throughout on 2026-11-04.

Data and sample as in figure_nutrition_over_time.py: interim/health/nova_calorie_panel.parquet
(build/health/build_nova_calorie_panel.py) merged to interim/panel_dataset/panel_hh_year.parquet,
hei_usda_panel.parquet and interim/panelists/panelists_all_years.parquet; household-years
2004-2020 with a complete roster, real income trimmed at the pooled 1st/99th percentiles,
calories per adult equivalent trimmed at the 1st/99th percentiles. The ultra-processed share is
the household's share of calories in NOVA group 4 (products without a USDA ingredient list take the group of
their relatives, build/health/build_upc_dictionary.py); household-years with fewer than 10% of calories in a group are dropped.

Figure figs/health/diet/upf_consumers.png:
  left   binscatter (20 bins) of daily calories per adult equivalent on the
         ultra-processed share, both residualized on year dummies (means added back), projection-weighted;
         slope from the household-level WLS, SE clustered on household
  middle the same for HEI (USDA labels)
  right  the adjusted difference in the ultra-processed share (percentage points, 95% CI) between each
         group and its base group, from one WLS of the share on all group dummies and year
         dummies, clustered on household
Table tabs/health/upf_consumers.tex (two side-by-side panels so it fits a page): for each group,
the projection-weighted mean ultra-processed share, the adjusted difference and its SE, and the
number of household-years. Area groups are quartiles
of the county's leave-own-household-out projection-weighted mean real income, share of
households with 16+ years of schooling, and mean HEI, over all panel household-years 2004-2020.
Also printed: the share of the variance of the ultra-processed share (net of year) explained by county
dummies, by ZIP dummies, and by the household dummies.
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
import statsmodels.api as sm
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

PD_ = idir / 'panel_dataset'
FIG = fdir / 'health' / 'diet'
TAB = tdir / 'health'
FIG.mkdir(parents=True, exist_ok=True); TAB.mkdir(parents=True, exist_ok=True)
BLUE, RED, GREY = '#2c5f8a', '#a63d40', '#7a7a7a'
YEARS = list(range(2004, 2021))
plt.rcParams.update({'font.size': 12, 'axes.labelsize': 13, 'axes.titlesize': 12.5, 'legend.fontsize': 10,
                     'axes.spines.top': False, 'axes.spines.right': False})
RACE = {1: 'White', 2: 'Black', 3: 'Asian', 4: 'Other race'}
# census region from the state FIPS code (region_code in the panelist file mixes regions and divisions across years)
REGION = {**{f: 'Northeast' for f in (9, 23, 25, 33, 44, 50, 34, 36, 42)}, **{f: 'Midwest' for f in (17, 18, 26, 39, 55, 19, 20, 27, 29, 31, 38, 46)},
          **{f: 'South' for f in (10, 11, 12, 13, 24, 37, 45, 51, 54, 1, 21, 28, 47, 5, 22, 40, 48)}, **{f: 'West' for f in (4, 8, 16, 30, 32, 35, 49, 56, 2, 6, 15, 41, 53)}}
RESID = {1: 'Single-family house', 2: 'Single-family house', 3: 'Multi-family building', 4: 'Multi-family building',
         5: 'Multi-family building', 6: 'Multi-family building', 7: 'Mobile home'}


def wmean(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok]) if ok.any() else np.nan


def leave_out_mean(df, group, col, w='projection_factor'):
    """Projection-weighted mean of col within group, excluding the household's own rows."""
    d = df[[group, col, w, 'household_code']].dropna()
    d['wx'] = d[col] * d[w]
    g = d.groupby(group).agg(swx=('wx', 'sum'), sw=(w, 'sum'))
    h = d.groupby([group, 'household_code']).agg(hwx=('wx', 'sum'), hw=(w, 'sum')).reset_index()
    h = h.merge(g, left_on=group, right_index=True)
    h['lo'] = (h['swx'] - h['hwx']) / (h['sw'] - h['hw']).replace(0, np.nan)
    return h[[group, 'household_code', 'lo']]


def quartile_labels(s, name):
    q = pd.qcut(s.rank(method='first'), 4, labels=[f'{name}: bottom quartile', f'{name}: second', f'{name}: third', f'{name}: top quartile'])
    return q.astype(object)


def load():
    hh = pd.read_parquet(PD_ / 'panel_hh_year.parquet',
                         columns=['household_code', 'panel_year', 'real_income', 'projection_factor', 'avg_age_hh_head', 'household_size', 'zip_code'])
    hh = hh[hh['panel_year'].isin(YEARS)].dropna(subset=['real_income', 'projection_factor'])
    p1, p99 = hh['real_income'].quantile([0.01, 0.99])
    hh = hh[hh['real_income'].between(p1, p99)]
    cal = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet')
    hei = pd.read_parquet(PD_ / 'hei_usda_panel.parquet', columns=['household_code', 'panel_year', 'hei_usda'])
    pan = pd.read_parquet(idir / 'panelists' / 'panelists_all_years.parquet',
                          columns=['household_code', 'panel_year', 'hh_avg_yrsofschool', 'race', 'hispanic_origin', 'region_code',
                                   'type_of_residence', 'fips_state_code', 'fips_county_code', 'age_and_presence_of_children', 'hh_employed'])
    df = hh.merge(cal, on=['household_code', 'panel_year'], how='inner').merge(hei, on=['household_code', 'panel_year'], how='left') \
           .merge(pan, on=['household_code', 'panel_year'], how='left')
    df = df[df['ae_complete'] == 1]
    df['cals_ae_day'] = df['cals_all'] / 365 / df['ae_total']
    c1, c99 = df['cals_ae_day'].quantile([0.01, 0.99])
    df = df[df['cals_ae_day'].between(c1, c99)]
    nova_cals = df[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
    df['share_nova4'] = 100 * df['cals_nova4'] / nova_cals.replace(0, np.nan)
    df['nova_cover'] = nova_cals / df['cals_all'].replace(0, np.nan)
    df = df[df['nova_cover'] >= 0.10].copy()
    df['county'] = df['fips_state_code'].astype('Int64').astype(str).str.zfill(2) + df['fips_county_code'].astype('Int64').astype(str).str.zfill(3)
    df['zip'] = df['zip_code'].astype('Int64').astype(str).str.zfill(5)
    df['college'] = (df['hh_avg_yrsofschool'] >= 16).astype(float)
    # groups
    df['g_income'] = df.groupby('panel_year')['real_income'].transform(lambda s: quartile_labels(s, 'Income'))
    df['g_educ'] = pd.cut(df['hh_avg_yrsofschool'], [0, 12, 15.99, 30], labels=['Education: 12 years or less', 'Education: 13-15 years', 'Education: 16+ years']).astype(object)
    df['g_age'] = pd.cut(df['avg_age_hh_head'], [0, 35, 45, 55, 65, 120], labels=['Head age: under 35', 'Head age: 35-44', 'Head age: 45-54', 'Head age: 55-64', 'Head age: 65+']).astype(object)
    df['g_kids'] = np.where(df['age_and_presence_of_children'] == 9, 'No children under 18', 'Children under 18')
    df['g_size'] = pd.cut(df['household_size'], [0, 1, 2, 20], labels=['Household size: 1', 'Household size: 2', 'Household size: 3+']).astype(object)
    df['g_race'] = df['race'].map(RACE)
    df['g_hisp'] = np.where(df['hispanic_origin'] == 1, 'Hispanic', 'Not Hispanic')
    df['g_region'] = df['fips_state_code'].astype('Int64').map(REGION)
    df['g_resid'] = df['type_of_residence'].map(RESID)
    df['g_emp'] = np.where(df['hh_employed'] == 1, 'Any head employed', 'No head employed')
    # area groups: leave-out county means over the sample
    for col, name in [('real_income', 'County mean income'), ('college', 'County share college'), ('hei_usda', 'County mean HEI')]:
        lo = leave_out_mean(df, 'county', col).rename(columns={'lo': f'lo_{col}'})
        df = df.merge(lo[['county', 'household_code', f'lo_{col}']], on=['county', 'household_code'], how='left')
        df[f'g_cty_{col}'] = quartile_labels(df[f'lo_{col}'], name)
        df.loc[df[f'lo_{col}'].isna(), f'g_cty_{col}'] = np.nan
    print(f'{len(df):,} household-years, {df.household_code.nunique():,} households, {df.county.nunique():,} counties')
    print('missing share by group variable:', df[GROUPS].isna().mean().round(3).to_dict())
    return df


def resid_year(df, col, w):
    """Residualize on year dummies (weighted), add the weighted mean back."""
    Y = pd.get_dummies(df['panel_year'], drop_first=True, dtype=float).values
    X = np.column_stack([np.ones(len(df)), Y])
    y = df[col].values.astype(float)
    sw = np.sqrt(w)
    beta = np.linalg.lstsq(X * sw[:, None], y * sw, rcond=None)[0]
    return y - X @ beta + np.average(y, weights=w)


def binscatter_fit(df, ycol, nq=20):
    d = df.dropna(subset=[ycol, 'share_nova4']).reset_index(drop=True)
    w = d['projection_factor'].values
    y = resid_year(d, ycol, w); x = resid_year(d, 'share_nova4', w)
    edges = np.percentile(x, np.linspace(0, 100, nq + 1)); edges[-1] += 1
    b = np.digitize(x, edges)
    pts = pd.DataFrame([(np.average(x[b == i], weights=w[b == i]), np.average(y[b == i], weights=w[b == i])) for i in range(1, nq + 1)], columns=['x', 'y'])
    fit = sm.WLS(y, sm.add_constant(x), weights=w).fit(cov_type='cluster', cov_kwds={'groups': d['household_code'].values})
    return pts, fit.params[1], fit.bse[1], fit.params[0], len(d)


GROUPS = ['g_income', 'g_educ', 'g_age', 'g_kids', 'g_size', 'g_race', 'g_hisp', 'g_emp', 'g_region', 'g_resid',
          'g_cty_real_income', 'g_cty_college', 'g_cty_hei_usda']
BASE_FIRST = {'g_income': 'Income: bottom quartile', 'g_educ': 'Education: 12 years or less', 'g_age': 'Head age: under 35',
              'g_kids': 'No children under 18', 'g_size': 'Household size: 1', 'g_race': 'White', 'g_hisp': 'Not Hispanic',
              'g_emp': 'No head employed', 'g_region': 'Northeast', 'g_resid': 'Single-family house',
              'g_cty_real_income': 'County mean income: bottom quartile', 'g_cty_college': 'County share college: bottom quartile',
              'g_cty_hei_usda': 'County mean HEI: bottom quartile'}
ORDER = {'g_income': ['Income: bottom quartile', 'Income: second', 'Income: third', 'Income: top quartile'],
         'g_educ': ['Education: 12 years or less', 'Education: 13-15 years', 'Education: 16+ years'],
         'g_age': ['Head age: under 35', 'Head age: 35-44', 'Head age: 45-54', 'Head age: 55-64', 'Head age: 65+'],
         'g_kids': ['No children under 18', 'Children under 18'], 'g_size': ['Household size: 1', 'Household size: 2', 'Household size: 3+'],
         'g_race': ['White', 'Black', 'Asian', 'Other race'], 'g_hisp': ['Not Hispanic', 'Hispanic'],
         'g_emp': ['No head employed', 'Any head employed'], 'g_region': ['Northeast', 'Midwest', 'South', 'West'],
         'g_resid': ['Single-family house', 'Multi-family building', 'Mobile home'],
         'g_cty_real_income': ['County mean income: bottom quartile', 'County mean income: second', 'County mean income: third', 'County mean income: top quartile'],
         'g_cty_college': ['County share college: bottom quartile', 'County share college: second', 'County share college: third', 'County share college: top quartile'],
         'g_cty_hei_usda': ['County mean HEI: bottom quartile', 'County mean HEI: second', 'County mean HEI: third', 'County mean HEI: top quartile']}


def who(df):
    d = df.dropna(subset=GROUPS + ['share_nova4']).reset_index(drop=True)
    w = d['projection_factor'].values
    cols, names = [], []
    for g in GROUPS:
        for lev in ORDER[g][1:]:
            cols.append((d[g] == lev).astype(float).values); names.append(lev)
    Y = pd.get_dummies(d['panel_year'], drop_first=True, dtype=float)
    X = np.column_stack([np.ones(len(d))] + cols + [Y.values])
    fit = sm.WLS(d['share_nova4'].values, X, weights=w).fit(cov_type='cluster', cov_kwds={'groups': d['household_code'].values})
    coef = pd.Series(fit.params[1:1 + len(names)], index=names); se = pd.Series(fit.bse[1:1 + len(names)], index=names)
    rows = []
    for g in GROUPS:
        for lev in ORDER[g]:
            m = d[g] == lev
            rows.append({'group': g, 'level': lev, 'mean': wmean(d.loc[m, 'share_nova4'], d.loc[m, 'projection_factor']),
                         'adj': 0.0 if lev == ORDER[g][0] else coef[lev], 'se': np.nan if lev == ORDER[g][0] else se[lev], 'n': int(m.sum())})
    tab = pd.DataFrame(rows)
    print(f'who regression: n = {len(d):,}, R2 = {fit.rsquared:.3f}')
    return tab


def variance_shares(df):
    d = df.dropna(subset=['share_nova4', 'county', 'zip']).reset_index(drop=True)
    w = d['projection_factor'].values
    y = resid_year(d, 'share_nova4', w)
    tot = np.average((y - np.average(y, weights=w)) ** 2, weights=w)
    out = {}
    for name, key in [('county', 'county'), ('zip', 'zip'), ('household', 'household_code')]:
        gm = pd.Series(y * w).groupby(d[key].values).sum() / pd.Series(w).groupby(d[key].values).sum()
        r = y - gm.reindex(d[key].values).values
        out[name] = 1 - np.average(r ** 2, weights=w) / tot
    return out


def main():
    df = load()
    print('ultra-processed share of calories: weighted mean %.1f, sd %.1f; p10 %.1f p90 %.1f' % (
        wmean(df.share_nova4, df.projection_factor), np.sqrt(np.average((df.share_nova4 - wmean(df.share_nova4, df.projection_factor)) ** 2, weights=df.projection_factor)),
        df.share_nova4.quantile(0.1), df.share_nova4.quantile(0.9)))

    fig, axes = plt.subplots(1, 3, figsize=(22, 7), gridspec_kw={'width_ratios': [1, 1, 1.25]})
    for ax, ycol, ylab, title in [(axes[0], 'cals_ae_day', 'Calories per adult equivalent per day', 'Calories bought'),
                                  (axes[1], 'hei_usda', 'HEI (0-100)', 'Diet quality')]:
        pts, b, se, a, n = binscatter_fit(df, ycol)
        ax.scatter(pts['x'], pts['y'], s=55, color=RED, zorder=4, edgecolors='white', linewidth=0.5)
        xx = np.linspace(pts['x'].min(), pts['x'].max(), 10); ax.plot(xx, a + b * xx, color=GREY, lw=1.5)
        ax.set_xlabel('Ultra-processed share of calories (%), net of year'); ax.set_ylabel(ylab)
        ax.set_title(f'{title}: {10 * b:+.2f} per 10 points of ultra-processed share (SE {10 * se:.2f})')
        print(f'{ycol} on ultra-processed share: slope per 10 pts {10 * b:.3f} (SE {10 * se:.3f}), n = {n:,}; bins {pts.y.iloc[0]:.1f} -> {pts.y.iloc[-1]:.1f}')
    tab = who(df)
    ax = axes[2]
    show = tab[tab['group'].isin(['g_income', 'g_educ', 'g_age', 'g_cty_real_income', 'g_cty_college'])].reset_index(drop=True)
    ypos = np.arange(len(show))[::-1]
    ax.errorbar(show['adj'], ypos, xerr=1.96 * show['se'].fillna(0), fmt='o', color=BLUE, ecolor=BLUE, capsize=2, ms=5)
    ax.axvline(0, color=GREY, lw=1)
    ax.set_yticks(ypos); ax.set_yticklabels(show['level'], fontsize=9.5)
    ax.set_xlabel('Adjusted difference in the ultra-processed share, percentage points (95% CI)')
    ax.set_title('Who buys more ultra-processed calories (all groups and year dummies held fixed)')
    fig.tight_layout()
    fig.savefig(FIG / 'upf_consumers.png', bbox_inches='tight', dpi=150); plt.close(fig)
    print(f'saved {FIG / "upf_consumers.png"}')

    vs = variance_shares(df)
    print('share of the variance of the ultra-processed share (net of year) explained by dummies:', {k: round(v, 3) for k, v in vs.items()})
    print('\n' + tab.round(2).to_string())
    # table: two side-by-side panels (left: household groups; right: employment, place and county groups)
    def panel_rows(groups):
        rows = []
        for g in groups:
            for _, r in tab[tab['group'] == g].iterrows():
                adj = '(base)' if np.isnan(r['se']) else f"{r['adj']:.2f}"
                se_ = '' if np.isnan(r['se']) else f"({r['se']:.2f})"
                rows.append(f"{r['level']} & {r['mean']:.1f} & {adj} & {se_} & {r['n']:,}")
            rows.append('ADDLINESPACE')
        return rows[:-1]
    left = panel_rows(['g_income', 'g_educ', 'g_age', 'g_kids', 'g_size', 'g_race', 'g_hisp'])
    right = panel_rows(['g_emp', 'g_region', 'g_resid', 'g_cty_real_income', 'g_cty_college', 'g_cty_hei_usda'])
    n = max(len(left), len(right)); left += [''] * (n - len(left)); right += [''] * (n - len(right))
    head = 'Group & Mean & Adj. diff. & SE & HH-years'
    lines = ['\\begin{tabular}{lrrrr@{\\hspace{2em}}lrrrr}', '\\toprule', f'{head} & {head} \\\\', '\\midrule']
    for l, r in zip(left, right):
        l_ = ' & & & & ' if l in ('', 'ADDLINESPACE') else l
        r_ = ' & & & & ' if r in ('', 'ADDLINESPACE') else r
        lines.append(f'{l_} & {r_} \\\\' + ('[0.4em]' if 'ADDLINESPACE' in (l, r) else ''))
    lines += ['\\midrule', f"\\multicolumn{{10}}{{l}}{{Share of the variance of the ultra-processed share (net of year) explained by county dummies {vs['county']:.3f}, "
              f"ZIP dummies {vs['zip']:.3f}, household dummies {vs['household']:.3f}}} \\\\", '\\bottomrule', '\\end{tabular}']
    (TAB / 'upf_consumers.tex').write_text('\n'.join(lines))
    print(f'saved {TAB / "upf_consumers.tex"}')


if __name__ == '__main__':
    main()
