"""How much does nutrition move WITHIN households at all? (Anya's "what moves nutrition" review,
2026-11-05.) Four descriptive exhibits on the household-year panel, 2004-2020, geographic movers
included:

1. VARIANCE AND PERSISTENCE (tabs/what_moves/dynamics_variance.tex): for each outcome, the SD of
   household means (between), the SD of year-to-year changes (within), the correlation of the level
   with its own value 1, 3 and 5 years earlier, the coefficient of the NEXT k-year change on a
   one-year change (beta_k: 0 if changes are permanent, -0.5 if they are pure noise around a fixed
   level), the implied permanent share of a one-year change (1 + 2 beta_1, from a random walk plus
   white noise), and the share of a household's within variance that its own linear trend explains
   (households with 6+ years; the noise benchmark is the same share for a series with no trend).
2. LONG-RUN DRIFT (figs/what_moves/dynamics/dynamics_longrun_drift.png): households observed 10+
   years; change in HEI from the mean of the first two observed years to the mean of the last two,
   its distribution, and its mean by head age at the start, by tercile of the change in real
   income, and by the change in household size.
3. COHORTS (dynamics_cohorts.png): projection-weighted mean HEI by 5-year birth cohort of the
   average head (panel year minus average head age), by panel year and by age; plus the
   within-household age slope (household fixed effects only: age and calendar year are the same
   thing inside a household, so year effects cannot be added).
4. BIG MOVERS (dynamics_big_movers.tex): households with 6+ years whose 3-year mean HEI in the
   last three observed years is 8+ points above (risers) or below (fallers) the first three; what
   else changed for them over the same windows and which HEI components moved.

Exhibits 2 and 3 are drawn for HEI, sugar per 1,000 kcal (Syndigo and USDA labels) and the soda share of grocery
spending (coordinator's addition, 2026-11-05); exhibit 4 also for sugar-defined movers (20+ g). Outcomes: full HEI (hei_usda), the Allcott index (hi_allcott, standardized, winsorized at the
0.5th/99.5th percentiles), sugar (g) and sodium (mg) per 1,000 kcal, produce share of calories and
ultra-processed share of labelled calories (both from the calorie panel, %), calories per adult
equivalent per day (dictionary densities, complete rosters, winsorized at p1/p99).
Inputs: interim/panel_dataset/panel_hh_year.parquet, hei_usda_panel.parquet,
interim/health/nova_calorie_panel.parquet, final/labor_supply_reg_data.dta (kids, heads,
employment, county). Household-level statistics are unweighted; cohort means use projection factors.
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
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

FIG = fdir / 'what_moves' / 'dynamics'
TAB = tdir / 'what_moves'
FIG.mkdir(parents=True, exist_ok=True)
TAB.mkdir(parents=True, exist_ok=True)
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#999999'
plt.rcParams.update({'font.size': 14, 'axes.labelsize': 15, 'axes.titlesize': 15, 'xtick.labelsize': 13,
                     'ytick.labelsize': 13, 'legend.fontsize': 12, 'axes.spines.top': False, 'axes.spines.right': False})

OUTCOMES = [('hei_usda', 'HEI (0-100)'), ('hi_allcott', 'Allcott index (SD units)'), ('sugar_1000', 'Sugar, g per 1,000 kcal'),
            ('sodium_1000', 'Sodium, mg per 1,000 kcal'), ('produce_share', 'Produce share of calories (%)'),
            ('upf_share', 'Ultra-processed share of calories (%)'), ('cals_ae_day', 'Calories per adult equivalent per day'),
            ('sugar_usda_1000', 'Sugar, g per 1,000 kcal (USDA labels)'), ('soda_spend_share', 'Soda share of grocery spending (%)')]
DRIFT_OUTCOMES = [('hei_usda', 'HEI', 'HEI points', ''), ('sugar_1000', 'sugar per 1,000 kcal (Syndigo labels)', 'grams', '_sugar'),
                  ('sugar_usda_1000', 'sugar per 1,000 kcal (USDA labels)', 'grams', '_sugar_usda'),
                  ('soda_spend_share', 'soda share of grocery spending', 'pct. points', '_soda')]
HEI_COMPONENTS = ['hei_c_totfruit', 'hei_c_wholfruit', 'hei_c_totveg', 'hei_c_greensbeans', 'hei_c_wholegrain', 'hei_c_dairy',
                  'hei_c_totprot', 'hei_c_seaplant', 'hei_c_fattyacid', 'hei_c_refgrain', 'hei_c_sodium', 'hei_c_addsugar', 'hei_c_satfat']
COMPONENT_LABELS = {'hei_c_totfruit': 'Total fruit (0-5)', 'hei_c_wholfruit': 'Whole fruit (0-5)', 'hei_c_totveg': 'Total vegetables (0-5)',
                    'hei_c_greensbeans': 'Greens and beans (0-5)', 'hei_c_wholegrain': 'Whole grains (0-10)', 'hei_c_dairy': 'Dairy (0-10)',
                    'hei_c_totprot': 'Total protein (0-5)', 'hei_c_seaplant': 'Seafood and plant protein (0-5)',
                    'hei_c_fattyacid': 'Fatty acids (0-10)', 'hei_c_refgrain': 'Refined grains (0-10)', 'hei_c_sodium': 'Sodium (0-10)',
                    'hei_c_addsugar': 'Added sugars (0-10)', 'hei_c_satfat': 'Saturated fat (0-10)'}


def log(msg):
    print(msg, flush=True)


def load():
    pan = pd.read_parquet(idir / 'panel_dataset' / 'panel_hh_year.parquet',
                          columns=['household_code', 'panel_year', 'hi_allcott', 'sugar_per_1000cal', 'sodium_per_1000cal',
                                   'real_income', 'household_size', 'avg_age_hh_head', 'zip_code', 'projection_factor'])
    hei = pd.read_parquet(idir / 'panel_dataset' / 'hei_usda_panel.parquet', columns=['household_code', 'panel_year', 'hei_usda'] + HEI_COMPONENTS)
    nova = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet',
                           columns=['household_code', 'panel_year', 'cals_all', 'cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4',
                                    'produce_cals_all', 'ae_total', 'ae_complete'])
    reg, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'),
                                 usecols=['hhid', 'year', 'fips', 'kids', 'male_head_employment', 'female_head_employment', 'n_heads',
                                          'sugar_per_1000cal_usda', 'spend_soda', 'spend_total'])
    reg = reg.rename(columns={'hhid': 'household_code', 'year': 'panel_year'})
    reg['household_code'] = reg['household_code'].astype('int64')
    reg['panel_year'] = reg['panel_year'].astype(int)
    df = pan.merge(hei, on=['household_code', 'panel_year'], how='left').merge(nova, on=['household_code', 'panel_year'], how='left') \
            .merge(reg, on=['household_code', 'panel_year'], how='left')
    df['sugar_1000'] = df['sugar_per_1000cal']
    df['sugar_usda_1000'] = df['sugar_per_1000cal_usda']
    df['soda_spend_share'] = 100 * df['spend_soda'] / df['spend_total']
    df['sodium_1000'] = 1000 * df['sodium_per_1000cal']
    df['produce_share'] = 100 * df['produce_cals_all'] / df['cals_all']
    df['upf_share'] = 100 * df['cals_nova4'] / df[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
    df['cals_ae_day'] = np.where(df['ae_complete'] == 1, df['cals_all'] / df['ae_total'] / 365, np.nan)
    lo, hi_ = df['cals_ae_day'].quantile([0.01, 0.99])
    df['cals_ae_day'] = df['cals_ae_day'].clip(lo, hi_)
    lo, hi_ = df['hi_allcott'].quantile([0.005, 0.995])
    df['hi_allcott'] = df['hi_allcott'].clip(lo, hi_)
    df['any_kids'] = (df['kids'] != 9).astype(float).where(df['kids'].notna())
    df['any_head_employed'] = ((df['male_head_employment'].isin([1, 2, 3])) | (df['female_head_employment'].isin([1, 2, 3]))).astype(float)
    df.loc[df['male_head_employment'].isna(), 'any_head_employed'] = np.nan
    df = df.sort_values(['household_code', 'panel_year']).reset_index(drop=True)
    log(f'panel: {len(df):,} household-years, {df.household_code.nunique():,} households')
    return df


# ------------------------------------------------------------------ 1. variance and persistence
def lagged(df, y, k):
    """pairs (y at t, y at t-k) for the same household, both observed."""
    a = df[['household_code', 'panel_year', y]].dropna()
    b = a.copy(); b['panel_year'] = b['panel_year'] + k
    m = a.merge(b, on=['household_code', 'panel_year'], suffixes=('', '_lag'))
    return m[y].values, m[f'{y}_lag'].values


def variance_table(df):
    rows = []
    for y, label in OUTCOMES:
        d = df[['household_code', 'panel_year', y]].dropna()
        g = d.groupby('household_code')[y]
        between = g.mean().std()
        cur, lag1 = lagged(d, y, 1)
        d1 = cur - lag1
        within = d1.std()
        ac = {k: np.corrcoef(*lagged(d, y, k))[0, 1] for k in (1, 3, 5)}
        # beta_k: next k-year change on the one-year change (needs t-1, t, t+k observed)
        base = d.rename(columns={y: 'y0'})
        prev = d.rename(columns={y: 'ym1'}); prev['panel_year'] += 1
        betas = {}
        for k in (1, 2, 3):
            nxt = d.rename(columns={y: 'yk'}); nxt['panel_year'] -= k
            m = base.merge(prev, on=['household_code', 'panel_year']).merge(nxt, on=['household_code', 'panel_year'])
            c0 = (m['y0'] - m['ym1']).values; ck = (m['yk'] - m['y0']).values
            betas[k] = np.cov(c0, ck)[0, 1] / np.var(c0, ddof=1)
        perm = 1 + 2 * betas[1]
        # household linear trend share (6+ years)
        n = g.transform('size'); dd = d[n >= 6].copy()
        gg = dd.groupby('household_code')
        dd['yc'] = dd[y] - gg[y].transform('mean'); dd['tc'] = dd['panel_year'] - gg['panel_year'].transform('mean')
        agg = dd.assign(sxy=dd.yc * dd.tc, sxx=dd.tc ** 2, syy=dd.yc ** 2).groupby('household_code')[['sxy', 'sxx', 'syy']].sum()
        agg['n'] = gg.size()
        trend_share = (agg['sxy'] ** 2 / agg['sxx']).sum() / agg['syy'].sum()
        noise_bench = (agg['syy'] / (agg['n'] - 1)).sum() / agg['syy'].sum()
        rows.append({'outcome': label, 'mean': d[y].mean(), 'between': between, 'within': within,
                     'ac1': ac[1], 'ac3': ac[3], 'ac5': ac[5], 'b1': betas[1], 'b2': betas[2], 'b3': betas[3],
                     'perm': perm, 'trend': trend_share, 'bench': noise_bench, 'n_hh': d.household_code.nunique(), 'n_pairs': len(d1)})
        log(f'{label}: mean {d[y].mean():.2f}, between SD {between:.2f}, within SD of annual change {within:.2f}, '
            f'autocorr 1/3/5 = {ac[1]:.2f}/{ac[3]:.2f}/{ac[5]:.2f}, beta_1/2/3 = {betas[1]:.3f}/{betas[2]:.3f}/{betas[3]:.3f}, '
            f'permanent share {perm:.2f}, trend share {trend_share:.3f} (noise benchmark {noise_bench:.3f})')
    t = pd.DataFrame(rows)
    lines = ['\\begin{tabular}{lrrrrrrrrrrr}', '\\toprule',
             ' & & SD of & SD of & \\multicolumn{3}{c}{Correlation of the level with} & \\multicolumn{2}{c}{Next change on} & Permanent & \\multicolumn{2}{c}{Household trend share} \\\\',
             ' & & household & annual & \\multicolumn{3}{c}{its value $k$ years earlier} & \\multicolumn{2}{c}{a one-year change} & share of a & \\multicolumn{2}{c}{of within variance} \\\\',
             '\\cmidrule(lr){5-7} \\cmidrule(lr){8-9} \\cmidrule(lr){11-12}',
             'Outcome & Mean & means & changes & $k=1$ & $k=3$ & $k=5$ & 1 year & 3 years & change & actual & no-trend benchmark \\\\', '\\midrule']
    for _, r in t.iterrows():
        lines.append(f"{r['outcome']} & {r['mean']:,.1f} & {r['between']:,.2f} & {r['within']:,.2f} & {r['ac1']:.2f} & {r['ac3']:.2f} & {r['ac5']:.2f} & "
                     f"{r['b1']:.2f} & {r['b3']:.2f} & {r['perm']:.2f} & {r['trend']:.2f} & {r['bench']:.2f} \\\\")
    lines += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'dynamics_variance.tex').write_text('\n'.join(lines) + '\n')
    log(f'saved {TAB / "dynamics_variance.tex"}')
    return t


# ------------------------------------------------------------------ 2. long-run drift
def window_means(df, cols, first=True, k=2, key='hei_usda'):
    d = df.dropna(subset=[key]).sort_values(['household_code', 'panel_year'])
    g = d.groupby('household_code')
    rank = g.cumcount() if first else g.cumcount(ascending=False)
    w = d[rank < k].groupby('household_code')[cols].mean()
    w['year'] = d[rank < k].groupby('household_code')['panel_year'].mean()
    return w


def fig_longrun_drift(df, y='hei_usda', yname='HEI', unit='HEI points', suffix=''):
    n = df.dropna(subset=[y]).groupby('household_code').size()
    keep = n[n >= 10].index
    d = df[df.household_code.isin(keep)].rename(columns={y: 'y_'})
    cols = ['y_', 'real_income', 'household_size', 'avg_age_hh_head']
    f = window_means(d, cols, True, key='y_'); l = window_means(d, cols, False, key='y_')
    ch = (l[cols] - f[cols]).rename(columns=lambda c: 'd_' + c)
    ch['age0'] = f['avg_age_hh_head']; ch['years'] = l['year'] - f['year']
    ch['age_grp'] = pd.cut(ch['age0'], [0, 35, 45, 55, 65, 200], labels=['<35', '35-44', '45-54', '55-64', '65+'], right=False)
    ch['inc_terc'] = pd.qcut(ch['d_real_income'], 3, labels=['bottom', 'middle', 'top'])
    ch['size_chg'] = pd.cut(ch['d_household_size'], [-99, -0.5, 0.5, 99], labels=['fell', 'same', 'rose'])
    log(f'long-run drift, {yname}: {len(ch):,} households with 10+ years, mean span {ch.years.mean():.1f} years; '
        f'mean change {ch.d_y_.mean():.2f} (SD {ch.d_y_.std():.2f}); p10/p50/p90 {ch.d_y_.quantile([.1, .5, .9]).round(2).tolist()}')
    fig, axes = plt.subplots(1, 4, figsize=(22, 5.5))
    ax = axes[0]
    lim = 3 * ch['d_y_'].std()
    ax.hist(ch['d_y_'].clip(-lim, lim), bins=50, color=BLUE, alpha=0.85)
    ax.axvline(0, color=GREY, ls='--'); ax.axvline(ch['d_y_'].mean(), color=RED, lw=2)
    ax.set_title(f'Change in {yname}, first two years to last two\n{len(ch):,} households with 10+ years, mean {ch.d_y_.mean():+.2f}, SD {ch.d_y_.std():.1f}')
    ax.set_xlabel(unit); ax.set_ylabel('Households')
    for ax, grp, title, order in [(axes[1], 'age_grp', 'By average head age at the start', ['<35', '35-44', '45-54', '55-64', '65+']),
                                  (axes[2], 'inc_terc', 'By tercile of the change in real income', ['bottom', 'middle', 'top']),
                                  (axes[3], 'size_chg', 'By change in household size', ['fell', 'same', 'rose'])]:
        s = ch.groupby(grp, observed=True)['d_y_'].agg(['mean', 'sem', 'size']).reindex(order)
        if grp == 'inc_terc':
            lab = [f'{o}\n({ch.loc[ch.inc_terc == o, "d_real_income"].mean():+,.0f}k)' for o in order]   # real_income is in $ thousands (2013)
        else:
            lab = order
        ax.bar(range(len(s)), s['mean'], yerr=1.96 * s['sem'], color=[BLUE, ORANGE, RED, GREY, '#555555'][:len(s)], capsize=4)
        ax.set_xticks(range(len(s))); ax.set_xticklabels(lab)
        for i, (m, nn) in enumerate(zip(s['mean'], s['size'])):
            ax.text(i, m + (0.02 if m >= 0 else -0.05) * abs(s['mean']).max() * 2, f'n={nn:,}', ha='center', fontsize=10, color=GREY)
        ax.axhline(0, color=GREY, ls='--'); ax.set_title(title); ax.set_ylabel(f'Mean change ({unit})')
        log(f'  {title}: ' + ', '.join(f'{o} {m:+.2f} ({se:.2f}, n={nn:,})' for o, m, se, nn in zip(s.index, s['mean'], s['sem'], s['size'])))
    fig.tight_layout()
    fig.savefig(FIG / f'dynamics_longrun_drift{suffix}.png', dpi=150, bbox_inches='tight'); plt.close(fig)
    log(f'saved {FIG / ("dynamics_longrun_drift" + suffix + ".png")}')


# ------------------------------------------------------------------ 3. cohorts
def fig_cohorts(df, y='hei_usda', yname='HEI', suffix=''):
    d = df[['household_code', 'panel_year', y, 'avg_age_hh_head', 'projection_factor']].dropna().rename(columns={y: 'hei_usda'}).copy()
    d['cohort'] = (np.floor((d['panel_year'] - d['avg_age_hh_head']) / 5) * 5).astype(int)
    d['age5'] = (np.floor(d['avg_age_hh_head'] / 5) * 5).astype(int)
    d['w_hei'] = d['hei_usda'] * d['projection_factor']
    def wmean_by(keys):
        a = d.groupby(keys).agg(w_hei=('w_hei', 'sum'), w=('projection_factor', 'sum'), n=('hei_usda', 'size')).reset_index()
        a['hei'] = a['w_hei'] / a['w']
        return a[a['n'] >= 300]
    by_year = wmean_by(['cohort', 'panel_year']); by_age = wmean_by(['cohort', 'age5'])
    cohorts = [c for c in sorted(by_year.cohort.unique()) if 1925 <= c <= 1985]
    cmap = plt.get_cmap('viridis')
    fig, axes = plt.subplots(1, 2, figsize=(17, 6.5))
    for i, c in enumerate(cohorts):
        col = cmap(0.8 * i / max(1, len(cohorts) - 1))   # stop at 0.8 of viridis: no yellow (project figure rule)
        a = by_year[by_year.cohort == c].sort_values('panel_year')
        axes[0].plot(a['panel_year'], a['hei'], color=col, lw=2, marker='o', ms=3, label=f'born {c}-{c + 4}')
        b = by_age[by_age.cohort == c].sort_values('age5')
        axes[1].plot(b['age5'] + 2.5, b['hei'], color=col, lw=2, marker='o', ms=3)
    axes[0].set_xlabel('Panel year'); axes[0].set_ylabel(f'Mean {yname}'); axes[0].set_title('By panel year, one line per 5-year birth cohort of the average head')
    axes[0].legend(ncol=2, frameon=False, fontsize=9, loc='lower right')
    axes[1].set_xlabel('Average head age'); axes[1].set_ylabel(f'Mean {yname}'); axes[1].set_title('The same cohorts by age')
    # within-household age slope (household FE only)
    g = d.groupby('household_code')
    yc = d['hei_usda'] - g['hei_usda'].transform('mean'); ac = d['avg_age_hh_head'] - g['avg_age_hh_head'].transform('mean')
    slope = np.cov(yc, ac)[0, 1] / np.var(ac, ddof=1)
    # the same, cross-section: HEI on age with year FE only
    yy = d['hei_usda'] - d.groupby('panel_year')['hei_usda'].transform('mean'); aa = d['avg_age_hh_head'] - d.groupby('panel_year')['avg_age_hh_head'].transform('mean')
    xslope = np.cov(yy, aa)[0, 1] / np.var(aa, ddof=1)
    axes[1].text(0.02, 0.97, f'within-household slope {10 * slope:+.2f} per 10 years of age (household FE; aging plus calendar time)\n'
                             f'cross-sectional slope {10 * xslope:+.2f} per 10 years (year FE)', transform=axes[1].transAxes, va='top', fontsize=11)
    fig.tight_layout()
    fig.savefig(FIG / f'dynamics_cohorts{suffix}.png', dpi=150, bbox_inches='tight'); plt.close(fig)
    log(f'cohorts, {yname}: within-household age slope {10 * slope:+.3f} per 10 years; cross-sectional {10 * xslope:+.3f} per 10 years')
    log('  cohort means in 2008 / 2014 / 2020: ' + '; '.join(
        f'{c}: ' + '/'.join(f'{v:.1f}' for v in by_year[(by_year.cohort == c) & (by_year.panel_year.isin([2008, 2014, 2020]))].sort_values('panel_year')['hei'])
        for c in cohorts))
    log(f'saved {FIG / ("dynamics_cohorts" + suffix + ".png")}')


# ------------------------------------------------------------------ 4. big movers
def big_movers(df, threshold=8, var='hei_usda', varlab='HEI', unit='points', suffix=''):
    n = df.dropna(subset=[var]).groupby('household_code').size()
    keep = n[n >= 6].index
    d = df[df.household_code.isin(keep)].copy()
    d['zip_code'] = d['zip_code'].astype(float)
    d['fips_num'] = pd.to_numeric(d['fips'], errors='coerce')
    cols = ['hei_usda', 'sugar_1000', 'sugar_usda_1000', 'soda_spend_share', 'real_income', 'household_size', 'any_kids', 'n_heads', 'any_head_employed', 'avg_age_hh_head'] + HEI_COMPONENTS
    f = window_means(d, cols + ['zip_code', 'fips_num'], True, 3, key=var); l = window_means(d, cols + ['zip_code', 'fips_num'], False, 3, key=var)
    ch = (l[cols] - f[cols])
    # location change: zip (or county) in the last observed year differs from the first observed year
    dd = d.sort_values(['household_code', 'panel_year'])
    first = dd.groupby('household_code')[['zip_code', 'fips_num']].first(); last = dd.groupby('household_code')[['zip_code', 'fips_num']].last()
    ch['zip_changed'] = 100 * (first['zip_code'] != last['zip_code']).astype(float)
    ch['county_changed'] = 100 * (first['fips_num'] != last['fips_num']).astype(float)
    ch['group'] = np.select([ch[var] >= threshold, ch[var] <= -threshold], ['risers', 'fallers'], 'others')
    ch['any_kids'] *= 100; ch['any_head_employed'] *= 100   # real_income is already in $ thousands (2013)
    counts = ch['group'].value_counts()
    half = threshold * 5 / 8
    n5 = ((ch[var] >= half).sum(), (ch[var] <= -half).sum())
    log(f'big movers (|change in 3-year mean {varlab}| >= {threshold} {unit}): risers {counts.get("risers", 0):,}, fallers {counts.get("fallers", 0):,} '
        f'of {len(ch):,} households with 6+ years ({100 * (counts.get("risers", 0) + counts.get("fallers", 0)) / len(ch):.1f}%); '
        f'with a {half:g}-{unit} threshold: {n5[0]:,} risers, {n5[1]:,} fallers; SD of the change {ch[var].std():.2f}')
    vars_ = [('hei_usda', 'HEI (points)'), ('sugar_1000', 'Sugar per 1,000 kcal, Syndigo (g)'), ('sugar_usda_1000', 'Sugar per 1,000 kcal, USDA (g)'),
             ('soda_spend_share', 'Soda share of grocery spending (pct. points)'), ('real_income', 'Real income (\\$ thousands, 2013)'), ('household_size', 'Household size (persons)'),
             ('any_kids', 'Any child under 18 (pct. points)'), ('n_heads', 'Number of heads'), ('any_head_employed', 'Any head employed (pct. points)'),
             ('avg_age_hh_head', 'Average head age (years)'), ('zip_changed', 'ZIP code changed, first to last year (\\%)'),
             ('county_changed', 'County changed, first to last year (\\%)')]
    stats = {grp: ch[ch.group == grp] for grp in ['risers', 'fallers', 'others']}
    lines = ['\\begin{tabular}{lrrr}', '\\toprule',
             ' & Risers & Fallers & Others \\\\',
             f' & {varlab} $\\geq +{threshold}$ {unit} & {varlab} $\\leq -{threshold}$ {unit} & \\\\',
             f' & N = {len(stats["risers"]):,} & N = {len(stats["fallers"]):,} & N = {len(stats["others"]):,} \\\\', '\\midrule',
             '\\multicolumn{4}{l}{\\textit{Change from the first three observed years to the last three (mean, SE)}} \\\\']
    for v, lab in vars_:
        cells = []
        for grp in ['risers', 'fallers', 'others']:
            x = stats[grp][v].dropna()
            cells.append(f'{x.mean():+,.2f} ({x.sem():.2f})' if len(x) else '')
        lines.append(f'{lab} & ' + ' & '.join(cells) + ' \\\\')
        log(f'  {lab}: ' + ', '.join(f'{grp} {stats[grp][v].mean():+.2f}' for grp in stats))
    lines += ['\\midrule', '\\multicolumn{4}{l}{\\textit{Change in HEI components (points of the component)}} \\\\']
    for c in HEI_COMPONENTS:
        cells = [f'{stats[grp][c].mean():+.2f}' for grp in ['risers', 'fallers', 'others']]
        lines.append(f'{COMPONENT_LABELS[c]} & ' + ' & '.join(cells) + ' \\\\')
    lines += ['\\bottomrule', '\\end{tabular}']
    (TAB / f'dynamics_big_movers{suffix}.tex').write_text('\n'.join(lines) + '\n')
    comp = pd.DataFrame({grp: stats[grp][HEI_COMPONENTS].mean() for grp in stats}).round(2)
    log('  component changes:\n' + comp.to_string())
    log(f'saved {TAB / ("dynamics_big_movers" + suffix + ".tex")}')


def main():
    df = load()
    variance_table(df)
    for y, yname, unit, suffix in DRIFT_OUTCOMES:
        fig_longrun_drift(df, y, yname, unit, suffix)
        fig_cohorts(df, y, yname, suffix)
    big_movers(df, 8, 'hei_usda', 'HEI', 'points', '')
    big_movers(df, 20, 'sugar_1000', 'sugar per 1,000 kcal', 'g', '_sugar')


if __name__ == '__main__':
    main()
