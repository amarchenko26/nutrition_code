"""
build_obesity_series.py

An obesity time series from the Nielsen Health Care Survey that holds the unit, the
respondent, the age-sex mix, and the sample composition fixed across years.
Reads interim/ailments/ailments_members.parquet and ailments_households.parquet
(clean/nielsen/clean_ailments_members.py) and interim/panelists/panelists_all_years.parquet.

Series (adults 18+, item "Obesity / Overweight", household projection factors as weights):
  all_adults      every adult member; 2011-2015 self-reports, 2016+ the respondent's report
                  on each member (self for the respondent, proxy for the others)
  self_report     2011-2015 every adult member (all self-reports); 2016+ the respondent only
  proxy_report    2016+ members other than the respondent
  each standardized to a fixed age x sex distribution (pooled 2011-2023 adult members,
  weighted; bins 18-24, 25-34, 35-44, 45-54, 55-64, 65+ by sex).
  hh_any_adult    household-level: any adult member flagged; projection-weighted, and
                  re-weighted by raking each year's responding households to the full
                  Kilts panel on head-age bracket x household size x income tercile x region.
  by income       all_adults and hh_any_adult by within-year quartile of the panel's
                  household income (household_income_midpoint, households ranked within year).
Checks: within-household change of hh_any_adult between consecutive survey years (the
2015->2016 and 2018->2019 pairs are the instrument changes); 2016-2017 bracket BMI vs the
item; response rates. Benchmark: NHANES measured obesity (NCHS data briefs 360 and 508);
CDC BRFSS self-reported obesity is printed for comparison but not plotted (Anya 2026-11-03).

Outputs (figs/health/obesity/): obesity_series.png, obesity_series_checks.png,
obesity_by_income.png
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
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

AIL = idir / 'ailments'
PAN = idir / 'panelists' / 'panelists_all_years.parquet'
FIG = fdir / 'health' / 'obesity'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#7a7a7a'
Q_COL = {1: RED, 2: ORANGE, 3: '#999999', 4: BLUE}
Q_LAB = {1: 'Bottom income quartile', 2: 'Second quartile', 3: 'Third quartile', 4: 'Top income quartile'}
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 10.5,
                     'axes.spines.top': False, 'axes.spines.right': False})

# CDC BRFSS, percent of adults 18+ who have obesity, nationwide, self-reported height and weight
# (data.cdc.gov/resource/hn4x-zwk7, locationabbr=US, questionid=Q036, Total; downloaded 2026-11-02)
BRFSS = {2011: 27.4, 2012: 27.7, 2013: 28.3, 2014: 28.9, 2015: 28.9, 2016: 29.6, 2017: 30.1, 2018: 30.9,
         2019: 31.4, 2020: 31.9, 2021: 33.0, 2022: 33.3, 2023: 32.8}
# NHANES measured obesity, adults 20+, age-adjusted (NCHS data briefs 360 and 508); plotted at cycle midpoints
NHANES = {2011.5: 34.9, 2013.5: 37.7, 2015.5: 39.6, 2017.5: 42.4, 2022.5: 40.3}
YLIM = (10, 45)   # the same y-axis span on every panel

# age bins follow the 2011-2015 questionnaire's brackets (Q7): 18-24, 25-34, 35-44, 45-54, 55-64, 65+
AGE_BINS = [18, 25, 35, 45, 55, 65, 200]
AGE_LAB = ['18-24', '25-34', '35-44', '45-54', '55-64', '65+']
BRACKET_MID = {1: 3, 2: 9, 3: 15, 4: 19, 5: 22.5, 6: 29.5, 7: 39.5, 8: 49.5, 9: 59.5, 10: 72}
REGIME_LINES = [(2015.5, 'one respondent per household,\nmember grid'), (2018.5, 'list renumbered')]


def wmean(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok]) if ok.any() and w[ok].sum() > 0 else np.nan


def standardized(d, ref, flag='obesity', w='w'):
    """Direct age x sex standardization: cell rates weighted by the reference cell shares."""
    if len(d) == 0:
        return np.nan
    cell = d.groupby(['age_bin', 'gender'], observed=True).apply(lambda g: wmean(g[flag], g[w]))
    cell = cell.reindex(ref.index)
    return float(np.nansum(cell.values * ref.values) / np.nansum(ref.values[~np.isnan(cell.values)]))


def rake(df, margins, w0='projection_factor', iters=30):
    """Iterative proportional fitting of df[w0] to target shares on each margin.
    margins: {column: Series of target shares indexed by category}."""
    w = df[w0].astype(float).values.copy()
    for _ in range(iters):
        for col, target in margins.items():
            vals = df[col].astype(str).values
            cur = pd.Series(w).groupby(vals).sum()
            cur = cur / cur.sum()
            t = target.copy(); t.index = t.index.astype(str)
            adj = (t / cur).reindex(vals).fillna(1.0).values
            w = w * adj
    return w


def regime_lines(ax, labels=False, ys=(44.5, 36.5)):
    for (x, lab), y in zip(REGIME_LINES, ys):
        ax.axvline(x, color=GREY, lw=1, ls=':')
        if labels:
            ax.text(x + 0.1, y, lab, fontsize=8.5, color=GREY, va='top')


def main():
    mem = pd.read_parquet(AIL / 'ailments_members.parquet')
    hh = pd.read_parquet(AIL / 'ailments_households.parquet')
    assert mem['panel_year'].nunique() >= 10, f'ailments_members.parquet holds only {sorted(mem.panel_year.unique())}; rerun clean_ailments_members.py'
    pan = pd.read_parquet(PAN, columns=['household_code', 'panel_year', 'projection_factor', 'household_size',
                                        'household_income_midpoint', 'male_head_age_exact', 'female_head_age_exact',
                                        'region_code'])
    pan['head_age'] = pan[['male_head_age_exact', 'female_head_age_exact']].max(axis=1)
    pan['head_age_bin'] = pd.cut(pan['head_age'], [0, 35, 45, 55, 65, 200], labels=['<35', '35-44', '45-54', '55-64', '65+'])
    pan['size_bin'] = pd.cut(pan['household_size'], [0, 1, 2, 100], labels=['1', '2', '3+'])
    pan['inc_ter'] = pan.groupby('panel_year')['household_income_midpoint'].transform(
        lambda s: pd.qcut(s.rank(method='first'), 3, labels=['low', 'mid', 'high']))
    pan['inc_q'] = pan.groupby('panel_year')['household_income_midpoint'].transform(
        lambda s: pd.qcut(s.rank(method='first'), 4, labels=[1, 2, 3, 4])).astype(float)
    pan['region'] = pan['region_code'].astype('Int64').astype(str)
    pan = pan.dropna(subset=['projection_factor'])

    # ---------------------------------------------------------------- members
    mem = mem.merge(pan[['household_code', 'panel_year', 'projection_factor', 'inc_q']], on=['household_code', 'panel_year'], how='left')
    match = mem.drop_duplicates(['household_code', 'panel_year']).groupby('panel_year')['projection_factor'].apply(lambda s: s.notna().mean())
    print('share of surveyed households found in the Kilts panelist file, by year:'); print(match.round(3).to_dict())
    mem = mem[mem['projection_factor'].notna()]
    mem['w'] = mem['projection_factor']
    # 2011-2015: age from the Q7 bracket (the birth-year field Q13a_yy is unreliable: it gives a mean
    # adult age above 70); 2016+: age in years from the roster
    br = mem['age_bracket'].notna()
    mem.loc[br, 'age'] = mem.loc[br, 'age_bracket'].map(BRACKET_MID)
    mem = mem[mem['age'] >= 18].copy()
    mem = mem[mem['gender'].isin([1, 2])]
    if 'answered' in mem.columns:
        mem = mem[(mem['answered'] == 1) | (mem['regime'] == 'member_proxy')]
    mem['age_bin'] = pd.cut(mem['age'], AGE_BINS, labels=AGE_LAB, right=False)
    ref = mem.groupby(['age_bin', 'gender'], observed=True)['w'].sum()
    ref = ref / ref.sum()
    print('reference age x sex shares (pooled adults):'); print(ref.round(3).to_string())

    years = sorted(mem['panel_year'].unique())
    ser = pd.DataFrame(index=years)
    byq = pd.DataFrame(index=years)
    for y in years:
        d = mem[mem['panel_year'] == y]
        ser.loc[y, 'n_adults'] = len(d)
        ser.loc[y, 'n_households'] = d['household_code'].nunique()
        ser.loc[y, 'all_adults_raw'] = 100 * wmean(d['obesity'], d['w'])
        ser.loc[y, 'all_adults_std'] = 100 * standardized(d, ref)
        s = d[d['is_respondent'] == 1]
        ser.loc[y, 'self_report_std'] = 100 * standardized(s, ref)
        ser.loc[y, 'self_n'] = len(s)
        p = d[(d['is_respondent'] == 0)]
        ser.loc[y, 'proxy_report_std'] = 100 * standardized(p, ref) if len(p) else np.nan
        ser.loc[y, 'mean_age'] = wmean(d['age'], d['w'])
        ser.loc[y, 'share_female'] = wmean((d['gender'] == 2).astype(float), d['w'])
        ser.loc[y, 'brfss'] = BRFSS.get(y, np.nan)
        for q in (1, 2, 3, 4):
            byq.loc[y, f'adults_q{q}'] = 100 * standardized(d[d['inc_q'] == q], ref)

    # ---------------------------------------------------------------- households
    anyad = mem.groupby(['household_code', 'panel_year'])['obesity'].max().rename('any_adult').reset_index()
    hh = hh.merge(anyad, on=['household_code', 'panel_year'], how='left')
    hh = hh.merge(pan, on=['household_code', 'panel_year'], how='inner')
    hh['w_raked'] = np.nan
    margins_cols = ['head_age_bin', 'size_bin', 'inc_ter', 'region']
    for y in years:
        tgt = pan[(pan['panel_year'] == y) & pan[margins_cols].notna().all(axis=1)]
        m = (hh['panel_year'] == y) & hh[margins_cols].notna().all(axis=1) & hh['any_adult'].notna()
        d = hh[m].copy()
        margins = {c: tgt.groupby(c, observed=True)['projection_factor'].sum().pipe(lambda s: s / s.sum()) for c in margins_cols}
        d['w_raked'] = rake(d, margins)
        hh.loc[m, 'w_raked'] = d['w_raked'].values
        ser.loc[y, 'hh_any_adult_pw'] = 100 * wmean(d['any_adult'], d['projection_factor'])
        ser.loc[y, 'hh_any_adult_raked'] = 100 * wmean(d['any_adult'], d['w_raked'])
        ser.loc[y, 'hh_list_obesity_pw'] = 100 * wmean(d['hh_obesity'], d['projection_factor']) if 'hh_obesity' in d else np.nan
        ser.loc[y, 'resp_rate'] = 100 * d['household_code'].nunique() / tgt['household_code'].nunique()
        ser.loc[y, 'hh_head_age'] = wmean(d['head_age'], d['projection_factor'])
        ser.loc[y, 'panel_head_age'] = wmean(tgt['head_age'], tgt['projection_factor'])
        for q in (1, 2, 3, 4):
            dq = d[d['inc_q'] == q]
            byq.loc[y, f'hh_q{q}'] = 100 * wmean(dq['any_adult'], dq['projection_factor'])

    # within-household change between consecutive survey years
    wide = hh.pivot_table(index='household_code', columns='panel_year', values='any_adult')
    chg = {}
    for a, b in zip(years[:-1], years[1:]):
        both = wide[[a, b]].dropna()
        if len(both):
            chg[(a, b)] = (len(both), 100 * (both[b] - both[a]).mean(), 100 * both[a].mean(), 100 * both[b].mean())
    print('\nwithin-household change in any-adult obesity flag (households in both years):')
    for (a, b), (n, dlt, ra, rb) in chg.items():
        print(f'  {a}->{b}: n = {n:6,}  rate {ra:5.1f} -> {rb:5.1f}  change {dlt:+.2f} pts')

    # BMI check, 2016-2017
    bmi = mem[(mem['panel_year'].isin([2016, 2017])) & mem['bmi'].notna() & (mem['bmi'] > 10) & (mem['bmi'] < 80)]
    print(f'\nBMI check (2016-2017 adults with bracket weight and height): n = {len(bmi):,}')
    for lab, cond in [('BMI >= 30', bmi['bmi'] >= 30), ('BMI 25-30', bmi['bmi'].between(25, 30)), ('BMI < 25', bmi['bmi'] < 25)]:
        g = bmi[cond]
        print(f'  {lab:10s}: share of adults {100 * wmean(cond.astype(float), bmi["w"]):5.1f}%; item flagged {100 * wmean(g["obesity"], g["w"]):5.1f}% '
              f'(self {100 * wmean(g.loc[g.is_respondent == 1, "obesity"], g.loc[g.is_respondent == 1, "w"]):5.1f}%, '
              f'proxy {100 * wmean(g.loc[g.is_respondent == 0, "obesity"], g.loc[g.is_respondent == 0, "w"]):5.1f}%)')
    for y in (2016, 2017):
        b = bmi[bmi['panel_year'] == y]
        print(f'  {y}: BMI>=30 share {100 * wmean((b.bmi >= 30).astype(float), b.w):.1f}%, item {100 * wmean(b.obesity, b.w):.1f}%')

    print('\nseries:'); print(ser.round(2).to_string())
    print('\nby within-year income quartile (adults age-sex standardized; households any adult):'); print(byq.round(1).to_string())

    # ---------------------------------------------------------------- figures
    fig, axes = plt.subplots(1, 2, figsize=(17, 6))
    ax = axes[0]
    ax.plot(ser.index, ser['self_report_std'], color=BLUE, lw=2.5, marker='o', label='Nielsen: self-reported "obesity / overweight", adults 18+, age-sex standardized')
    ax.plot(ser.index, ser['all_adults_std'], color=BLUE, lw=1.8, ls='--', marker='o', ms=4, label='Nielsen: all adult members (2016+ includes proxy reports)')
    ax.plot(list(NHANES), list(NHANES.values()), color='black', lw=2.5, marker='D', ms=6, label='NHANES, measured BMI ≥ 30, adults 20+, age-adjusted (by survey cycle)')
    for (x, lab), y in zip(REGIME_LINES, (44.5, 36.5)):
        ax.axvline(x, color=GREY, lw=1, ls=':')
        ax.text(x + 0.1, y, lab, fontsize=8.5, color=GREY, va='top')
    ax.set_ylabel('Percent of adults')
    ax.set_xlabel('Survey year')
    ax.set_title('Adults reporting obesity / overweight')
    ax.set_ylim(*YLIM)
    ax.legend(loc='lower right', frameon=False, fontsize=9)
    ax = axes[1]
    ax.plot(ser.index, ser['hh_any_adult_pw'], color=RED, lw=2.5, marker='o', label='Any adult member flagged, projection-weighted')
    ax.plot(ser.index, ser['hh_any_adult_raked'], color=RED, lw=2, ls='--', marker='o', ms=4, label='Same, respondents re-weighted to the panel\n(head age × size × income tercile × region)')
    ax.plot(ser.index, ser['hh_list_obesity_pw'], color=GREY, lw=1.5, ls='-.', marker='^', ms=4, label='Household-level list item (2016+ question; 2011-15 any member)')
    regime_lines(ax)
    ax.set_ylabel('Percent of households')
    ax.set_xlabel('Survey year')
    ax.set_title('Households with any adult reporting obesity / overweight')
    ax.set_ylim(*YLIM)
    ax.legend(loc='lower left', frameon=False, fontsize=9)
    fig.savefig(FIG / 'obesity_series.png', bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'saved {FIG / "obesity_series.png"}')

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    ax = axes[0]
    ax.bar([f'{a}\n{b}' for (a, b) in chg], [v[1] for v in chg.values()], color=[RED if (a, b) in [(2015, 2016), (2018, 2019)] else BLUE for (a, b) in chg])
    ax.axhline(0, color=GREY, lw=1)
    ax.set_ylabel('Change in any-adult flag, pts')
    ax.set_title('Within-household change between consecutive surveys\n(red = survey redesign)')
    ax.tick_params(axis='x', labelsize=8)
    ax = axes[1]
    ax.plot(ser.index, ser['resp_rate'], color=BLUE, lw=2.5, marker='o')
    ax.set_ylabel('Responding households, % of Kilts panel')
    ax.set_title('Survey coverage of the panel')
    ax.set_ylim(0, 105)
    fig.savefig(FIG / 'obesity_series_checks.png', bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'saved {FIG / "obesity_series_checks.png"}')

    fig, axes = plt.subplots(1, 2, figsize=(17, 6))
    ax = axes[0]
    for q in (1, 2, 3, 4):
        ax.plot(byq.index, byq[f'adults_q{q}'], color=Q_COL[q], lw=2.5 if q in (1, 4) else 1.8, marker='o', ms=5 if q in (1, 4) else 4, label=Q_LAB[q])
    regime_lines(ax, labels=True, ys=(34.5, 30.5))
    ax.set_ylabel('Percent of adults'); ax.set_xlabel('Survey year')
    ax.set_title('Adults reporting obesity / overweight, by household income quartile\n(all adult members, age-sex standardized)')
    ax.set_ylim(*YLIM)
    ax.legend(loc='upper right', frameon=False)
    ax = axes[1]
    for q in (1, 2, 3, 4):
        ax.plot(byq.index, byq[f'hh_q{q}'], color=Q_COL[q], lw=2.5 if q in (1, 4) else 1.8, marker='o', ms=5 if q in (1, 4) else 4, label=Q_LAB[q])
    regime_lines(ax)
    ax.set_ylabel('Percent of households'); ax.set_xlabel('Survey year')
    ax.set_title('Households with any adult reporting obesity / overweight,\nby income quartile (projection-weighted)')
    ax.set_ylim(*YLIM)
    ax.legend(loc='upper right', frameon=False)
    fig.savefig(FIG / 'obesity_by_income.png', bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'saved {FIG / "obesity_by_income.png"}')


if __name__ == '__main__':
    main()
