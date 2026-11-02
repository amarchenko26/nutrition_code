"""
Build the occupation-movers panel for the Finkelstein-Gentzkow-Williams (2016)
style design with occupations as "places" and nutrition as the outcome.

Input:  final/labor_supply_reg_data.dta (household x panel year)
Output: interim/movers/occupation_movers_panel.parquet / .dta
        interim/movers/occupation_means.csv   (mean outcome by occupation, y_j)

Occupation: the household's assigned Nielsen occupation code -- the male
head's unless he is absent (0) or in the armed forces (7), then the female
head's (the paper's cell assignment). Codes: 1 professional, 2 manager/owner,
3 clerical, 4 sales, 5 craftsman, 6 operative/driver, 7 armed forces,
8 service, 9 farmer, 10 student employed <30h, 11 laborer, 12 not employed.

Sample: geographic non-movers (movers_f == 0), as in the paper's regressions.
The descriptive figures use all 12 codes; the event-study sample
(`analysis == 1`) drops armed forces (7) and students (10), which are tiny.

Movers (FGW definition): the occupation code changes EXACTLY once over the
household's observed years, and the change is between two consecutive panel
years so the move can be dated. Non-movers never change code. Households
whose code changes more than once are "multi movers": for a robustness
variant their panel is truncated just before the second change and the first
change is the event (mover_type 2). A change across a gap in the panel cannot
be dated (mover_type 3) and is never an event.

Timing: the year-t panelist file reports occupation as of the fall of t-1, so
a new code first visible in file year m reflects a move during calendar year
m-1. Following FGW, relative year r = 0 is the (partially treated) move
year, i.e. file year m-1; r = -1 is the last clean pre-move year (file m-2)
and r = 1 the first clean post-move year (file m).

Other switches (for 8_build_other_switch_event_studies.do): the first consecutive-year
change of a household state, panel cut before the state's next change; never-changers
are the comparison group. birth: no child under 6 -> child under 6 with household size
up (r_birth, birth_grp 1 mover / 0 never-changer). death: two heads -> one head, widowed
(r_death, death_grp; death_state 0 two heads / 1 one widowed head).

delta_<outcome> = y_dest - y_orig (also for real_income, $10k), where y_j is the projection-weighted mean
of the outcome by occupation and year, averaged across years (non-movers and
single movers outside r = 0, as in FGW).
"""

import numpy as np
import pandas as pd
import pyreadstat
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
IN = BASE / 'final' / 'labor_supply_reg_data.dta'
OUT_DIR = BASE / 'interim' / 'movers'

OUTCOMES = ['hei_usda', 'hi_usda', 'hi', 'real_income']   # real_income: occupation income gap, $10k (2013$)
OCC_LABELS = {1: 'Professional', 2: 'Manager/owner', 3: 'Clerical', 4: 'Sales',
              5: 'Craftsman', 6: 'Operative/driver', 7: 'Armed forces',
              8: 'Service', 9: 'Farmer', 10: 'Student <30h', 11: 'Laborer',
              12: 'Not employed'}
DROP_FROM_ANALYSIS = {7, 10}

# cell bins as in build/iv/build_iv.py
EDUC_BINS = [0, 11.9, 12.9, 15.9, np.inf]
SIZE_BINS = [0, 1, 2, 4, np.inf]

COLS = ['hhid', 'year', 'male_head_occupation', 'female_head_occupation',
        'hh_avg_yrsofschool', 'hh_size', 'avg_age_hh_head', 'kids', 'hh_comp',
        'fips_state_code', 'fips', 'zip_code', 'projection_factor', 'real_income',
        'movers_f', 'n_heads', 'male_head_employment', 'female_head_employment', 'marital_status'] + [v for v in OUTCOMES if v != 'real_income']


def wmean(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok]) if ok.any() else np.nan


def first_switch_events(df, state, name, is_event):
    """First change of `state` over the household's observed years. The household is a
    `name` mover if that first change is between consecutive years and satisfies
    is_event (a function of the row, with _prev = previous state and _size_prev =
    previous household size); r_<name> follows the FGW timing (r = 0 is file year m-1)
    and is cut before the state's next change. <name>_grp: 1 mover, 0 never-changer."""
    df = df.sort_values(['hhid', 'year']).reset_index(drop=True)
    g = df.groupby('hhid')
    df['_prev'] = g[state].shift(1)
    df['_size_prev'] = g['hh_size'].shift(1)
    df['_chg'] = df['_prev'].notna() & (df[state] != df['_prev'])
    df['_chgno'] = g['_chg'].cumsum()
    n_chg = g['_chg'].transform('sum')
    first = df[df['_chg'] & (df['_chgno'] == 1)]
    ok = is_event(first) & (first['gap'] == 1)
    ev = first.loc[ok, ['hhid', 'year']].rename(columns={'year': f'm_{name}'})
    df = df.merge(ev, on='hhid', how='left')
    df[f'r_{name}'] = df['year'] - (df[f'm_{name}'] - 1)
    df.loc[df['_chgno'] >= 2, f'r_{name}'] = np.nan
    df[f'{name}_grp'] = np.where(df[f'm_{name}'].notna(), 1.0, np.where(n_chg == 0, 0.0, np.nan))
    return df.drop(columns=['_prev', '_size_prev', '_chg', '_chgno'])


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df, _ = pyreadstat.read_dta(str(IN), usecols=COLS)
    df['hhid'] = df['hhid'].astype('int64')
    df['year'] = df['year'].astype(int)
    print(f'loaded {len(df):,} household-years')

    # --------------------------------------------------- assigned occupation
    m = df['male_head_occupation'].astype(float)
    f = df['female_head_occupation'].astype(float)
    df['occ'] = m.where(~(m.isna() | m.isin([0, 7])), f)
    df = df[df['occ'].notna() & (df['occ'] > 0)].copy()
    df['occ'] = df['occ'].astype(int)
    df = df[df['movers_f'] == 0].copy()
    print(f'geographic non-movers with an occupation code: {len(df):,} '
          f'household-years, {df.hhid.nunique():,} households')

    df = df.sort_values(['hhid', 'year']).reset_index(drop=True)
    g = df.groupby('hhid')
    df['n_obs'] = g['year'].transform('size')
    df['gap'] = g['year'].diff()                       # years since previous obs
    df['occ_prev'] = g['occ'].shift(1)
    df['switch'] = (df['occ_prev'].notna()) & (df['occ'] != df['occ_prev'])
    df['switch_consec'] = df['switch'] & (df['gap'] == 1)
    df['n_switch'] = g['switch'].transform('sum').astype(int)
    df['switch_no'] = g['switch'].cumsum()             # 0 before first switch, 1 after, ...

    # ------------------------------------------------------ mover types
    # first switch row per household
    first = df[df['switch'] & (df['switch_no'] == 1)][['hhid', 'year', 'occ', 'occ_prev', 'switch_consec']]
    first = first.rename(columns={'year': 'm_file', 'occ': 'dest', 'occ_prev': 'origin',
                                  'switch_consec': 'first_consec'})
    df = df.merge(first, on='hhid', how='left')

    df['mover_type'] = 0                                # non-mover
    df.loc[(df['n_switch'] == 1) & df['first_consec'].eq(True), 'mover_type'] = 1
    df.loc[(df['n_switch'] >= 2) & df['first_consec'].eq(True), 'mover_type'] = 2
    df.loc[(df['n_switch'] >= 1) & df['first_consec'].eq(False), 'mover_type'] = 3

    # multi movers: keep rows before the second switch only (first move = event)
    df['keep_multi'] = ~((df['mover_type'] == 2) & (df['switch_no'] >= 2))

    # FGW relative year: r = 0 is the calendar year of the move = file year m-1
    df['event_year'] = df['m_file'] - 1
    df['r'] = df['year'] - df['event_year']
    df.loc[~df['mover_type'].isin([1, 2]), ['r', 'event_year']] = np.nan
    df.loc[df['mover_type'] == 2, 'r'] = np.where(df.loc[df['mover_type'] == 2, 'keep_multi'],
                                                  df.loc[df['mover_type'] == 2, 'r'], np.nan)
    df.loc[~df['mover_type'].isin([1, 2]), ['origin', 'dest']] = np.nan

    # ------------------------------------------------------------ bins
    df['educ_bin'] = pd.cut(df['hh_avg_yrsofschool'], bins=EDUC_BINS, labels=[1, 2, 3, 4]).astype(float)
    df['size_bin'] = pd.cut(df['hh_size'], bins=SIZE_BINS, labels=[1, 2, 3, 4]).astype(float)
    df['age5'] = (np.floor(df['avg_age_hh_head'] / 5) * 5).clip(upper=85)
    df['state'] = pd.to_numeric(df['fips_state_code'], errors='coerce')
    df['county'] = pd.to_numeric(df['fips'], errors='coerce')
    df['zip'] = pd.to_numeric(df['zip_code'], errors='coerce')

    # pure occupation move: education and size bins unchanged from file m-1 to m
    at_m = df[df['year'] == df['m_file']][['hhid', 'educ_bin', 'size_bin']].rename(
        columns={'educ_bin': 'educ_m', 'size_bin': 'size_m'})
    at_m1 = df[df['year'] == df['m_file'] - 1][['hhid', 'educ_bin', 'size_bin']].rename(
        columns={'educ_bin': 'educ_m1', 'size_bin': 'size_m1'})
    df = df.merge(at_m, on='hhid', how='left').merge(at_m1, on='hhid', how='left')
    df['pure_move'] = ((df['educ_m'] == df['educ_m1']) & (df['size_m'] == df['size_m1'])).astype(int)
    df.loc[~df['mover_type'].isin([1, 2]), 'pure_move'] = 0
    # cell of the move (at file m-1) for stratified comparisons
    df['educ_move'] = df['educ_m1']
    df['size_move'] = df['size_m1']
    df = df.drop(columns=['educ_m', 'size_m', 'educ_m1', 'size_m1'])

    # -------------------------------------- other switches: births, deaths
    df['young_child'] = df['kids'].isin([1, 4, 5, 7]).astype(int)
    df['death_state'] = np.where(df['n_heads'] == 2, 0.0,
                                 np.where((df['n_heads'] == 1) & (df['marital_status'] == 2), 1.0, np.nan))
    df = first_switch_events(df, 'young_child', 'birth',
                             lambda d: (d['_prev'] == 0) & (d['young_child'] == 1) & (d['hh_size'] > d['_size_prev']))
    df = first_switch_events(df, 'n_heads', 'death',
                             lambda d: (d['_prev'] == 2) & (d['n_heads'] == 1) & (d['marital_status'] == 2))
    for name in ['birth', 'death']:
        print(f"{name}: movers {int((df.groupby('hhid')[f'{name}_grp'].first() == 1).sum()):,}, "
              f"never-changers {int((df.groupby('hhid')[f'{name}_grp'].first() == 0).sum()):,}")

    # ----------------------------------------------------- analysis flag
    df['analysis'] = ((~df['occ'].isin(DROP_FROM_ANALYSIS))
                      & df['keep_multi']
                      & (~df['origin'].isin(DROP_FROM_ANALYSIS) | df['origin'].isna())
                      & (~df['dest'].isin(DROP_FROM_ANALYSIS) | df['dest'].isna())
                      & (df['mover_type'] != 3)).astype(int)

    # ------------------------------------------ occupation means y_j (FGW)
    base = df[(df['analysis'] == 1) & (df['mover_type'].isin([0, 1])) & (df['r'].fillna(-99) != 0)]
    rows = []
    for j, d in base.groupby('occ'):
        rec = {'occ': j, 'label': OCC_LABELS[j], 'n_hh_years': len(d),
               'income_w': wmean(d['real_income'], d['projection_factor']),
               'age_w': wmean(d['avg_age_hh_head'], d['projection_factor'])}
        for v in OUTCOMES:
            by_year = d.groupby('year').apply(lambda x: wmean(x[v], x['projection_factor']))
            rec[f'ybar_{v}'] = by_year.mean()
        rows.append(rec)
    means = pd.DataFrame(rows).set_index('occ')
    # the full 12-code means (descriptives) on the same rule
    base12 = df[(df['mover_type'].isin([0, 1])) & (df['r'].fillna(-99) != 0)]
    rows = []
    for j, d in base12.groupby('occ'):
        rec = {'occ': j, 'label': OCC_LABELS[j], 'n_hh_years': len(d),
               'n_households': d['hhid'].nunique(),
               'income_w': wmean(d['real_income'], d['projection_factor']),
               'age_w': wmean(d['avg_age_hh_head'], d['projection_factor'])}
        for v in OUTCOMES:
            by_year = d.groupby('year').apply(lambda x: wmean(x[v], x['projection_factor']))
            rec[f'ybar_{v}'] = by_year.mean()
            rec[f'ybar_{v}_se'] = d[v].std() / np.sqrt(d['hhid'].nunique())
        rows.append(rec)
    means12 = pd.DataFrame(rows).set_index('occ')
    means12['income_rank'] = means12['income_w'].rank().astype(int)
    means12['hei_rank'] = means12['ybar_hei_usda'].rank().astype(int)
    means12.to_csv(OUT_DIR / 'occupation_means.csv')
    print(means12.round(3).to_string())

    for v in OUTCOMES:
        ymap = means[f'ybar_{v}']
        df[f'delta_{v}'] = df['dest'].map(ymap) - df['origin'].map(ymap)
    df['income_rank'] = df['occ'].map(means12['income_rank'])
    df['rank_orig'] = df['origin'].map(means12['income_rank'])
    df['rank_dest'] = df['dest'].map(means12['income_rank'])
    df['up_income'] = np.where(df['origin'].notna(), (df['rank_dest'] > df['rank_orig']).astype(float), np.nan)

    # ------------------------------------------------------------ save
    keep = ['hhid', 'year', 'occ', 'mover_type', 'keep_multi', 'r', 'event_year', 'm_file',
            'origin', 'dest', 'n_obs', 'n_switch', 'switch', 'switch_consec', 'gap', 'occ_prev',
            'analysis', 'pure_move', 'educ_bin', 'size_bin', 'educ_move', 'size_move', 'age5',
            'state', 'county', 'zip', 'kids', 'hh_comp', 'hh_size', 'avg_age_hh_head',
            'projection_factor', 'real_income', 'income_rank', 'rank_orig', 'rank_dest',
            'up_income', 'n_heads', 'male_head_employment', 'female_head_employment', 'marital_status',
            'young_child', 'death_state', 'm_birth', 'r_birth', 'birth_grp', 'm_death', 'r_death', 'death_grp'] \
        + [v for v in OUTCOMES if v != 'real_income'] + [f'delta_{v}' for v in OUTCOMES]
    out = df[keep].copy()
    out['keep_multi'] = out['keep_multi'].astype(int)
    out['switch'] = out['switch'].astype(int)
    out['switch_consec'] = out['switch_consec'].astype(int)
    out.to_parquet(OUT_DIR / 'occupation_movers_panel.parquet', index=False)
    out.to_stata(OUT_DIR / 'occupation_movers_panel.dta', write_index=False, version=118)

    # summary
    hh = out.groupby('hhid').agg(mover_type=('mover_type', 'first'), n_obs=('n_obs', 'first'),
                                 analysis=('analysis', 'max'))
    print('\nhouseholds by mover type (all 12 codes):')
    print(hh['mover_type'].value_counts().sort_index().rename(
        {0: 'non-mover', 1: 'single mover', 2: 'multi mover', 3: 'undated (gap)'}))
    print('\nhouseholds by mover type, analysis sample:')
    print(hh[hh['analysis'] == 1]['mover_type'].value_counts().sort_index())
    print('\nrelative-year distribution, single movers (analysis):')
    print(out[(out['mover_type'] == 1) & (out['analysis'] == 1)]['r'].value_counts().sort_index().to_dict())
    print(f'\nsaved {OUT_DIR / "occupation_movers_panel.parquet"}')


if __name__ == '__main__':
    main()
