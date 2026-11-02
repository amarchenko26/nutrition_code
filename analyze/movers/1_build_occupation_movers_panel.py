"""
Build the occupation-movers panels for the Finkelstein-Gentzkow-Williams (2016)
design with occupations as "places" and nutrition as the outcome.

Input:  final/labor_supply_reg_data.dta (household x panel year)
Output: interim/movers/occupation_movers_panel.parquet       household-year panel (descriptives)
        interim/movers/movers_events_panel.parquet / .dta    stacked event panel (estimation sample)
        interim/movers/occupation_means.csv                  mean outcome by occupation, y_j

Occupation: the household's assigned Nielsen occupation code -- the male head's
unless he is absent (0) or in the armed forces (7), then the female head's (the
paper's cell assignment). Codes: 1 professional, 2 manager/owner, 3 clerical,
4 sales, 5 craftsman, 6 operative/driver, 7 armed forces, 8 service, 9 farmer,
10 student employed <30h, 11 laborer, 12 not employed.

Sample: geographic non-movers (movers_f == 0), as in the paper's regressions.
The descriptive figures use all 12 codes; events drop armed forces (7) and
students (10), which are tiny.

Reversals (2026-11-02): a code that changes in year t and returns to the year
t-1 code in year t+1 (three consecutive observed years) is treated as
measurement error: occ_t is set back to the t-1 code (`occ`; the reported code
is kept as `occ_raw`). A household whose only changes are such blips is a
non-mover.

Events (2026-11-02, main analysis = moves within employment, all movers): every
change of the smoothed code between two consecutive panel years with origin and
destination both employed codes (1-6, 8, 9, 11) is an event (emp2emp = 1);
changes into or out of not employed (12) are built as events too (emp2emp = 0)
but are not the main sample. The event window is the origin spell back to the
previous change (or panel start) and the destination spell up to the next
change (or panel end), so a household that moves twice contributes two events
and the years between its moves sit in both windows; the Stata event study
absorbs event fixed effects and clusters on household. A household with one
move has one event and the design reduces to FGW's single-mover design.

Timing: the year-t panelist file carries the demographics reported in the
NielsenIQ survey of the fall of t-1 (Consumer Panel Dataset Manual 2024,
Panelists file, p. 21; FAQ 9, p. 59), so a code first visible in file year m
reflects a move during calendar year m-1. Following FGW, relative year r = 0
is the (partially treated) move year, i.e. file year m-1; r = -1 is the last
clean pre-move year (file m-2) and r = 1 the first clean post-move year (file m).

delta_<outcome> = y_dest - y_orig (also for real_income, $10k), where y_j is the
projection-weighted mean of the outcome by occupation and year, averaged across
years, on household-years outside any event's r = 0 (as in FGW).
"""

import numpy as np
import pandas as pd
import pyreadstat
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
IN = BASE / 'final' / 'labor_supply_reg_data.dta'
OUT_DIR = BASE / 'interim' / 'movers'

OUTCOMES = ['hei_usda', 'hi_usda', 'hi', 'real_income']   # real_income in $10k (2013$)
OCC_LABELS = {1: 'Professional', 2: 'Manager/owner', 3: 'Clerical', 4: 'Sales',
              5: 'Craftsman', 6: 'Operative/driver', 7: 'Armed forces',
              8: 'Service', 9: 'Farmer', 10: 'Student <30h', 11: 'Laborer',
              12: 'Not employed'}
EMPLOYED = {1, 2, 3, 4, 5, 6, 8, 9, 11}
DROP_FROM_ANALYSIS = {7, 10}

# cell bins as in build/iv/build_iv.py
EDUC_BINS = [0, 11.9, 12.9, 15.9, np.inf]
SIZE_BINS = [0, 1, 2, 4, np.inf]

COLS = ['hhid', 'year', 'male_head_occupation', 'female_head_occupation',
        'hh_avg_yrsofschool', 'hh_size', 'avg_age_hh_head', 'kids', 'hh_comp',
        'fips_state_code', 'fips', 'zip_code', 'projection_factor', 'real_income',
        'movers_f'] + [v for v in OUTCOMES if v != 'real_income']


def wmean(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok]) if ok.any() else np.nan


def smooth_reversals(df):
    """occ_t -> occ_{t-1} when occ_t != occ_{t-1} and occ_{t+1} == occ_{t-1}, with
    t-1, t, t+1 consecutive observed years of the same household. Sequential, so
    a smoothed value feeds the next comparison (A,B,A,B,A -> A,A,A,A,A)."""
    hh = df['hhid'].values
    yr = df['year'].values
    occ = df['occ'].values.copy()
    raw = df['occ'].values
    n_blip = 0
    for i in range(1, len(occ) - 1):
        if hh[i - 1] == hh[i] == hh[i + 1] and yr[i] - yr[i - 1] == 1 and yr[i + 1] - yr[i] == 1 \
                and raw[i] != occ[i - 1] and raw[i + 1] == occ[i - 1]:
            occ[i] = occ[i - 1]
            n_blip += 1
    return occ, n_blip


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
    df['occ_raw'] = df['occ']
    df['occ'], n_blip = smooth_reversals(df)
    print(f'one-year reversals set back to the origin code: {n_blip:,} household-years '
          f'({100 * n_blip / len(df):.2f}% of rows)')

    g = df.groupby('hhid')
    df['n_obs'] = g['year'].transform('size')
    df['gap'] = g['year'].diff()                       # years since previous obs
    df['occ_prev'] = g['occ'].shift(1)
    df['switch'] = (df['occ_prev'].notna()) & (df['occ'] != df['occ_prev'])
    df['switch_consec'] = df['switch'] & (df['gap'] == 1)
    df['n_switch'] = g['switch'].transform('sum').astype(int)
    df['seg'] = g['switch'].cumsum()                   # spell number: 0 before the first change

    # ------------------------------------------------------------ bins
    df['educ_bin'] = pd.cut(df['hh_avg_yrsofschool'], bins=EDUC_BINS, labels=[1, 2, 3, 4]).astype(float)
    df['size_bin'] = pd.cut(df['hh_size'], bins=SIZE_BINS, labels=[1, 2, 3, 4]).astype(float)
    df['age5'] = (np.floor(df['avg_age_hh_head'] / 5) * 5).clip(upper=85)
    df['state'] = pd.to_numeric(df['fips_state_code'], errors='coerce')
    df['county'] = pd.to_numeric(df['fips'], errors='coerce')

    # ------------------------------------------------------------ events
    ev = df[df['switch_consec'] & ~df['occ'].isin(DROP_FROM_ANALYSIS) & ~df['occ_prev'].isin(DROP_FROM_ANALYSIS)]
    ev = ev[['hhid', 'year', 'seg', 'occ_prev', 'occ']].rename(
        columns={'year': 'm_file', 'occ_prev': 'origin', 'occ': 'dest'}).reset_index(drop=True)
    ev['origin'] = ev['origin'].astype(int)
    ev['event_id'] = np.arange(1, len(ev) + 1)
    ev['emp2emp'] = (ev['origin'].isin(EMPLOYED) & ev['dest'].isin(EMPLOYED)).astype(int)
    ev['notemp'] = ((ev['origin'] == 12) | (ev['dest'] == 12)).astype(int)
    n_sw = g['switch'].first().rename('n_sw')  # placeholder, replaced below
    n_sw = df.groupby('hhid')['n_switch'].first()
    ev['single'] = (ev['hhid'].map(n_sw) == 1).astype(int)            # the household's only change
    ev['first_event'] = (ev['seg'] == 1).astype(int)                   # the household's first change
    nxt = df[df['switch']][['hhid', 'seg', 'occ']].rename(columns={'occ': 'next_code'})
    nxt['seg'] -= 1
    ev = ev.merge(nxt, on=['hhid', 'seg'], how='left')
    ev['reversed'] = (ev['next_code'] == ev['origin']).astype(int)     # a later change returns to the origin
    ev = ev.drop(columns='next_code')
    ev['event_year'] = ev['m_file'] - 1                                 # FGW r = 0

    # household-level mover type (smoothed codes)
    n_ev = ev.groupby('hhid').size()
    n_ee = ev[ev['emp2emp'] == 1].groupby('hhid').size()
    hh = pd.DataFrame({'n_switch': n_sw})
    hh['n_events_ee'] = n_ee.reindex(hh.index).fillna(0).astype(int)
    hh['n_events'] = n_ev.reindex(hh.index).fillna(0).astype(int)
    hh['mover_type'] = np.select(
        [hh['n_switch'] == 0,
         (hh['n_switch'] == 1) & (hh['n_events_ee'] == 1),
         hh['n_events_ee'] >= 1],
        [0, 1, 2], default=3)   # 0 non-mover, 1 one E->E move only, 2 E->E mover with other changes, 3 changes but no E->E event
    df = df.merge(hh[['mover_type', 'n_events', 'n_events_ee']], left_on='hhid', right_index=True, how='left')

    # r = 0 rows of any event (excluded from occupation means)
    r0 = ev[['hhid', 'event_year']].rename(columns={'event_year': 'year'}).drop_duplicates().assign(is_r0=1)
    df = df.merge(r0, on=['hhid', 'year'], how='left')
    df['is_r0'] = df['is_r0'].fillna(0).astype(int)

    # ----------------------------------------------------- analysis flag
    df['analysis'] = (~df['occ'].isin(DROP_FROM_ANALYSIS)).astype(int)

    # ------------------------------------------ occupation means y_j (FGW)
    def occ_means(d, with_se):
        rows = []
        for j, x in d.groupby('occ'):
            rec = {'occ': j, 'label': OCC_LABELS[j], 'n_hh_years': len(x), 'n_households': x['hhid'].nunique(),
                   'income_w': wmean(x['real_income'], x['projection_factor']),
                   'age_w': wmean(x['avg_age_hh_head'], x['projection_factor'])}
            for v in OUTCOMES:
                by_year = x.groupby('year').apply(lambda z: wmean(z[v], z['projection_factor']))
                rec[f'ybar_{v}'] = by_year.mean()
                if with_se:
                    rec[f'ybar_{v}_se'] = x[v].std() / np.sqrt(x['hhid'].nunique())
            rows.append(rec)
        return pd.DataFrame(rows).set_index('occ')

    base = df[(df['analysis'] == 1) & (df['is_r0'] == 0)]
    means = occ_means(base, with_se=False)
    means12 = occ_means(df[df['is_r0'] == 0], with_se=True)
    means12['income_rank'] = means12['income_w'].rank().astype(int)
    means12['hei_rank'] = means12['ybar_hei_usda'].rank().astype(int)
    means12.to_csv(OUT_DIR / 'occupation_means.csv')
    print(means12.round(3).to_string())

    for v in OUTCOMES:
        ev[f'delta_{v}'] = ev['dest'].map(means[f'ybar_{v}']) - ev['origin'].map(means[f'ybar_{v}'])
    df['income_rank'] = df['occ'].map(means12['income_rank'])
    ev['rank_orig'] = ev['origin'].map(means12['income_rank'])
    ev['rank_dest'] = ev['dest'].map(means12['income_rank'])
    ev['up_income'] = (ev['rank_dest'] > ev['rank_orig']).astype(int)

    # ------------------------------------------------- stacked event panel
    cols = ['hhid', 'year', 'seg', 'occ', 'projection_factor', 'age5', 'kids', 'hh_comp', 'avg_age_hh_head',
            'educ_bin', 'size_bin', 'state', 'county', 'real_income'] + [v for v in OUTCOMES if v != 'real_income']
    evcols = ['event_id', 'hhid', 'seg', 'm_file', 'event_year', 'origin', 'dest', 'emp2emp', 'notemp', 'single',
              'first_event', 'reversed', 'rank_orig', 'rank_dest', 'up_income'] + [f'delta_{v}' for v in OUTCOMES]
    pre = df[cols].merge(ev[evcols].assign(seg=ev['seg'] - 1), on=['hhid', 'seg'])
    post = df[cols].merge(ev[evcols], on=['hhid', 'seg'])
    st = pd.concat([pre, post], ignore_index=True)
    st['r'] = st['year'] - st['event_year']
    n_pre = st[st['r'] <= 0].groupby('event_id').size().rename('n_pre')
    n_post = st[st['r'] >= 1].groupby('event_id').size().rename('n_post')
    st = st.merge(n_pre, on='event_id', how='left').merge(n_post, on='event_id', how='left')
    st['n_pre'] = st['n_pre'].fillna(0).astype(int)
    st['n_post'] = st['n_post'].fillna(0).astype(int)
    st = st.drop(columns='seg').sort_values(['event_id', 'year']).reset_index(drop=True)
    st.to_parquet(OUT_DIR / 'movers_events_panel.parquet', index=False)
    st.to_stata(OUT_DIR / 'movers_events_panel.dta', write_index=False, version=118)

    # -------------------------------------------- household-year panel
    keep = ['hhid', 'year', 'occ', 'occ_raw', 'mover_type', 'n_events', 'n_events_ee', 'n_obs', 'n_switch',
            'switch', 'switch_consec', 'gap', 'occ_prev', 'seg', 'is_r0', 'analysis', 'educ_bin', 'size_bin',
            'age5', 'state', 'county', 'kids', 'hh_comp', 'hh_size', 'avg_age_hh_head', 'projection_factor',
            'real_income', 'income_rank'] + [v for v in OUTCOMES if v != 'real_income']
    out = df[keep].copy()
    for c in ['switch', 'switch_consec']:
        out[c] = out[c].astype(int)
    out.to_parquet(OUT_DIR / 'occupation_movers_panel.parquet', index=False)

    # ------------------------------------------------------------ summary
    print('\nhouseholds by mover type (0 never changes, 1 one employed-to-employed move only, '
          '2 employed-to-employed mover with other changes, 3 changes but no employed-to-employed event):')
    print(hh['mover_type'].value_counts().sort_index().to_dict())
    ee = ev[ev['emp2emp'] == 1]
    print(f'\nevents: {len(ev):,} dated changes; employed-to-employed {len(ee):,} in {ee.hhid.nunique():,} households '
          f'(single-change households {int(ee.single.sum()):,}, first changes {int(ee.first_event.sum()):,}, '
          f'later reversed {int(ee.reversed.sum()):,}); into/out of not employed {int(ev.notemp.sum()):,}')
    see = st[st['emp2emp'] == 1]
    print(f'stacked rows, employed-to-employed: {len(see):,}; relative-year distribution: '
          f'{see["r"].clip(-8, 8).value_counts().sort_index().to_dict()}')
    print(f'\nsaved {OUT_DIR / "occupation_movers_panel.parquet"}, {OUT_DIR / "movers_events_panel.dta"}')


if __name__ == '__main__':
    main()
