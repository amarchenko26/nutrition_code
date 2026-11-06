"""Household-year panel for the employment-change event studies (time available for cooking).

For Anya's "what moves nutrition" review (2026-11-05). Reads final/labor_supply_reg_data.dta,
interim/health/nova_calorie_panel.parquet and interim/what_moves/nonfood_on_food_trips.parquet;
geographic non-movers only (movers_f == 0).

The household's employment state is the pair of head hours bins (male_head_employment,
female_head_employment: 0 no such head, 1 under 30 h, 2 30-34 h, 3 35+ h, 9 not employed). A pair
that changes in year t and returns to the year t-1 pair in year t+1 (three consecutive observed
years) is set back to the t-1 pair, as the occupation-movers builder does for occupation codes
(analyze/movers/1_; on the raw pairs 6-23% of first changes are such one-year reversals). An event
is the household's FIRST change of the smoothed pair between two consecutive panel years in which
exactly one head's bin changes and no head appears or disappears; the window is cut before the
pair's next change. Event types, by the changing head:
  1 retirement     employed (1-3) -> not employed (9), average head age 62 or more in the new year
  2 job loss       the same, average head age under 62
  3 re-employment  not employed -> employed
  4 hours cut      35+ h (3) -> under 35 h (1 or 2), still employed
  5 hours increase under 35 h (1 or 2) -> 35+ h
Other first changes (both heads at once, a head appearing or disappearing, 1 <-> 2) exclude the
household. Controls (evt = 0): households whose smoothed pair never changes.
r = year - (m_file - 1), with m_file the first file year showing the new state (the fall-(m-1)
survey reported it; FGW timing as in analyze/movers). who = 1 male head changes, 2 female head.

balanced = 1 for movers observed in every year r = -2, ..., 2 (the new state then lasts at least
the two file years r = 1, 2). basecell = the household's employment state at its first observation
(1 all present heads 35+ h, 2 all present heads employed with some under 35 h, 3 some head employed
and some not, 4 no head employed) x 10-year bin of average head age at first observation; tenure =
years since the household's first observed year, capped at 8.

Outcomes: hei_usda, prep_time_raw (basket preparation-time score, 1-9), upf_share (% of
calories ultra-processed), cals_ae_day (calories per adult equivalent per day, complete rosters,
winsorized p1/p99), produce_share (% of calories), sugar_1000 (g per 1,000 kcal), n_trips (trips
with a food item), spend_ae (real 2013$ food spending per adult equivalent per year), real_income
($10k 2013$), nonfood_ae (real 2013$ non-food spending on food trips per adult equivalent per
year), food_share_trip (% of real trip totals that is food).
Output: interim/what_moves/time_events_panel.dta
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

IN = gdir / 'labor_supply_reg_data.dta'
NOVA = idir / 'health' / 'nova_calorie_panel.parquet'
NONFOOD = idir / 'what_moves' / 'nonfood_on_food_trips.parquet'
OUT_DIR = idir / 'what_moves'
EMP = {1, 2, 3}


def smooth_reversals(hh, yr, state):
    """state_t -> state_{t-1} when state_t != state_{t-1} and state_{t+1} == state_{t-1}, with t-1, t, t+1
    consecutive observed years of the same household (sequential, as smooth_reversals in analyze/movers/1_)."""
    out = state.copy()
    n = 0
    for i in range(1, len(state) - 1):
        if hh[i - 1] == hh[i] == hh[i + 1] and yr[i] - yr[i - 1] == 1 and yr[i + 1] - yr[i] == 1 \
                and state[i] != out[i - 1] and state[i + 1] == out[i - 1]:
            out[i] = out[i - 1]
            n += 1
    return out, n


def classify(row):
    """Returns (event type, who)."""
    if row['gap'] != 1:
        return 0, 0
    m0, m1, f0, f1 = row['m_prev'], row['m_now'], row['f_prev'], row['f_now']
    if (m0 == 0) != (m1 == 0) or (f0 == 0) != (f1 == 0):
        return 0, 0
    mc, fc = m0 != m1, f0 != f1
    if mc == fc:
        return 0, 0
    p, n, who = (m0, m1, 1) if mc else (f0, f1, 2)
    if p in EMP and n == 9:
        return (1 if row['avg_age_hh_head'] >= 62 else 2), who
    if p == 9 and n in EMP:
        return 3, who
    if p == 3 and n in (1, 2):
        return 4, who
    if p in (1, 2) and n == 3:
        return 5, who
    return 0, 0


def main():
    cols = ['hhid', 'year', 'male_head_employment', 'female_head_employment', 'avg_age_hh_head', 'kids', 'hh_comp', 'prep_time_raw', 'n_trips',
            'spend_total', 'movers_f', 'projection_factor', 'real_income', 'hei_usda', 'hi_usda', 'sugar_per_1000cal_usda']
    df, _ = pyreadstat.read_dta(str(IN), usecols=cols)
    df['hhid'] = df['hhid'].astype('int64'); df['year'] = df['year'].astype(int)
    df = df[df['movers_f'] == 0].sort_values(['hhid', 'year']).reset_index(drop=True)
    print(f'geographic non-movers: {len(df):,} household-years, {df.hhid.nunique():,} households')

    nova = pd.read_parquet(NOVA, columns=['household_code', 'panel_year', 'cals_all', 'cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4',
                                          'produce_cals_all', 'ae_total', 'ae_complete'])
    nova = nova[nova['ae_complete'] == 1].copy()
    nova['cals_ae_day'] = nova['cals_all'] / nova['ae_total'] / 365
    lo, hi = nova['cals_ae_day'].quantile([0.01, 0.99]); nova['cals_ae_day'] = nova['cals_ae_day'].clip(lo, hi)
    nova['upf_share'] = 100 * nova['cals_nova4'] / nova[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
    nova['produce_share'] = 100 * nova['produce_cals_all'] / nova['cals_all']
    nova = nova.rename(columns={'household_code': 'hhid', 'panel_year': 'year'})[['hhid', 'year', 'cals_ae_day', 'upf_share', 'produce_share', 'ae_total']]
    nova['hhid'] = nova['hhid'].astype('int64'); nova['year'] = nova['year'].astype(int)
    df = df.merge(nova, on=['hhid', 'year'], how='left')
    nf = pd.read_parquet(NONFOOD).rename(columns={'household_code': 'hhid', 'panel_year': 'year'})
    nf['hhid'] = nf['hhid'].astype('int64'); nf['year'] = nf['year'].astype(int)
    df = df.merge(nf[['hhid', 'year', 'food_real', 'trip_total_real', 'nonfood_real']], on=['hhid', 'year'], how='left')
    df['spend_ae'] = df['spend_total'] / df['ae_total']
    df['nonfood_ae'] = df['nonfood_real'] / df['ae_total']
    for c in ['spend_ae', 'nonfood_ae']:                 # winsorize the two per-AE dollar outcomes at p1/p99, as calories
        l, h = df[c].quantile([0.01, 0.99]); df[c] = df[c].clip(l, h)
    df['food_share_trip'] = 100 * df['food_real'] / df['trip_total_real']
    df['sugar_1000'] = df['sugar_per_1000cal_usda']
    df['age5'] = (np.floor(df['avg_age_hh_head'] / 5) * 5).clip(upper=85)

    # ------------------------------------------------ smoothed employment state
    df['pair_raw'] = df['male_head_employment'].astype(int) * 100 + df['female_head_employment'].astype(int)
    smoothed, n_blip = smooth_reversals(df['hhid'].values, df['year'].values, df['pair_raw'].values)
    df['pair'] = smoothed
    print(f'one-year reversals set back: {n_blip:,} household-years ({100 * n_blip / len(df):.2f}% of rows)')
    df['m_now'] = df['pair'] // 100; df['f_now'] = df['pair'] % 100
    g = df.groupby('hhid')
    df['m_prev'] = g['m_now'].shift(1); df['f_prev'] = g['f_now'].shift(1); df['gap'] = g['year'].diff()
    df['chg'] = df['m_prev'].notna() & (df['pair'] != g['pair'].shift(1))
    df['chgno'] = g['chg'].cumsum()
    n_chg = g['chg'].transform('sum')

    first = df[df['chg'] & (df['chgno'] == 1)].copy()
    res = first.apply(classify, axis=1, result_type='expand')
    first['evt'], first['who'] = res[0], res[1]
    print('first changes by type (0 = other, household excluded):', first['evt'].value_counts().sort_index().to_dict())
    ev = first[first['evt'] > 0][['hhid', 'year', 'evt', 'who']].rename(columns={'year': 'm_file'})
    df = df.merge(ev, on='hhid', how='left')
    df['evt'] = df['evt'].fillna(pd.Series(np.where(n_chg == 0, 0, -1), index=df.index)).astype(int)     # 0 never-changer, -1 excluded
    df['who'] = df['who'].fillna(0).astype(int)
    df['r'] = np.where(df['evt'] > 0, df['year'] - (df['m_file'] - 1), np.nan)

    # baseline cell and tenure, from the household's first observed row (before any cut)
    f = df.sort_values(['hhid', 'year']).groupby('hhid').first()
    m, fm = f['m_now'], f['f_now']
    present_emp = ((m.isin(EMP)).astype(int) + (fm.isin(EMP)).astype(int))
    present_ft = ((m == 3).astype(int) + (fm == 3).astype(int))
    present = ((m != 0).astype(int) + (fm != 0).astype(int))
    state = np.select([present_ft == present, present_emp == present, present_emp > 0], [1, 2, 3], default=4)
    age10 = (np.floor(f['avg_age_hh_head'].fillna(50) / 10) * 10).clip(20, 70).astype(int)
    base = pd.DataFrame({'base_state': state, 'base_age10': age10, 'first_year': f['year']}, index=f.index)
    base['basecell'] = base['base_state'] * 100 + base['base_age10']
    df = df.merge(base, left_on='hhid', right_index=True)
    df['tenure'] = (df['year'] - df['first_year']).clip(upper=8)

    df = df[(df['evt'] == 0) | ((df['evt'] > 0) & (df['chgno'] <= 1))].copy()   # cut mover windows before the second change
    cov = df[(df['evt'] > 0) & df['r'].between(-2, 2)].groupby('hhid')['r'].nunique()
    df['balanced'] = df['hhid'].map(cov).eq(5).astype(int)
    labels = {0: 'never-changers', 1: 'retirement', 2: 'job loss', 3: 're-employment', 4: 'hours cut', 5: 'hours increase'}
    hhs = df.groupby('hhid').agg(evt=('evt', 'first'), who=('who', 'first'), balanced=('balanced', 'first'))
    print('households by group:', hhs['evt'].map(labels).value_counts().to_dict())
    print('movers by event x changing head (1 male, 2 female):', pd.crosstab(hhs[hhs.evt > 0]['evt'].map(labels), hhs[hhs.evt > 0]['who']).to_dict())
    print('balanced movers by event:', hhs[(hhs.evt > 0) & (hhs.balanced == 1)]['evt'].map(labels).value_counts().to_dict())
    keep = ['hhid', 'year', 'evt', 'who', 'r', 'balanced', 'basecell', 'tenure', 'age5', 'kids', 'hh_comp', 'projection_factor', 'avg_age_hh_head', 'hei_usda', 'hi_usda',
            'prep_time_raw', 'upf_share', 'cals_ae_day', 'produce_share', 'sugar_1000', 'n_trips', 'spend_ae', 'real_income', 'nonfood_ae', 'food_share_trip']
    out = df[keep].copy()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out.to_stata(OUT_DIR / 'time_events_panel.dta', write_index=False, version=118)
    print('means:', out[['hei_usda', 'prep_time_raw', 'upf_share', 'cals_ae_day', 'produce_share', 'sugar_1000', 'n_trips', 'spend_ae', 'real_income', 'nonfood_ae', 'food_share_trip']].mean().round(2).to_dict())
    print(f'saved {OUT_DIR / "time_events_panel.dta"}: {len(out):,} rows')


if __name__ == '__main__':
    main()
