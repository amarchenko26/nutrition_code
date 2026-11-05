"""Household-year panel for the employment-change event studies (time available for cooking).

For Anya's "what moves nutrition" review (2026-11-05). Reads final/labor_supply_reg_data.dta and
interim/health/nova_calorie_panel.parquet; geographic non-movers only (movers_f == 0).

The household's employment state is the pair of head hours bins (male_head_employment,
female_head_employment: 0 no such head, 1 under 30 h, 2 30-34 h, 3 35+ h, 9 not employed). An
event is the household's FIRST change of that pair between two consecutive panel years in which
the number of heads is unchanged and exactly one head's bin changes; the window is cut before the
pair's next change (rows from the second change on are dropped). Event types, by the changing head:
  1 retirement     employed (1-3) -> not employed (9), average head age 62 or more in the new year
  2 job loss       the same, average head age under 62
  3 re-employment  not employed -> employed
  4 hours cut      35+ h (3) -> under 35 h (1 or 2), still employed
  5 hours increase under 35 h (1 or 2) -> 35+ h
Other first changes (both heads at once, a head appearing or disappearing, 1 <-> 2) exclude the
household. Controls (evt = 0): households whose pair never changes over their observed years.
r = year - (m_file - 1), with m_file the first file year showing the new state (the fall-(m-1)
survey reported it; FGW timing as in analyze/movers).

Outcomes: hei_usda, prep_time_raw (basket preparation-time score, 1-9), upf_share (% of labelled
calories ultra-processed), cals_ae_day (calories per adult equivalent per day, complete rosters,
winsorized p1/p99), produce_share (% of calories), sugar_1000 (g per 1,000 kcal), n_trips,
spend_ae (nominal grocery spending per adult equivalent per year), real_income ($10k 2013$).
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
OUT_DIR = idir / 'what_moves'
EMP = {1, 2, 3}

cols = ['hhid', 'year', 'male_head_employment', 'female_head_employment', 'n_heads', 'avg_age_hh_head', 'kids', 'hh_comp',
        'prep_time_raw', 'n_trips', 'spend_total', 'movers_f', 'projection_factor', 'real_income', 'hei_usda', 'hi_usda',
        'sugar_per_1000cal_usda', 'produce_usda']
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
df['spend_ae'] = df['spend_total'] / df['ae_total']
df['sugar_1000'] = df['sugar_per_1000cal_usda']
df['age5'] = (np.floor(df['avg_age_hh_head'] / 5) * 5).clip(upper=85)

g = df.groupby('hhid')
df['m_prev'] = g['male_head_employment'].shift(1); df['f_prev'] = g['female_head_employment'].shift(1)
df['nh_prev'] = g['n_heads'].shift(1); df['gap'] = g['year'].diff()
df['chg'] = df['m_prev'].notna() & ((df['male_head_employment'] != df['m_prev']) | (df['female_head_employment'] != df['f_prev']))
df['chgno'] = g['chg'].cumsum()
n_chg = g['chg'].transform('sum')

def classify(row):
    if row['gap'] != 1 or row['n_heads'] != row['nh_prev']:
        return 0
    m0, m1, f0, f1 = row['m_prev'], row['male_head_employment'], row['f_prev'], row['female_head_employment']
    if (m0 == 0) != (m1 == 0) or (f0 == 0) != (f1 == 0):
        return 0
    mc, fc = m0 != m1, f0 != f1
    if mc == fc:          # both change or neither
        return 0
    p, n = (m0, m1) if mc else (f0, f1)
    if p in EMP and n == 9:
        return 1 if row['avg_age_hh_head'] >= 62 else 2
    if p == 9 and n in EMP:
        return 3
    if p == 3 and n in (1, 2):
        return 4
    if p in (1, 2) and n == 3:
        return 5
    return 0

first = df[df['chg'] & (df['chgno'] == 1)].copy()
first['evt'] = first.apply(classify, axis=1)
ev = first[first['evt'] > 0][['hhid', 'year', 'evt']].rename(columns={'year': 'm_file'})
df = df.merge(ev, on='hhid', how='left')
df['evt'] = df['evt'].fillna(pd.Series(np.where(n_chg == 0, 0, -1), index=df.index)).astype(int)     # 0 never-changer, -1 excluded
df['r'] = np.where(df['evt'] > 0, df['year'] - (df['m_file'] - 1), np.nan)
df = df[(df['evt'] == 0) | ((df['evt'] > 0) & (df['chgno'] <= 1))].copy()   # cut mover windows before the second change
df.loc[(df['evt'] > 0) & (df['chgno'] >= 2), 'r'] = np.nan
labels = {0: 'never-changers', 1: 'retirement', 2: 'job loss', 3: 're-employment', 4: 'hours cut', 5: 'hours increase'}
hh = df.groupby('hhid')['evt'].first().map(labels).value_counts()
print('households by group:', hh.to_dict())
print('movers by event: relative years observed per household (median):', df[df.evt > 0].groupby('hhid')['r'].agg(['min', 'max']).median().to_dict())
keep = ['hhid', 'year', 'evt', 'r', 'age5', 'kids', 'hh_comp', 'projection_factor', 'avg_age_hh_head', 'hei_usda', 'hi_usda', 'prep_time_raw',
        'upf_share', 'cals_ae_day', 'produce_share', 'sugar_1000', 'n_trips', 'spend_ae', 'real_income']
out = df[keep].copy()
OUT_DIR.mkdir(parents=True, exist_ok=True)
out.to_stata(OUT_DIR / 'time_events_panel.dta', write_index=False, version=118)
print('means:', out[['hei_usda', 'prep_time_raw', 'upf_share', 'cals_ae_day', 'produce_share', 'sugar_1000', 'n_trips', 'spend_ae', 'real_income']].mean().round(2).to_dict())
print(f'saved {OUT_DIR / "time_events_panel.dta"}: {len(out):,} rows')
