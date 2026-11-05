"""Household-year panel for the health-diagnosis event studies (what moves nutrition, 2026-11-05).

Two kinds of event, for type-2 diabetes, hypertension, high cholesterol, heart disease, obesity:
  FIRST REPORT (m_<cond>): the first survey year the household reports the condition after two or more
    earlier survey years without it, first report in 2013-2019 (interim/ailments/ailments_households.parquet).
    conf_<cond> = 1 if the household reports it again in its next survey, 0 if not, missing if no later survey.
  DATED NEW DIAGNOSIS (t_<cond>): a member answers "less than a year ago" to "About how long ago was this
    household member diagnosed?" in the 2011-2015 survey t, every member selecting the condition that year
    gives that answer, and the household did not report the condition in any earlier survey
    (interim/ailments/ailments_diagnosis_timing.parquet, clean_ailments_diagnosis_timing.py).
Clock, the same for both: the survey is fielded in January-March of its file year, so a diagnosis first seen in
year s happened in about calendar year s-1. r = year - (s - 1): r = 0 is the diagnosis year (partly before, partly
after), r = -1 the last full year before it, r = 1 the first full year after it.
Controls (grp_<cond> = 0): households that answered the survey in 2+ years and never report the condition.
Outcomes: HEI, sugar g and sodium mg per 1,000 kcal, soda share of grocery spending %, trips and grocery spending from
final/labor_supply_reg_data.dta (on 2026-11-05 its HEI is still the version from before the 2026-11-04 fatty-acid fix; the
outcome rebuild must refresh it, or the pre-2009 years are wrong); calories per adult equivalent per day at
the UPC-dictionary densities (complete rosters, winsorized p1/p99), ultra-processed share of labelled calories %, produce
share of calories % from interim/health/nova_calorie_panel.parquet; products a diabetes diagnosis should move, from
interim/purchases_food: regular soda ounces per week (module 1484, winsorized p99), diet share of soda ounces % among soda
buyers (1553 / (1484 + 1553)), any sugar substitute bought % (1535), any dietetic candy bought % (1533, 1536).
Output: interim/what_moves/diagnosis_panel.dta (households in an event group or the control group of some condition)
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

CONDS = {'diabetes': 'hh_diabetes_type2', 'hypertension': 'hh_hypertension', 'cholesterol': 'hh_cholesterol',
         'heart': 'hh_heart_disease', 'obesity': 'hh_obesity'}
EVENT_YEARS = (2013, 2019)
OZ = {'OZ': 1.0, 'FL OZ': 1.0, 'QT': 32.0, 'GAL': 128.0, 'LT': 33.814, 'ML': 0.033814, 'PT': 16.0}

cols = ['hhid', 'year', 'hei_usda', 'sugar_per_1000cal_usda', 'sodium_per_1000cal_usda', 'spend_soda', 'spend_total', 'n_trips',
        'kids', 'hh_comp', 'avg_age_hh_head', 'projection_factor']
df, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'), usecols=cols)
df['hhid'] = df['hhid'].astype('int64'); df['year'] = df['year'].astype(int)
df['sodium_1000'] = 1000 * df['sodium_per_1000cal_usda']
df['sugar_1000'] = df['sugar_per_1000cal_usda']
df['soda_share'] = 100 * df['spend_soda'] / df['spend_total']
df['age5'] = (np.floor(df['avg_age_hh_head'] / 5) * 5).clip(upper=85)
df = df.drop(columns=['sodium_per_1000cal_usda', 'sugar_per_1000cal_usda', 'spend_soda', 'avg_age_hh_head'])

nova = pd.read_parquet(idir / 'health' / 'nova_calorie_panel.parquet',
                       columns=['household_code', 'panel_year', 'cals_all', 'cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4',
                                'produce_cals_all', 'ae_total', 'ae_complete'])
nova = nova[nova['ae_complete'] == 1].copy()
nova['cals_ae_day'] = nova['cals_all'] / nova['ae_total'] / 365
lo, hi = nova['cals_ae_day'].quantile([0.01, 0.99]); nova['cals_ae_day'] = nova['cals_ae_day'].clip(lo, hi)
nova['upf_share'] = 100 * nova['cals_nova4'] / nova[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
nova['produce_share'] = 100 * nova['produce_cals_all'] / nova['cals_all']
nova = nova.rename(columns={'household_code': 'hhid', 'panel_year': 'year'})[['hhid', 'year', 'cals_ae_day', 'upf_share', 'produce_share']]
nova['hhid'] = nova['hhid'].astype('int64'); nova['year'] = nova['year'].astype(int)
df = df.merge(nova, on=['hhid', 'year'], how='left')

# ------------------------------------------------ products a diabetes diagnosis should move
parts, unknown = [], 0.0
for y in range(2004, 2021):
    p = pd.read_parquet(idir / 'purchases_food' / f'panel_year={y}',
                        columns=['household_code', 'product_module_code', 'quantity', 'multi', 'size1_amount', 'size1_units'],
                        filters=[('product_module_code', 'in', [1484, 1553, 1535, 1533, 1536])])
    p['oz'] = p['quantity'] * p['multi'].fillna(1).clip(lower=1) * p['size1_amount'] * p['size1_units'].map(OZ)
    s = p[p['product_module_code'].isin([1484, 1553])]
    unknown += s['oz'].isna().sum()
    a = pd.DataFrame({
        'reg_oz': s[s['product_module_code'] == 1484].groupby('household_code')['oz'].sum(),
        'diet_oz': s[s['product_module_code'] == 1553].groupby('household_code')['oz'].sum(),
        'sweetener': p[p['product_module_code'] == 1535].groupby('household_code').size(),
        'candy': p[p['product_module_code'].isin([1533, 1536])].groupby('household_code').size()}).reset_index()
    a['year'] = y
    parts.append(a)
print(f'soda rows with a size unit not convertible to ounces: {int(unknown):,}')
prod = pd.concat(parts, ignore_index=True).rename(columns={'household_code': 'hhid'})
prod['hhid'] = prod['hhid'].astype('int64')
df = df.merge(prod, on=['hhid', 'year'], how='left')
df[['reg_oz', 'diet_oz', 'sweetener', 'candy']] = df[['reg_oz', 'diet_oz', 'sweetener', 'candy']].fillna(0)
df['reg_soda_oz_wk'] = (df['reg_oz'] / 52).clip(upper=(df['reg_oz'] / 52).quantile(0.99))
df['diet_share'] = np.where(df['reg_oz'] + df['diet_oz'] > 0, 100 * df['diet_oz'] / (df['reg_oz'] + df['diet_oz']), np.nan)
df['sweetener_any'] = 100.0 * (df['sweetener'] > 0)
df['diet_candy_any'] = 100.0 * (df['candy'] > 0)
df = df.drop(columns=['reg_oz', 'diet_oz', 'sweetener', 'candy'])

# ------------------------------------------------ events
a = pd.read_parquet(idir / 'ailments' / 'ailments_households.parquet').rename(columns={'household_code': 'hhid', 'panel_year': 'year'})
a['hhid'] = a['hhid'].astype('int64')
a = a.sort_values(['hhid', 'year'])
tm = pd.read_parquet(idir / 'ailments' / 'ailments_diagnosis_timing.parquet').rename(columns={'household_code': 'hhid', 'panel_year': 'year'})
hh = pd.DataFrame({'n_surveys': a.groupby('hhid').size()})
checks = []
for name, col in CONDS.items():
    first_on = a[a[col] == 1].groupby('hhid')['year'].min().rename('m')
    b = a.merge(first_on, on='hhid')
    clean_before = b[b['year'] < b['m']].groupby('hhid').size().rename('n_clean_before')
    ev = pd.concat([first_on, clean_before], axis=1).fillna({'n_clean_before': 0})
    ever = a.groupby('hhid')[col].max()
    hh[f'm_{name}'] = ev['m'].where((ev['n_clean_before'] >= 2) & ev['m'].between(*EVENT_YEARS))
    nxt = b[b['year'] > b['m']].sort_values(['hhid', 'year']).groupby('hhid')[col].first()
    hh[f'conf_{name}'] = nxt.reindex(hh.index).where(hh[f'm_{name}'].notna())
    # dated new diagnoses: every member selecting the condition that year says "less than a year ago"
    t = tm[tm['condition'] == name]
    by = t.groupby(['hhid', 'year'])['recency'].agg(['min', 'max', 'count']).reset_index()
    new = by[(by['min'] == 1) & (by['max'] == 1)].merge(first_on, left_on='hhid', right_index=True, how='left')
    new = new[new['year'] == new['m']]            # no report of the condition in any earlier survey
    hh[f't_{name}'] = new.groupby('hhid')['year'].min().reindex(hh.index)
    hh[f'grp_{name}'] = np.where(ever.reindex(hh.index) == 0, np.where(hh['n_surveys'] >= 2, 0.0, np.nan), np.nan)
    # how good is a first report? recency answer in the first-report survey, 2013-2015
    fr = t.merge(hh[[f'm_{name}']].dropna().rename(columns={f'm_{name}': 'm'}), left_on=['hhid', 'year'], right_on=['hhid', 'm'])
    best = fr.groupby('hhid')['recency'].min()
    # all prevalent selections in the same surveys, for comparison
    checks.append(dict(condition=name, first_reports_2013_19=int(hh[f'm_{name}'].notna().sum()), repeat_next=round(100 * hh[f'conf_{name}'].mean(), 1), first_reports_2013_15=int(best.notna().sum()),
                       lt1yr=round(100 * (best == 1).mean(), 1), yrs1_2=round(100 * (best == 2).mean(), 1), yrs3_4=round(100 * (best == 3).mean(), 1), yrs5plus=round(100 * (best == 4).mean(), 1),
                       dated_events=int(hh[f't_{name}'].notna().sum()), controls=int((hh[f'grp_{name}'] == 0).sum()),
                       diet_new=round(100 * t[t['recency'] == 1]['diet'].eq(1).mean(), 1), diet_5plus=round(100 * t[t['recency'] == 4]['diet'].eq(1).mean(), 1),
                       rx_new=round(100 * t[t['recency'] == 1]['rx'].eq(1).mean(), 1), notreat_new=round(100 * t[t['recency'] == 1]['no_treat'].eq(1).mean(), 1)))
pd.set_option('display.width', 220)
print(pd.DataFrame(checks).to_string(index=False))
(idir / 'what_moves').mkdir(parents=True, exist_ok=True)
pd.DataFrame(checks).to_csv(idir / 'what_moves' / 'diagnosis_event_quality.csv', index=False)   # read by figure_diagnosis_events.py

df = df.merge(hh.reset_index(), on='hhid', how='inner')
keep_hh = df[[f'grp_{n}' for n in CONDS] + [f'm_{n}' for n in CONDS] + [f't_{n}' for n in CONDS]].notna().any(axis=1)
df = df[keep_hh].copy()
out = idir / 'what_moves' / 'diagnosis_panel.dta'
out.parent.mkdir(parents=True, exist_ok=True)
df.to_stata(out, write_index=False, version=118)
print(f'saved {len(df):,} household-years, {df.hhid.nunique():,} households to {out}')
print(df[['hei_usda', 'sugar_1000', 'sodium_1000', 'soda_share', 'n_trips', 'spend_total', 'cals_ae_day', 'upf_share', 'produce_share', 'reg_soda_oz_wk', 'diet_share', 'sweetener_any', 'diet_candy_any']].describe().T[['count', 'mean', 'std']].round(2).to_string())
