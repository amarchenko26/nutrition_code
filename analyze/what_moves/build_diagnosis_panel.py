"""Household-year panel for the health-diagnosis event studies (what moves nutrition, 2026-11-05).

Event: the FIRST panel year a household reports a condition in the Nielsen ailments survey after at
least two earlier survey years without it (2011 onward), for type-2 diabetes, hypertension, high
cholesterol, heart disease and obesity (interim/ailments/ailments_households.parquet: household-level
flags, one questionnaire per member in 2011-2015 and one household respondent from 2016). r = year
minus the first-report year. Controls: households that answered the survey in at least two years and
never report the condition. Households whose first report has fewer than two clean survey years
before it (left-censored) are in neither group. Events with a first report in 2013-2019 are kept so
each has at least one post year of purchases (outcomes run 2004-2020).
Outcomes: from final/labor_supply_reg_data.dta (HEI, simple USDA index, sugar g and sodium mg per 1,000
kcal, soda share of grocery spending %) and interim/health/nova_calorie_panel.parquet (calories per
adult equivalent per day at the UPC-dictionary densities, complete rosters, winsorized at p1/p99;
ultra-processed share of labelled calories %; produce share of calories %).
Output: interim/what_moves/diagnosis_panel.dta
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

cols = ['hhid', 'year', 'hei_usda', 'hi_usda', 'sugar_per_1000cal_usda', 'sodium_per_1000cal_usda', 'spend_soda', 'spend_total',
        'kids', 'hh_comp', 'avg_age_hh_head', 'fips', 'projection_factor', 'movers_f']
df, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'), usecols=cols)
df['hhid'] = df['hhid'].astype('int64'); df['year'] = df['year'].astype(int)
df['sodium_1000'] = 1000 * df['sodium_per_1000cal_usda']
df['sugar_1000'] = df['sugar_per_1000cal_usda']
df['soda_share'] = 100 * df['spend_soda'] / df['spend_total']
df['age5'] = (np.floor(df['avg_age_hh_head'] / 5) * 5).clip(upper=85)
df = df.drop(columns=['sodium_per_1000cal_usda', 'sugar_per_1000cal_usda', 'spend_soda', 'spend_total'])

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

a = pd.read_parquet(idir / 'ailments' / 'ailments_households.parquet').rename(columns={'household_code': 'hhid', 'panel_year': 'year'})
a = a.sort_values(['hhid', 'year'])
n_surveys = a.groupby('hhid').size().rename('n_surveys')
hh = pd.DataFrame({'n_surveys': n_surveys})
for name, col in CONDS.items():
    first_on = a[a[col] == 1].groupby('hhid')['year'].min().rename('m')
    clean_before = a.merge(first_on, on='hhid').query('year < m').groupby('hhid').size().rename('n_clean_before')
    ev = pd.concat([first_on, clean_before], axis=1).fillna({'n_clean_before': 0})
    ever = a.groupby('hhid')[col].max()
    hh[f'm_{name}'] = ev['m'].where((ev['n_clean_before'] >= 2) & ev['m'].between(*EVENT_YEARS))
    hh[f'grp_{name}'] = np.where(hh[f'm_{name}'].notna(), 1.0, np.where((ever.reindex(hh.index) == 0) & (hh['n_surveys'] >= 2), 0.0, np.nan))
    print(f"{name}: events {int((hh[f'grp_{name}'] == 1).sum()):,} (first reports 2013-2019 with 2+ clean survey years before), "
          f"never-reporters with 2+ surveys {int((hh[f'grp_{name}'] == 0).sum()):,}")
df = df.merge(hh.reset_index(), on='hhid', how='left')
for name in CONDS:
    df[f'r_{name}'] = df['year'] - df[f'm_{name}']
keep = df[[c for c in df.columns]].copy()
out = idir / 'what_moves' / 'diagnosis_panel.dta'
out.parent.mkdir(parents=True, exist_ok=True)
keep.to_stata(out, write_index=False, version=118)
print(f'saved {len(keep):,} household-years to {out}')
