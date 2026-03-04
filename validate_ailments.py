"""
validate_ailments.py

Quick validation of dietary_ailments_by_household.parquet output.
"""

import os
import pandas as pd
import numpy as np

BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'

ailments = pd.read_parquet(os.path.join(BASE_DATA_DIR, 'interim', 'ailments',
                                        'dietary_ailments_by_household.parquet'))
panelists = pd.read_parquet(os.path.join(BASE_DATA_DIR, 'interim', 'panelists',
                                          'panelists_all_years.parquet'))

CONDITIONS = ['cholesterol', 'prediabetes', 'diabetes_type1', 'diabetes_type2',
              'heart_disease', 'hypertension', 'obesity']

print("=" * 70)
print("1. YEAR-BY-YEAR PREVALENCE RATES (%)")
print("=" * 70)
pct = ailments.groupby('panel_year')[CONDITIONS].mean().mul(100).round(1)
print(pct.to_string())

print("\n" + "=" * 70)
print("2. HH COUNTS PER YEAR (ailments vs panelists)")
print("=" * 70)
ail_counts = ailments.groupby('panel_year')['household_code'].nunique().rename('ailments_hh')
pan_counts = panelists.groupby('panel_year')['household_code'].nunique().rename('panelist_hh')
coverage = pd.concat([pan_counts, ail_counts], axis=1)
coverage['pct_covered'] = (coverage['ailments_hh'] / coverage['panelist_hh'] * 100).round(1)
print(coverage.to_string())

print("\n" + "=" * 70)
print("3. HH-YEAR OVERLAP (how many panelists have ailments data)")
print("=" * 70)
merged = panelists.merge(ailments, on=['household_code', 'panel_year'], how='left')
n_with = merged['cholesterol'].notna().sum()
n_total = len(merged)
print(f"  Panelist HH-years with ailments data: {n_with:,} / {n_total:,} ({n_with/n_total*100:.1f}%)")

print("\n" + "=" * 70)
print("4. SPOT CHECK: sample 5 HHs from a single year and show their conditions")
print("=" * 70)
sample_year = 2015
sample = ailments[ailments['panel_year'] == sample_year].head(5)
print(f"  Year {sample_year}, first 5 HHs:")
print(sample[['household_code'] + CONDITIONS].to_string(index=False))

print("\n" + "=" * 70)
print("5. CDC BENCHMARK COMPARISON (approximate US adult population)")
print("=" * 70)
cdc = {
    'cholesterol':    ('~28% diagnosed', 'self-reported lower expected'),
    'prediabetes':    ('~38% by A1C (NHANES); ~10% self-reported diagnosed', ''),
    'diabetes_type2': ('~10-11%', ''),
    'hypertension':   ('~47%', 'diagnosed + treated lower'),
    'obesity':        ('~40%', 'self-reported lower expected'),
    'heart_disease':  ('~6-8%', ''),
}
latest_year = ailments['panel_year'].max()
latest = ailments[ailments['panel_year'] == latest_year][CONDITIONS].mean().mul(100)
print(f"  Our rates ({latest_year}) vs CDC benchmarks:")
for cond, (bench, note) in cdc.items():
    if cond in latest.index:
        print(f"  {cond:<20s}: {latest[cond]:5.1f}%   CDC: {bench}  {note}")

print()
