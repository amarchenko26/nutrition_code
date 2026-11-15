"""
build_jaravel_ssiv.py  --  run LOCALLY

Constructs the Jaravel (2018) SSIV for product modules:

    Z_m = Σ_h  s_{hm,0} × g_h

where:
  h         = demographic group  (age × education × race × children × state)
  s_{hm,0}  = baseline (2004-2006) share of module m's spending from group h  (HMS)
  g_h       = annualized log change in group-h household count               (ACS)
              from early period (2000-2004 avg) to late period (2012-2016 avg)

Demographic bin definitions (chosen to match HMS panel codes exactly):

  Age (9 bins): <25 | 25-29 | 30-34 | 35-39 | 40-44 | 45-49 | 50-54 | 55-64 | 65+
      HMS: male_head_age codes 1-9 (fall back to female if no male head)
      ACS: continuous AGE variable, same bin edges

  Education (5 bins): lt_hs | hs | some_col | col | post_col
      HMS: male_head_education codes 1-6 (1-2→lt_hs, 3→hs, 4→some_col, 5→col, 6→post_col)
      ACS: EDUCD codes (≤61→lt_hs, 62-64→hs, 65-100→some_col, 101→col, ≥110→post_col)

  Race (3 bins): white | black | other
      HMS: race codes (1→white, 2→black, 3-4→other)
      ACS: RACE codes (1→white, 2→black, 3-9→other)

  Children (2 bins): has_children | no_children
      HMS: age_and_presence_of_children (9→no_children, 1-8→has_children)
      ACS: NCHILD (0→no_children, ≥1→has_children)

  State: numeric FIPS state code (same in both HMS and ACS)

Output:
  interim/panel_dataset/jaravel_ssiv.dta
    product_module_code  iv_jaravel  n_groups  coverage
"""

import pandas as pd
import numpy as np
from pathlib import Path

import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
BASE  = data
ACS   = BASE / 'raw' / 'acs' / 'usa_00003.csv'
PAN   = BASE / 'interim' / 'panelists' / 'panelists_all_years.parquet'
PURCH = BASE / 'interim' / 'purchases_food'
OUT   = BASE / 'interim' / 'panel_dataset' / 'jaravel_ssiv.dta'

BASELINE_YEARS = [2004, 2005, 2006]
ACS_EARLY      = (2000, 2004)
ACS_LATE       = (2012, 2016)
MIN_HH_CELL    = 5   # drop (group, module) cells with fewer HMS households

def log(msg): print(msg, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# BIN MAPPERS
# ─────────────────────────────────────────────────────────────────────────────

def acs_age_bin(age):
    """Continuous ACS age → integer bin 1-9 matching HMS male_head_age codes."""
    if age < 25:   return 1
    elif age <= 29: return 2
    elif age <= 34: return 3
    elif age <= 39: return 4
    elif age <= 44: return 5
    elif age <= 49: return 6
    elif age <= 54: return 7
    elif age <= 64: return 8
    else:           return 9

def acs_educ_bin(educd):
    """IPUMS EDUCD → 5 education labels matching HMS education codes."""
    if educd <= 61:    return 'lt_hs'
    elif educd <= 64:  return 'hs'
    elif educd <= 100: return 'some_col'
    elif educd == 101: return 'col'
    else:              return 'post_col'   # 110+

def acs_race_bin(race):
    if race == 1:  return 'white'
    elif race == 2: return 'black'
    else:           return 'other'

def acs_children_bin(nchild):
    return 'no_children' if nchild == 0 else 'has_children'

def hms_age_bin(male_age, female_age):
    """HMS age codes 1-9 (0 = no head). Use male, fall back to female."""
    m = int(male_age)   if not pd.isna(male_age)   else 0
    f = int(female_age) if not pd.isna(female_age) else 0
    v = m if m > 0 else f
    return v if v > 0 else np.nan

def hms_educ_bin(male_educ, female_educ):
    """HMS education codes 1-6 (0 = no head / unknown). Male-primary, female fallback."""
    m = int(male_educ)   if not pd.isna(male_educ)   else 0
    f = int(female_educ) if not pd.isna(female_educ) else 0
    v = m if m > 0 else f
    mapping = {1: 'lt_hs', 2: 'lt_hs', 3: 'hs', 4: 'some_col', 5: 'col', 6: 'post_col'}
    return mapping.get(v, None)

def hms_race_bin(r):
    mapping = {1: 'white', 2: 'black', 3: 'other', 4: 'other'}
    return mapping.get(int(r) if not pd.isna(r) else 0, None)

def hms_children_bin(c):
    v = int(c) if not pd.isna(c) else 0
    if v == 9:   return 'no_children'
    elif v > 0:  return 'has_children'
    else:        return None


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1  ACS — compute g_h
# ─────────────────────────────────────────────────────────────────────────────
log("=" * 65)
log("STEP 1: ACS  →  g_h (annualized log population change)")
log("=" * 65)

acs_cols = ['YEAR', 'GQ', 'RELATE', 'HHWT',
            'AGE', 'EDUCD', 'RACE', 'NCHILD', 'STATEFIP']
acs = pd.read_csv(ACS, usecols=acs_cols)
log(f"  ACS rows loaded: {len(acs):,}")

# Keep household heads in non-institutional housing only
acs = acs[(acs['RELATE'] == 1) & (acs['GQ'].isin([1, 2]))]
log(f"  After filter (heads, non-institutional): {len(acs):,}")

# Assign period
acs = acs[acs['YEAR'].between(*ACS_EARLY) | acs['YEAR'].between(*ACS_LATE)].copy()
acs['period'] = np.where(acs['YEAR'].between(*ACS_EARLY), 'early', 'late')

# Map to bins
acs['age_bin']      = acs['AGE'].map(acs_age_bin)
acs['educ_bin']     = acs['EDUCD'].map(acs_educ_bin)
acs['race_bin']     = acs['RACE'].map(acs_race_bin)
acs['children_bin'] = acs['NCHILD'].map(acs_children_bin)
acs['state']        = acs['STATEFIP'].astype(int)

acs['group_h'] = (acs['age_bin'].astype(str)      + '|' +
                  acs['educ_bin'].astype(str)      + '|' +
                  acs['race_bin'].astype(str)      + '|' +
                  acs['children_bin'].astype(str)  + '|' +
                  acs['state'].astype(str))

# Household count = sum of HHWT (for household heads, HHWT = # HHs each represents)
pop_yr = (acs.groupby(['group_h', 'YEAR'])['HHWT']
          .sum().reset_index(name='pop'))

pop_yr['period'] = np.where(
    pop_yr['YEAR'].between(*ACS_EARLY), 'early', 'late')

# Average household count within each 5-year window
pop_period = (pop_yr.groupby(['group_h', 'period'])['pop']
              .mean().unstack('period'))

pop_period = pop_period.dropna(subset=['early', 'late'])
pop_period = pop_period[(pop_period['early'] > 0) & (pop_period['late'] > 0)]

# g_h = annualized log change (12-year gap between window midpoints: 2002 → 2014)
pop_period['g_h'] = (np.log(pop_period['late']) - np.log(pop_period['early'])) / 12.0

g_h = pop_period[['g_h']].reset_index()
log(f"  Groups with valid g_h: {len(g_h):,}")
log(f"  g_h:  mean={g_h['g_h'].mean():.5f}  sd={g_h['g_h'].std():.5f}  "
    f"p5={g_h['g_h'].quantile(.05):.4f}  p95={g_h['g_h'].quantile(.95):.4f}")

del acs, pop_yr, pop_period


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2  HMS — compute s_{hm,0}
# ─────────────────────────────────────────────────────────────────────────────
log("")
log("=" * 65)
log("STEP 2: HMS (2004-2006)  →  s_{hm,0} (spending shares by group)")
log("=" * 65)

# 2a. Load panelists, assign each household to group h
log("  Loading panelists 2004-2006...")
pan = pd.read_parquet(PAN, columns=[
    'household_code', 'panel_year',
    'male_head_age',   'female_head_age',
    'male_head_education', 'female_head_education',
    'race', 'age_and_presence_of_children', 'fips_state_code',
])
pan = pan[pan['panel_year'].isin(BASELINE_YEARS)].copy()

pan['age_bin'] = pan.apply(
    lambda r: hms_age_bin(r['male_head_age'], r['female_head_age']), axis=1)
pan['educ_bin'] = pan.apply(
    lambda r: hms_educ_bin(r['male_head_education'], r['female_head_education']), axis=1)
pan['race_bin']     = pan['race'].apply(hms_race_bin)
pan['children_bin'] = pan['age_and_presence_of_children'].apply(hms_children_bin)
pan['state']        = pd.to_numeric(pan['fips_state_code'], errors='coerce')

pan = pan.dropna(subset=['age_bin', 'educ_bin', 'race_bin', 'children_bin', 'state'])
pan['age_bin'] = pan['age_bin'].astype(int)
pan['state']   = pan['state'].astype(int)

pan['group_h'] = (pan['age_bin'].astype(str)      + '|' +
                  pan['educ_bin'].astype(str)      + '|' +
                  pan['race_bin'].astype(str)      + '|' +
                  pan['children_bin'].astype(str)  + '|' +
                  pan['state'].astype(str))

pan_groups = pan[['household_code', 'panel_year', 'group_h']].copy()
log(f"  Panelists assigned to groups: {len(pan_groups):,}")
log(f"  Unique groups h in HMS:       {pan_groups['group_h'].nunique():,}")

# 2b. Load purchases 2004-2006, aggregate to (household_code, year, module)
log("  Loading purchases 2004-2006...")
purch_frames = []
for yr in BASELINE_YEARS:
    pdir = PURCH / f'panel_year={yr}'
    for fp in pdir.glob('*.parquet'):
        df = pd.read_parquet(fp, columns=['household_code', 'product_module_code',
                                          'total_price_paid'])
        df['panel_year'] = yr
        purch_frames.append(
            df.groupby(['household_code', 'panel_year', 'product_module_code'])
              ['total_price_paid'].sum().reset_index()
        )
    log(f"    {yr} loaded")

purchases = pd.concat(purch_frames, ignore_index=True)
del purch_frames
log(f"  Purchase records: {len(purchases):,}")

# 2c. Merge purchases with group assignments
merged = purchases.merge(pan_groups, on=['household_code', 'panel_year'], how='inner')
log(f"  Matched purchase-group records: {len(merged):,}")
del purchases, pan_groups

# 2d. Aggregate to (group_h, module): sum spending over all three years
spend_hm = (merged.groupby(['group_h', 'product_module_code'])
            .agg(spend_hm=('total_price_paid', 'sum'),
                 n_hh=('household_code', 'nunique'))
            .reset_index())

# 2e. Drop (group, module) cells with too few HMS households (noisy shares)
spend_hm = spend_hm[spend_hm['n_hh'] >= MIN_HH_CELL]

# 2f. Total spending per module across all qualifying groups
spend_m = (spend_hm.groupby('product_module_code')['spend_hm']
           .sum().rename('spend_m').reset_index())

shares = spend_hm.merge(spend_m, on='product_module_code')
shares['s_hm'] = shares['spend_hm'] / shares['spend_m']

log(f"  (group, module) cells with s_hm: {len(shares):,}")
log(f"  Modules covered:                 {shares['product_module_code'].nunique():,}")
log(f"  Groups h in spending data:       {shares['group_h'].nunique():,}")
del merged, spend_hm, spend_m


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3  Merge g_h → compute Z_m
# ─────────────────────────────────────────────────────────────────────────────
log("")
log("=" * 65)
log("STEP 3: Z_m = Σ_h  s_{hm,0} × g_h")
log("=" * 65)

iv_data = shares.merge(g_h, on='group_h', how='inner')
n_unmatched = shares['group_h'].nunique() - iv_data['group_h'].nunique()
log(f"  HMS groups matched to ACS g_h: {iv_data['group_h'].nunique():,}  "
    f"(dropped {n_unmatched} without ACS match)")

iv_data['contribution'] = iv_data['s_hm'] * iv_data['g_h']

ssiv = (iv_data.groupby('product_module_code')
        .agg(
            iv_jaravel = ('contribution', 'sum'),
            n_groups   = ('group_h',     'nunique'),
            coverage   = ('s_hm',        'sum'),   # Σ s_hm ≤ 1 (cells dropped by MIN_HH filter)
        )
        .reset_index())

log(f"  Modules with iv_jaravel: {len(ssiv):,}")
log(f"  iv_jaravel: mean={ssiv['iv_jaravel'].mean():.6f}  "
    f"sd={ssiv['iv_jaravel'].std():.6f}  "
    f"p5={ssiv['iv_jaravel'].quantile(.05):.6f}  "
    f"p95={ssiv['iv_jaravel'].quantile(.95):.6f}")
log(f"  Coverage (avg Σ s_hm per module): {ssiv['coverage'].mean():.3f}")
log(f"  Groups per module (median):        {ssiv['n_groups'].median():.0f}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4  Save
# ─────────────────────────────────────────────────────────────────────────────
log("")
log("=" * 65)
log(f"STEP 4: Save → {OUT}")
log("=" * 65)

ssiv.to_stata(str(OUT), write_index=False)
log(f"  Saved {len(ssiv):,} module rows.")
log("""
  Quick Stata check:
    use "jaravel_ssiv.dta", clear
    merge 1:m product_module_code using "innovation_reg_data.dta", nogen
    collapse (mean) ssnp d_spending spending iv_jaravel, by(product_module_code)
    ivreg2 ssnp (d_spending = iv_jaravel) [aw=sqrt(spending)], first cluster(product_module_code)
""")
