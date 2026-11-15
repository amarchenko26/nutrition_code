"""
clean_panelist.py

Extract panelist.tsv files from all raw Nielsen Consumer Panel tgz archives,
append across years, clean column names, and map household_income codes to
human-readable labels and midpoint values.

Handles:
  - Filename change: panelists_YYYY.tsv (2004-2020) → panelist.tsv (2021+)
  - Column name change: CamelCase (2004-2020) → snake_case (2021+)
  - Income code change: codes 28/29/30 only exist 2006-2009, so all codes
    >= 27 are collapsed to "$100,000+" with a single midpoint for consistency

Also builds HOUSEHOLD LABOR-SUPPLY measures (see add_labor_supply_measures)
and keeps the Member_1..7 fields. See LABOR_SUPPLY.md for the full definitions,
the Nielsen head/member data-structure caveats, and how these feed build_hi.do.

Output:
  - interim/panelists/panelists_all_years.parquet  (+ .dta)  HH x year, enriched
  - interim/panelists/panelist_heads_long.parquet            HH x year x role
        (male_head / female_head / partner) with FT/PT/NE status and
        year-over-year transitions — the panel for the labor-supply event study
"""

import os
import subprocess
import io
import pandas as pd
import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
RAW_CONSUMER  = os.path.join(BASE_DATA_DIR, 'raw', 'consumer')
OUTPUT_DIR    = os.path.join(BASE_DATA_DIR, 'interim', 'panelists')

YEARS = range(2004, 2025)

# Columns to keep (in final snake_case names)
KEEP_COLS = [
    'household_code', 'panel_year', 'projection_factor',
    'projection_factor_magnet', 'household_income', 'household_size',
    'type_of_residence', 'household_composition',
    'age_and_presence_of_children', 'male_head_age', 'female_head_age',
    'male_head_employment', 'female_head_employment',
    'male_head_education', 'female_head_education',
    'male_head_occupation', 'female_head_occupation',
    'male_head_birth', 'female_head_birth',
    'marital_status', 'race', 'hispanic_origin',
    'panelist_zip_code', 'fips_state_code', 'fips_county_code',
    'region_code', 'wic_indicator_current', 'wic_indicator_ever_not_current',
] + [f'member_{i}_{suf}'
     for i in range(1, 8)
     for suf in ('birth', 'relationship_sex', 'employment')]

# CamelCase → snake_case mapping for pre-2021 columns that differ
COLUMN_RENAME = {
    'household_cd':                    'household_code',
    'panelist_zipcd':                  'panelist_zip_code',
    'fips_state_cd':                   'fips_state_code',
    'fips_county_cd':                  'fips_county_code',
    'region_cd':                       'region_code',
}

#---------------------------------------------------------------------------
# Income code mapping
# ---------------------------------------------------------------------------
# Codes 3-26 are consistent across all years.
# The top bracket changes: code 27 = $100,000+ in 2004-2005 and 2010+, but in
# 2006-2009 Nielsen split the top into 27 = $100-125k, 28 = $125-150k,
# 29 = $150-200k, 30 = $200k+. So that the income variable has the SAME
# bracket structure in every year (the IV design is in first differences,
# and a bracket change is a mechanical income "shock"), codes 27-30 are all
# collapsed to "$100,000+" with a single midpoint of $140,000 in every year
# (decision 2026-09-16). The 2006-09 detail supports that value: the mean of
# the detailed midpoints among top-coded households in those years is
# $135-141k.
# ---------------------------------------------------------------------------

INCOME_LABEL = {
    3:  'Under $5,000',
    4:  '$5,000-$7,999',
    6:  '$8,000-$9,999',
    8:  '$10,000-$11,999',
    10: '$12,000-$14,999',
    11: '$15,000-$19,999',
    13: '$20,000-$24,999',
    15: '$25,000-$29,999',
    16: '$30,000-$34,999',
    17: '$35,000-$39,999',
    18: '$40,000-$44,999',
    19: '$45,000-$49,999',
    21: '$50,000-$59,999',
    23: '$60,000-$69,999',
    26: '$70,000-$99,999',
    27: '$100,000+',
    28: '$100,000+',   # 2006-09 detail ($125-150k) collapsed, see above
    29: '$100,000+',   # 2006-09 detail ($150-200k) collapsed
    30: '$100,000+',   # 2006-09 detail ($200k+) collapsed
}

INCOME_MIDPOINT = {
    3:  2500,
    4:  6500,
    6:  9000,
    8:  11000,
    10: 13500,
    11: 17500,
    13: 22500,
    15: 27500,
    16: 32500,
    17: 37500,
    18: 42500,
    19: 47500,
    21: 55000,
    23: 65000,
    26: 85000,
    27: 140000,   # $100,000+ in every year (codes 28-30 collapsed into it)
    28: 140000,
    29: 140000,
    30: 140000,
}

HOUSEHOLD_COMPOSITION_LABEL = {
    1: 'Married',
    2: 'Female Head Living with Others Related',
    3: 'Male Head Living with Others Related',
    5: 'Female Living Alone',
    6: 'Female Living with Non-Related',
    7: 'Male Living Alone',
    8: 'Male Living with Non-Related',
}

AGE_CHILDREN_LABEL = {
    1: 'Under 6 only',
    2: '6-12 only',
    3: '13-17 only',
    4: 'Under 6 & 6-12',
    5: 'Under 6 & 13-17',
    6: '6-12 & 13-17',
    7: 'Under 6 & 6-12 & 13-17',
    9: 'No Children Under 18',
}

HEAD_AGE_LABEL = {
    0: 'No Head',
    1: 'Under 25',
    2: '25-29',
    3: '30-34',
    4: '35-39',
    5: '40-44',
    6: '45-49',
    7: '50-54',
    8: '55-64',
    9: '65+',
}

EMPLOYMENT_LABEL = {
    0: 'No Head',
    1: 'Under 30 hours',
    2: '30-34 hours',
    3: '35+ hours',
    9: 'Not Employed for Pay',
}

EDUCATION_LABEL = {
    0: 'No Head or Unknown',
    1: 'Grade School',
    2: 'Some High School',
    3: 'Graduated High School',
    4: 'Some College',
    5: 'Graduated College',
    6: 'Post College Grad',
}

MARITAL_STATUS_LABEL = {
    1: 'Married',
    2: 'Widowed',
    3: 'Divorced/Separated',
    4: 'Single',
}

RACE_LABEL = {
    1: 'White/Caucasian',
    2: 'Black/African American',
    3: 'Asian',
    4: 'Other',
}

HISPANIC_ORIGIN_LABEL = {
    1: 'Hispanic',
    2: 'Not Hispanic',
}

# Numeric scales for combining male/female head variables (Allcott-style)
# Code 0 = No Head → NaN (person doesn't exist, excluded from mean)
EDUC_YEARS = {0: np.nan, 1: 6, 2: 10, 3: 12, 4: 14, 5: 16, 6: 18}
# Employment → hours per week worked; code 9 (Not Employed) = 0 hrs; code 1 uses BLS part-time avg
EMPLOY_HOURS = {0: np.nan, 1: 20, 2: 32, 3: 40, 9: 0}
# Conditional on working: code 9 excluded (NaN)
EMPLOY_HOURS_IF_EMPLOYED = {0: np.nan, 1: 20, 2: 32, 3: 40, 9: np.nan}
# For a household SUM, an absent head (code 0) contributes 0 hours, not NaN
EMPLOY_HOURS_SUM = {0: 0, 1: 20, 2: 32, 3: 40, 9: 0}

# --- Non-head member fields (for recovering an unmarked partner's labor) -----
# Member_N_Relationship_Sex codes (Kilts NielsenIQ manual):
#   1 Son  2 Daughter  3 Other Male Relative  4 Other Female Relative
#   5 Male Not Related  6 Female Not Related  blank = No Member / Unknown
# There is NO spouse/partner code, so a non-head partner is indistinguishable
# from an aunt/parent (3/4) or roommate (5/6). We proxy "partner" as an
# opposite-sex, spouse-aged, adult NON-CHILD member in a single-head household.
MEMBER_ADULT_FEMALE_CODES = {4, 6}   # exclude Daughter (2)
MEMBER_ADULT_MALE_CODES   = {3, 5}   # exclude Son (1)
PARTNER_AGE_WINDOW = 15              # yrs from head's (binned) age to count as spouse-like
# Member_N_Employment is BINARY (1 = employed / blank only) — no hours detail.

# Head binned-age code → representative age (codebook bin midpoints; 73 for 65+
# is the observed mean, as in Allcott et al.). Used only as a FALLBACK when a
# head's birth year is missing or implausible; the exact age from
# *_head_birth is preferred everywhere (decision 2026-09-16).
HEAD_AGE_MID = {1: 22, 2: 27, 3: 32, 4: 37, 5: 42, 6: 47, 7: 52, 8: 60, 9: 73}


def head_exact_age(df, birth_col, age_code_col):
    """Head age = panel year - reported birth year; codebook midpoint fallback.

    Birth years outside 1900-2010 are treated as missing. Returns NaN where
    there is no such head (age code 0 / blank).
    """
    yr    = pd.to_numeric(df['panel_year'], errors='coerce')
    birth = pd.to_numeric(df[birth_col], errors='coerce')
    code  = pd.to_numeric(df[age_code_col], errors='coerce')
    exact = (yr - birth).where(birth.between(1900, 2010))
    fallback = code.map(lambda v: HEAD_AGE_MID.get(int(v)) if pd.notna(v) else np.nan)
    return exact.where(exact.notna(), fallback).where(code > 0)


def head_status(code):
    """Head employment code → FT / PT / NE status (None if no such head)."""
    if code == 3:      return 'FT'   # 35+ hours
    if code in (1, 2): return 'PT'   # under 35 hours
    if code == 9:      return 'NE'   # not employed for pay
    return None                      # 0 / missing / no head


def add_labor_supply_measures(df):
    """
    Build household labor-supply measures and a head/adult-level long panel.

    Heads (intensive margin, from hours bins):
      {male,female}_head_age_exact  age from the reported birth year (codebook
                                    midpoint fallback); NaN if no such head
      {male,female}_head_status     FT / PT / NE / <NA>
      {male,female}_head_employed   1 / 0 / <NA>
    Household extensive margin (counts of earning adults):
      n_heads                 number of designated heads (1 or 2, self-reported)
      n_head_earners          employed heads (0/1/2)
      hh_total_workhours      summed head hours (absent head = 0 hrs)
      has_recovered_partner   an opposite-sex, spouse-aged (+/-15 yrs of the
                              head's exact age), adult NON-CHILD member in a
                              SINGLE-head HH — the partner the household
                              declined to mark as a head
      n_recovered_partner_earners  employed such members (binary employment only)
      n_earners_total         n_head_earners + recovered partner earners

    Returns (df, heads_long). heads_long has one row per household x year x role
    (male_head / female_head / partner) with status, employed, and year-over-year
    `transition` (e.g. 'FT->PT'). Transitions are computed for heads only — the
    two head slots are stable across years, but member slots are not, so partner
    identity cannot be tracked over time.
    """
    yr = pd.to_numeric(df['panel_year'], errors='coerce')

    # --- heads: exact age, status, employed indicator, presence, summed hours
    for sex in ['male', 'female']:
        df[f'{sex}_head_age_exact'] = head_exact_age(df, f'{sex}_head_birth', f'{sex}_head_age')
        emp = pd.to_numeric(df[f'{sex}_head_employment'], errors='coerce')
        df[f'{sex}_head_status']   = emp.map(head_status).astype('object')
        df[f'{sex}_head_employed'] = emp.map(
            lambda x: np.nan if (pd.isna(x) or x == 0) else (0.0 if x == 9 else 1.0))
        df[f'_{sex}_hrs']     = emp.map(EMPLOY_HOURS_SUM)
        df[f'_{sex}_present'] = pd.to_numeric(df[f'{sex}_head_age'], errors='coerce') > 0

    df['n_heads'] = df['_male_present'].astype(int) + df['_female_present'].astype(int)
    df['n_head_earners'] = df[['male_head_employed', 'female_head_employed']].sum(axis=1, min_count=1)
    df['hh_total_workhours'] = df[['_male_hrs', '_female_hrs']].sum(axis=1, min_count=1)

    # --- recover an unmarked partner (single-head households only) ----------
    single = df['n_heads'] == 1
    head_is_male   = single & df['_male_present']
    head_is_female = single & df['_female_present']
    # single head's age (whichever head exists): exact from birth year,
    # codebook midpoint as fallback
    head_age_mid = df['male_head_age_exact'].where(df['_male_present'], df['female_head_age_exact'])

    has_partner       = pd.Series(False, index=df.index)
    n_partner_earners = pd.Series(0.0, index=df.index)
    partner_sex       = pd.Series(np.nan, index=df.index, dtype='object')
    for i in range(1, 8):
        rc, bc, ec = (f'member_{i}_relationship_sex', f'member_{i}_birth', f'member_{i}_employment')
        if rc not in df.columns:
            continue
        rel  = pd.to_numeric(df[rc], errors='coerce')
        mage = yr - pd.to_numeric(df[bc], errors='coerce')
        memp = pd.to_numeric(df[ec], errors='coerce')
        opp_female = head_is_male   & rel.isin(MEMBER_ADULT_FEMALE_CODES)
        opp_male   = head_is_female & rel.isin(MEMBER_ADULT_MALE_CODES)
        q = ((opp_female | opp_male)
             & (mage >= 18)
             & ((mage - head_age_mid).abs() <= PARTNER_AGE_WINDOW)).fillna(False)
        has_partner       |= q
        n_partner_earners += (q & (memp == 1)).astype(float)
        take = q & partner_sex.isna()
        partner_sex = partner_sex.mask(take, np.where(opp_female.fillna(False), 'F', 'M'))

    df['has_recovered_partner']       = has_partner
    df['recovered_partner_sex']       = partner_sex
    df['n_recovered_partner_earners'] = np.where(has_partner, n_partner_earners, np.nan)
    df['n_earners_total']             = df['n_head_earners'].fillna(0) + n_partner_earners

    # --- head/adult-level LONG panel (transitions for event study) ---------
    long_frames = []
    for sex, s in [('male', 'M'), ('female', 'F')]:
        present = df[f'{sex}_head_employed'].notna()
        sub = df.loc[present, ['household_code', 'panel_year']].copy()
        sub['role']     = f'{sex}_head'
        sub['sex']      = s
        sub['status']   = df.loc[present, f'{sex}_head_status']
        sub['employed'] = df.loc[present, f'{sex}_head_employed']
        sub['margin']   = 'intensive'
        long_frames.append(sub)
    pp = has_partner
    sub = df.loc[pp, ['household_code', 'panel_year']].copy()
    sub['sex']      = df.loc[pp, 'recovered_partner_sex']
    sub['role']     = 'partner'
    sub['employed'] = (n_partner_earners[pp] > 0).astype(float)
    sub['status']   = np.where(sub['employed'] == 1, 'employed', 'not_employed')
    sub['margin']   = 'extensive'
    long_frames.append(sub)

    heads_long = pd.concat(long_frames, ignore_index=True)
    heads_long = heads_long.sort_values(['household_code', 'role', 'panel_year'])
    is_head = heads_long['role'].isin(['male_head', 'female_head'])
    heads_long['status_lag'] = heads_long.groupby(['household_code', 'role'])['status'].shift()
    heads_long.loc[~is_head, 'status_lag'] = np.nan   # member slots not stable across years
    heads_long['transition'] = np.where(
        heads_long['status_lag'].notna() & heads_long['status'].notna(),
        heads_long['status_lag'].astype(str) + '->' + heads_long['status'].astype(str),
        np.nan)

    df.drop(columns=[c for c in df.columns
                     if c.startswith('_male_') or c.startswith('_female_')],
            inplace=True, errors='ignore')
    return df, heads_long


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("CLEAN PANELIST DATA")
    print("=" * 80)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ------------------------------------------------------------------
    # Extract panelist data from each year's tgz
    # ------------------------------------------------------------------
    frames = []
    for year in YEARS:
        tgz = os.path.join(RAW_CONSUMER, f'Consumer_Panel_Data_{year}.tgz')
        if not os.path.exists(tgz):
            print(f"  {year}: tgz not found, skipping")
            continue

        # Filename changed in 2021
        if year <= 2020:
            inner = f'nielsen_extracts/HMS/{year}/Annual_Files/panelists_{year}.tsv'
        else:
            inner = f'nielsen_extracts/HMS/{year}/Annual_Files/panelist.tsv'

        try:
            result = subprocess.run(
                ['tar', '-xzf', tgz, '--to-stdout', inner],
                capture_output=True, timeout=120)
            if result.returncode != 0:
                print(f"  {year}: tar extraction failed")
                continue

            df = pd.read_csv(io.BytesIO(result.stdout), sep='\t')

            # Normalize column names: lowercase + strip whitespace, then apply renames
            df.columns = [c.lower().strip() for c in df.columns]
            df = df.rename(columns=COLUMN_RENAME)

            # Keep only the columns we want (some may not exist in all years)
            available = [c for c in KEEP_COLS if c in df.columns]
            missing = [c for c in KEEP_COLS if c not in df.columns]
            df = df[available]

            frames.append(df)
            print(f"  {year}: {len(df):>7,} panelists  "
                  f"({len(available)} cols, missing: {missing if missing else 'none'})")

        except Exception as e:
            print(f"  {year}: error — {e}")
            continue

    if not frames:
        print("\nERROR: No panelist data extracted.")
        return

    # ------------------------------------------------------------------
    # Combine all years
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("COMBINING ALL YEARS")

    panelists = pd.concat(frames, ignore_index=True)
    print(f"  Total panelist-years: {len(panelists):,}")
    print(f"  Unique households: {panelists['household_code'].nunique():,}")
    print(f"  Years: {sorted(panelists['panel_year'].unique())}")
    print(f"  Columns: {list(panelists.columns)}")

    # ------------------------------------------------------------------
    # Map household_income codes
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("MAPPING HOUSEHOLD INCOME")

    panelists['household_income'] = pd.to_numeric(
        panelists['household_income'], errors='coerce')

    # Show raw distribution
    print("\n  Raw income code distribution:")
    code_counts = panelists['household_income'].value_counts().sort_index()
    for code, count in code_counts.items():
        print(f"    {code:>3.0f}: {count:>9,}")

    # Map to labels
    panelists['household_income_label'] = panelists['household_income'].map(INCOME_LABEL)

    # Map to midpoints (codes 27-30 share the collapsed $100,000+ midpoint)
    panelists['household_income_midpoint'] = panelists['household_income'].map(INCOME_MIDPOINT)

    # ------------------------------------------------------------------
    # Income attributed to the year it was EARNED (Kilts manual Note 3)
    # ------------------------------------------------------------------
    # Panelists are surveyed in the fall BEFORE the panel year about their
    # total annual income as of year-end of the PREVIOUS calendar year, so
    # the income in the panel-year-t file is CALENDAR-YEAR t-2 income (e.g.
    # the 2021 panel is surveyed in September 2020 about income at the end
    # of 2019). household_income[_midpoint]_earned at year t is therefore
    # the value reported in the household's t+2 file — attributed by a merge
    # on (household, t+2), so a household observed at t and t+2 but not t+1
    # still gets earned income at t; a household's last two panel years have
    # none. VALIDATION-ONLY series (see CLAUDE.md, income timing): do not
    # estimate with it.
    rep = panelists[['household_code', 'panel_year', 'household_income',
                     'household_income_midpoint']].copy()
    rep['earn_year'] = rep['panel_year'] - 2
    rep = rep.rename(columns={
        'household_income': 'household_income_earned',
        'household_income_midpoint': 'household_income_midpoint_earned'})
    panelists = panelists.merge(
        rep[['household_code', 'earn_year', 'household_income_earned',
             'household_income_midpoint_earned']],
        left_on=['household_code', 'panel_year'],
        right_on=['household_code', 'earn_year'],
        how='left').drop(columns='earn_year')
    n_earned = panelists['household_income_midpoint_earned'].notna().sum()
    print(f"\n  Earned-income attribution (Note 3, from the t+2 file): "
          f"{n_earned:,}/{len(panelists):,} HH-years covered "
          f"({100 * n_earned / len(panelists):.1f}%)")

    # Check unmapped
    unmapped = panelists['household_income_label'].isna() & panelists['household_income'].notna()
    if unmapped.any():
        print(f"\n  WARNING: {unmapped.sum():,} rows with unrecognized income codes:")
        print(f"    {panelists.loc[unmapped, 'household_income'].value_counts().to_dict()}")

    # Summary
    print(f"\n  Income label distribution:")
    label_counts = panelists.groupby('household_income_label')['household_income_midpoint'].agg(
        ['count', 'first']).sort_values('first')
    for label, row in label_counts.iterrows():
        print(f"    {label:<25s}  midpoint=${row['first']:>9,.0f}  n={row['count']:>9,}")

    # ------------------------------------------------------------------
    # Map other categorical codes to labels
    # ------------------------------------------------------------------
    label_maps = [
        ('household_composition',        HOUSEHOLD_COMPOSITION_LABEL),
        ('age_and_presence_of_children', AGE_CHILDREN_LABEL),
        ('male_head_age',                HEAD_AGE_LABEL),
        ('female_head_age',              HEAD_AGE_LABEL),
        ('male_head_employment',         EMPLOYMENT_LABEL),
        ('female_head_employment',       EMPLOYMENT_LABEL),
        ('male_head_education',          EDUCATION_LABEL),
        ('female_head_education',        EDUCATION_LABEL),
        ('marital_status',               MARITAL_STATUS_LABEL),
        ('race',                         RACE_LABEL),
        ('hispanic_origin',              HISPANIC_ORIGIN_LABEL),
    ]
    for col, mapping in label_maps:
        if col in panelists.columns:
            panelists[col + '_label'] = pd.to_numeric(
                panelists[col], errors='coerce').map(mapping)

    # ------------------------------------------------------------------
    # Combined head variables (Allcott-style)
    # For each variable: convert to numeric scale, average both heads,
    # fall back to single head if one is missing (code 0 = No Head → NaN).
    # ------------------------------------------------------------------

    # Education (years of schooling)
    for sex in ['male', 'female']:
        col = f'{sex}_head_education'
        if col in panelists.columns:
            panelists[f'_{sex}_educ_yrs'] = pd.to_numeric(
                panelists[col], errors='coerce').map(EDUC_YEARS)
    if '_male_educ_yrs' in panelists.columns and '_female_educ_yrs' in panelists.columns:
        panelists['hh_avg_yrsofschool'] = panelists[['_male_educ_yrs', '_female_educ_yrs']].mean(axis=1)
    elif '_male_educ_yrs' in panelists.columns:
        panelists['hh_avg_yrsofschool'] = panelists['_male_educ_yrs']
    elif '_female_educ_yrs' in panelists.columns:
        panelists['hh_avg_yrsofschool'] = panelists['_female_educ_yrs']

    # Employment: hours worked per week & employed indicator
    for sex in ['male', 'female']:
        col = f'{sex}_head_employment'
        if col in panelists.columns:
            emp = pd.to_numeric(panelists[col], errors='coerce')
            panelists[f'_{sex}_work_hours']         = emp.map(EMPLOY_HOURS)
            panelists[f'_{sex}_work_hours_if_empl'] = emp.map(EMPLOY_HOURS_IF_EMPLOYED)
            panelists[f'_{sex}_employed']           = emp.map(
                lambda x: np.nan if (pd.isna(x) or x == 0) else (0.0 if x == 9 else 1.0))
    if '_male_work_hours' in panelists.columns and '_female_work_hours' in panelists.columns:
        panelists['hh_avg_workhours']             = panelists[['_male_work_hours', '_female_work_hours']].mean(axis=1)
        panelists['hh_avg_workhours_if_employed'] = panelists[['_male_work_hours_if_empl', '_female_work_hours_if_empl']].mean(axis=1)
        panelists['hh_employed']                  = panelists[['_male_employed', '_female_employed']].mean(axis=1)
    elif '_male_work_hours' in panelists.columns:
        panelists['hh_avg_workhours']             = panelists['_male_work_hours']
        panelists['hh_avg_workhours_if_employed'] = panelists['_male_work_hours_if_empl']
        panelists['hh_employed']                  = panelists['_male_employed']
    elif '_female_work_hours' in panelists.columns:
        panelists['hh_avg_workhours']             = panelists['_female_work_hours']
        panelists['hh_avg_workhours_if_employed'] = panelists['_female_work_hours_if_empl']
        panelists['hh_employed']                  = panelists['_female_employed']

    # Drop intermediate columns
    drop_cols = [c for c in panelists.columns if c.startswith('_male_') or c.startswith('_female_')]
    panelists.drop(columns=drop_cols, inplace=True)

    # ------------------------------------------------------------------
    # Labor-supply measures (head FT/PT/NE + recovered-partner extensive)
    # and head/adult-level long panel with year-over-year transitions.
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("BUILDING LABOR-SUPPLY MEASURES")
    panelists, heads_long = add_labor_supply_measures(panelists)
    print(f"  n_heads distribution:\n{panelists['n_heads'].value_counts().sort_index().to_string()}")
    print(f"  HHs with a recovered partner: {panelists['has_recovered_partner'].sum():,}")
    print(f"  Head-level long panel rows: {len(heads_long):,} "
          f"(heads {(heads_long['role'] != 'partner').sum():,}, "
          f"partners {(heads_long['role'] == 'partner').sum():,})")
    print(f"  Non-null head transitions: {heads_long['transition'].notna().sum():,}")

    # ------------------------------------------------------------------
    # Merge dietary ailments (from clean_ailments.py output, 2011-2023)
    # HHs not in the health survey get NaN for all ailment columns.
    # ------------------------------------------------------------------
    ailments_path = os.path.join(BASE_DATA_DIR, 'interim', 'ailments',
                                 'dietary_ailments_by_household.parquet')
    if os.path.exists(ailments_path):
        print("\n" + "=" * 80)
        print("MERGING DIETARY AILMENTS")
        ailments = pd.read_parquet(ailments_path)
        ailments['household_code'] = pd.to_numeric(ailments['household_code'], errors='coerce')
        panelists['household_code'] = pd.to_numeric(panelists['household_code'], errors='coerce')
        before = len(panelists)
        panelists = panelists.merge(ailments, on=['household_code', 'panel_year'], how='left')
        assert len(panelists) == before, "Ailment merge inflated row count — check for duplicates"
        ailment_cols = [c for c in ailments.columns if c not in ('household_code', 'panel_year')]
        n_matched = panelists[ailment_cols[0]].notna().sum()
        print(f"  Matched {n_matched:,} / {len(panelists):,} panelist-years to ailment survey")
        print(f"  Ailment columns added: {ailment_cols}")
    else:
        print(f"\n  NOTE: ailments file not found at {ailments_path}")
        print("  Run clean_ailments.py first to add dietary disease variables.")

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("SAVING")

    out_path = os.path.join(OUTPUT_DIR, 'panelists_all_years.parquet')
    panelists.to_parquet(out_path, index=False)

    # head/adult-level long panel (one row per HH x year x role)
    heads_long_path = os.path.join(OUTPUT_DIR, 'panelist_heads_long.parquet')
    heads_long.to_parquet(heads_long_path, index=False)
    print(f"  Saved: {heads_long_path}  {heads_long.shape}")

    # export to stata
    stata_path = os.path.join(OUTPUT_DIR, 'panelists_all_years.dta')
    panelists.to_stata(stata_path)

    print(f"  Saved: {out_path}")
    print(f"  Shape: {panelists.shape}")
    print(f"  Columns: {list(panelists.columns)}")

    print("\nDone.")


if __name__ == '__main__':
    main()
