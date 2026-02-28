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

Output:
  - interim/panelists/panelists_all_years.parquet
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
]

# CamelCase → snake_case mapping for pre-2021 columns that differ
COLUMN_RENAME = {
    'household_cd':                    'household_code',
    'panelist_zipcd':                  'panelist_zip_code',
    'fips_state_cd':                   'fips_state_code',
    'fips_county_cd':                  'fips_county_code',
    'region_cd':                       'region_code',
}

# ---------------------------------------------------------------------------
# Income code mapping
# ---------------------------------------------------------------------------
# Codes 3-26 are consistent across all years.
# Code 27 = $100,000+ in 2004-2005 and 2010+ (midpoint $140,000),
#           $100,000-$124,999 in 2006-2009 (midpoint $112,500).
# Codes 28-30 only exist in 2006-2009.
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
    28: '$125,000-$149,999',
    29: '$150,000-$199,999',
    30: '$200,000+',
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
    28: 137500,
    29: 175000,
    30: 250000,
}

# Code 27 midpoint depends on year
INCOME_MIDPOINT_27_EXPANDED = 112500   # 2006-2009: $100,000-$124,999
INCOME_MIDPOINT_27_DEFAULT  = 140000   # all other years: $100,000+


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

            # Normalize column names: lowercase, then apply renames
            df.columns = [c.lower() for c in df.columns]
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

    # Map to midpoints (code 27 is year-dependent)
    panelists['household_income_midpoint'] = panelists['household_income'].map(INCOME_MIDPOINT)

    expanded_years = panelists['panel_year'].between(2006, 2009)
    is_code_27 = panelists['household_income'] == 27
    panelists.loc[is_code_27 & expanded_years, 'household_income_midpoint'] = INCOME_MIDPOINT_27_EXPANDED
    panelists.loc[is_code_27 & ~expanded_years, 'household_income_midpoint'] = INCOME_MIDPOINT_27_DEFAULT

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
    # Save
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("SAVING")

    out_path = os.path.join(OUTPUT_DIR, 'panelists_all_years.parquet')
    panelists.to_parquet(out_path, index=False)

    # export to stata
    stata_path = os.path.join(OUTPUT_DIR, 'panelists_all_years.dta')
    panelists.to_stata(stata_path)

    print(f"  Saved: {out_path}")
    print(f"  Shape: {panelists.shape}")
    print(f"  Columns: {list(panelists.columns)}")

    print("\nDone.")


if __name__ == '__main__':
    main()
