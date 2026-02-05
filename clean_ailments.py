#!/usr/bin/env python3
"""
Clean Nielsen Ailments Data

Extracts dietary disease-related ailments from Nielsen Healthcare survey data
across all available years (2011-2023).

Target ailments (dietary/metabolic diseases):
- Cholesterol Problems
- Pre-Diabetes
- Diabetes Type I
- Diabetes Type II
- Heart Disease/Heart Attack/Angina/Heart Failure
- High Blood Pressure/Hypertension
- Obesity/Overweight
"""

import os
import re
import pandas as pd
from pathlib import Path


# ============================================================================
# CONFIGURATION
# ============================================================================
# Set to True to use sample data (faster iteration during development)
# Set to False to use full data (for production runs)
# Note: Ailments data is household-level survey data, so we don't sample it.
# This toggle only affects output path naming for consistency with other scripts.
USE_SAMPLE = True

# Base paths
BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'


def get_ailments_paths():
    """Get input/output paths based on USE_SAMPLE setting."""
    # Note: Input is always from raw ailments (no sample version)
    # Output suffix added for consistency with rest of pipeline
    suffix = '_sample' if USE_SAMPLE else ''
    return {
        'ailments_dir': os.path.join(BASE_DATA_DIR, 'raw/ailments'),
        'output_dir': os.path.join(BASE_DATA_DIR, f'interim/ailments{suffix}'),
    }


# Dietary disease keywords to search for in column names
DIETARY_DISEASE_KEYWORDS = {
    'cholesterol': 'cholesterol',
    'pre-diabetes': 'prediabetes',
    'pre diabetes': 'prediabetes',
    'prediabetes': 'prediabetes',
    'diabetes type i': 'diabetes_type1',
    'diabetes - type i': 'diabetes_type1',
    'diabetes type 1': 'diabetes_type1',
    'diabetes type ii': 'diabetes_type2',
    'diabetes - type ii': 'diabetes_type2',
    'diabetes type 2': 'diabetes_type2',
    'heart disease': 'heart_disease',
    'heart attack': 'heart_disease',
    'angina': 'heart_disease',
    'heart failure': 'heart_disease',
    'high blood pressure': 'hypertension',
    'hypertension': 'hypertension',
    'obesity': 'obesity',
    'overweight': 'obesity',
}


def find_ailment_columns(df):
    """
    Find columns related to dietary disease ailments.

    Parameters:
    -----------
    df : DataFrame
        Raw ailments data

    Returns:
    --------
    dict : Mapping of standardized ailment name -> column name
    """
    ailment_cols = {}

    for col in df.columns:
        col_lower = col.lower()

        # Check for each keyword
        for keyword, std_name in DIETARY_DISEASE_KEYWORDS.items():
            if keyword in col_lower:
                # Prefer Q1_Ailment columns (the main ailment indicator)
                # or Q36_ columns (2023 format)
                is_main_indicator = (
                    'q1_ailment' in col_lower or
                    col_lower.startswith('q36_') or
                    (std_name not in ailment_cols)  # take first match if no better option
                )

                if is_main_indicator:
                    # Avoid duplicate mappings - only take the most specific one
                    # e.g., "Diabetes Type II" should not match "Diabetes Type I"
                    if std_name == 'diabetes_type1' and 'type ii' in col_lower:
                        continue
                    if std_name == 'diabetes_type2' and 'type i' in col_lower and 'type ii' not in col_lower:
                        continue

                    ailment_cols[std_name] = col
                    break

    return ailment_cols


def find_household_id_column(df):
    """
    Find the household ID column (varies by year).

    Parameters:
    -----------
    df : DataFrame
        Raw ailments data

    Returns:
    --------
    str : Column name for household ID
    """
    for col in df.columns:
        col_lower = col.lower()
        if 'household' in col_lower or 'hhid' in col_lower or 'panelistid' in col_lower:
            return col

    # Fallback to first column if it looks like an ID
    first_col = df.columns[0]
    if df[first_col].dtype in ['int64', 'float64']:
        return first_col

    return None


def process_year(year, ailments_dir):
    """
    Process ailments data for a single year.

    Parameters:
    -----------
    year : int
        Year to process
    ailments_dir : str
        Base directory for ailments data

    Returns:
    --------
    DataFrame with household_id, year, and dietary disease columns
    """
    year_dir = os.path.join(ailments_dir, str(year))

    if not os.path.exists(year_dir):
        print(f"  Year {year}: Directory not found")
        return None

    # Find the data file (Excel file with data, not format/layout)
    data_files = []
    for f in os.listdir(year_dir):
        if f.endswith('.xlsx') and not f.startswith('~'):
            # Skip format/layout files
            f_lower = f.lower()
            if 'format' in f_lower or 'layout' in f_lower:
                continue
            data_files.append(f)

    if not data_files:
        print(f"  Year {year}: No data file found")
        return None

    # Use the largest file (likely the main data file)
    data_file = max(data_files, key=lambda f: os.path.getsize(os.path.join(year_dir, f)))
    data_path = os.path.join(year_dir, data_file)

    print(f"\n  Year {year}: Reading {data_file}")

    try:
        df = pd.read_excel(data_path, engine='openpyxl')
    except Exception as e:
        print(f"  Year {year}: Error reading file - {e}")
        return None

    print(f"    Rows: {len(df):,}, Columns: {len(df.columns)}")

    # Find household ID column
    hh_col = find_household_id_column(df)
    if hh_col is None:
        print(f"  Year {year}: Could not find household ID column")
        return None
    print(f"    Household ID column: {hh_col}")

    # Find ailment columns
    ailment_cols = find_ailment_columns(df)
    print(f"    Found {len(ailment_cols)} dietary disease columns:")
    for std_name, col_name in ailment_cols.items():
        print(f"      {std_name}: {col_name[:60]}...")

    if not ailment_cols:
        print(f"  Year {year}: No dietary disease columns found")
        return None

    # Extract relevant data
    result_cols = {'household_id': df[hh_col], 'survey_year': year}

    for std_name, col_name in ailment_cols.items():
        # Convert to binary (1 = has ailment, 0 = does not)
        values = df[col_name].copy()

        # Handle different value formats
        if values.dtype == 'object':
            # Text values
            values = values.fillna(0)
            values = values.apply(lambda x: 1 if x in [1, '1', 'Yes', 'yes', 'Y', 'y'] else 0)
        else:
            # Numeric values
            values = values.fillna(0)
            values = (values == 1).astype(int)

        result_cols[std_name] = values

    result_df = pd.DataFrame(result_cols)

    # Remove duplicates (keep first occurrence per household)
    n_before = len(result_df)
    result_df = result_df.drop_duplicates(subset=['household_id'], keep='first')
    n_after = len(result_df)
    if n_before != n_after:
        print(f"    Removed {n_before - n_after} duplicate households")

    # Print prevalence rates
    print(f"    Prevalence rates:")
    for std_name in ailment_cols.keys():
        if std_name in result_df.columns:
            rate = result_df[std_name].mean() * 100
            print(f"      {std_name}: {rate:.1f}%")

    return result_df


def main():
    """Main function to clean ailments data."""
    print("="*80)
    print("CLEANING NIELSEN AILMENTS DATA")
    print("="*80)

    # Get paths based on USE_SAMPLE setting
    paths = get_ailments_paths()
    ailments_dir = paths['ailments_dir']
    output_dir = paths['output_dir']

    print(f"USE_SAMPLE: {USE_SAMPLE}")
    print(f"Input: {ailments_dir}")
    print(f"Output: {output_dir}")

    os.makedirs(output_dir, exist_ok=True)

    # Find available years
    years = []
    for item in os.listdir(ailments_dir):
        if item.isdigit() and os.path.isdir(os.path.join(ailments_dir, item)):
            years.append(int(item))
    years = sorted(years)

    print(f"\nFound {len(years)} years: {years}")

    # Process each year
    all_years_data = []

    for year in years:
        result = process_year(year, ailments_dir)
        if result is not None:
            all_years_data.append(result)

    if not all_years_data:
        print("\nERROR: No data processed successfully")
        return

    # Combine all years
    print("\n" + "="*80)
    print("COMBINING ALL YEARS")
    print("="*80)

    combined_df = pd.concat(all_years_data, ignore_index=True)
    print(f"\nTotal rows: {len(combined_df):,}")
    print(f"Unique households: {combined_df['household_id'].nunique():,}")
    print(f"Years covered: {combined_df['survey_year'].min()} - {combined_df['survey_year'].max()}")

    # Fill missing ailment columns with 0 (not surveyed = assume no ailment)
    ailment_columns = [c for c in combined_df.columns if c not in ['household_id', 'survey_year']]
    for col in ailment_columns:
        combined_df[col] = combined_df[col].fillna(0).astype(int)

    # Create composite measures
    print("\nCreating composite measures...")

    # Any diabetes (pre-diabetes, type 1, or type 2)
    diabetes_cols = [c for c in ailment_columns if 'diabetes' in c]
    if diabetes_cols:
        combined_df['any_diabetes'] = combined_df[diabetes_cols].max(axis=1)
        print(f"  any_diabetes (from {diabetes_cols})")

    # Any metabolic disease (diabetes, obesity, hypertension, cholesterol, heart)
    metabolic_cols = ailment_columns
    if metabolic_cols:
        combined_df['any_metabolic_disease'] = combined_df[metabolic_cols].max(axis=1)
        print(f"  any_metabolic_disease (from all {len(metabolic_cols)} conditions)")

    # Count of conditions
    combined_df['n_dietary_conditions'] = combined_df[ailment_columns].sum(axis=1)
    print(f"  n_dietary_conditions (count of conditions)")

    # Summary statistics
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)

    print(f"\nHouseholds by year:")
    year_counts = combined_df.groupby('survey_year')['household_id'].nunique()
    for year, count in year_counts.items():
        print(f"  {year}: {count:,} households")

    print(f"\nOverall prevalence rates:")
    for col in ailment_columns + ['any_diabetes', 'any_metabolic_disease']:
        if col in combined_df.columns:
            rate = combined_df[col].mean() * 100
            print(f"  {col}: {rate:.1f}%")

    print(f"\nCondition counts:")
    print(combined_df['n_dietary_conditions'].value_counts().sort_index())

    # Save to parquet
    output_path = os.path.join(output_dir, 'dietary_ailments_by_household.parquet')
    combined_df.to_parquet(output_path, index=False)
    print(f"\nSaved to: {output_path}")

    # Also save as CSV for inspection
    csv_path = os.path.join(output_dir, 'dietary_ailments_by_household.csv')
    combined_df.to_csv(csv_path, index=False)
    print(f"Saved to: {csv_path}")

    # Save summary by year
    summary_by_year = []
    for year in combined_df['survey_year'].unique():
        year_data = combined_df[combined_df['survey_year'] == year]
        row = {'year': year, 'n_households': len(year_data)}
        for col in ailment_columns + ['any_diabetes', 'any_metabolic_disease']:
            if col in year_data.columns:
                row[f'{col}_rate'] = year_data[col].mean() * 100
        summary_by_year.append(row)

    summary_df = pd.DataFrame(summary_by_year)
    summary_path = os.path.join(output_dir, 'dietary_ailments_summary_by_year.csv')
    summary_df.to_csv(summary_path, index=False)
    print(f"Saved summary to: {summary_path}")

    print("\n" + "="*80)
    print("AILMENTS CLEANING COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
