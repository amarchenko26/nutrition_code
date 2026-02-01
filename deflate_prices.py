#!/usr/bin/env python3
"""
Deflate Prices Script

Deflates nominal prices to real prices using CPI data.
Formula: real_price = nominal_price × (target_year_CPI / current_month_CPI)

Default target year: 2013 (uses annual average CPI for 2013)

Usage:
    python deflate_prices.py

This script:
1. Loads CPI data from FRED (CPIEBEV - Consumer Price Index for Food and Beverages)
2. Deflates total_price_paid and total_spent columns in the purchases data
3. Saves deflated data to a new directory
"""

import os
import pandas as pd
from glob import glob


# ============================================================================
# CONFIGURATION
# ============================================================================
# Target year for deflation (will use annual average CPI for this year)
TARGET_YEAR = 2013

# CPI data path (FRED CPIEBEV - Food and Beverages CPI)
CPI_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/price_deflator/CPIEBEV.csv'

# Input purchases data path
PURCHASES_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_with_corn_classification'

# Output path for deflated data
OUTPUT_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_deflated'

# Columns to deflate
COLUMNS_TO_DEFLATE = ['total_price_paid', 'total_spent']


def load_cpi_data(cpi_path=CPI_PATH):
    """
    Load CPI data and create year-month to CPI mapping.

    Parameters:
    -----------
    cpi_path : str
        Path to CPI CSV file

    Returns:
    --------
    tuple: (cpi_df with year/month columns, dict mapping (year, month) -> CPI value)
    """
    print(f"Loading CPI data from: {cpi_path}")
    cpi_df = pd.read_csv(cpi_path)

    # Parse dates
    cpi_df['date'] = pd.to_datetime(cpi_df['observation_date'])
    cpi_df['year'] = cpi_df['date'].dt.year
    cpi_df['month'] = cpi_df['date'].dt.month

    # Remove rows with missing CPI values
    cpi_df = cpi_df.dropna(subset=['CPIEBEV'])

    # Create lookup dictionary: (year, month) -> CPI
    cpi_lookup = dict(zip(
        zip(cpi_df['year'], cpi_df['month']),
        cpi_df['CPIEBEV']
    ))

    print(f"  Loaded {len(cpi_lookup)} monthly CPI values")
    print(f"  Years covered: {cpi_df['year'].min()} - {cpi_df['year'].max()}")

    return cpi_df, cpi_lookup


def get_target_cpi(cpi_df, target_year=TARGET_YEAR):
    """
    Get the average CPI for the target year.

    Parameters:
    -----------
    cpi_df : DataFrame
        CPI data with year column
    target_year : int
        Year to use as target for deflation

    Returns:
    --------
    float: Average CPI for the target year
    """
    target_cpi = cpi_df[cpi_df['year'] == target_year]['CPIEBEV'].mean()
    print(f"  Target year {target_year} average CPI: {target_cpi:.3f}")
    return target_cpi


def deflate_year(year_dir, cpi_lookup, target_cpi, columns_to_deflate=COLUMNS_TO_DEFLATE):
    """
    Deflate prices for a single year of purchases data.

    Parameters:
    -----------
    year_dir : str
        Path to year partition directory
    cpi_lookup : dict
        Mapping of (year, month) -> CPI value
    target_cpi : float
        Target year CPI value
    columns_to_deflate : list
        List of column names to deflate

    Returns:
    --------
    DataFrame with deflated price columns added
    """
    year = int(os.path.basename(year_dir).replace('panel_year=', ''))
    print(f"\nProcessing year {year}...")

    # Load data
    df = pd.read_parquet(year_dir)
    n_rows = len(df)
    print(f"  Loaded {n_rows:,} rows")

    # Parse purchase_date to get year and month
    if 'purchase_date' in df.columns:
        df['purchase_date'] = pd.to_datetime(df['purchase_date'])
        df['purchase_year'] = df['purchase_date'].dt.year
        df['purchase_month'] = df['purchase_date'].dt.month
    else:
        # Fall back to panel year
        print(f"  WARNING: No purchase_date column, using panel year for all rows")
        df['purchase_year'] = year
        df['purchase_month'] = 6  # Use June as midpoint

    # Create deflator column: target_cpi / current_month_cpi
    # First, map (year, month) to CPI
    df['current_cpi'] = df.apply(
        lambda row: cpi_lookup.get((row['purchase_year'], row['purchase_month'])),
        axis=1
    )

    # Check for missing CPI values
    n_missing = df['current_cpi'].isna().sum()
    if n_missing > 0:
        print(f"  WARNING: {n_missing:,} rows ({n_missing/n_rows*100:.1f}%) missing CPI value")
        # Fill with annual average for that year
        for y in df[df['current_cpi'].isna()]['purchase_year'].unique():
            year_cpi = df[(df['purchase_year'] == y) & (df['current_cpi'].notna())]['current_cpi'].mean()
            if pd.isna(year_cpi):
                # If no CPI for this year at all, use closest year
                available_years = sorted([k[0] for k in cpi_lookup.keys()])
                closest_year = min(available_years, key=lambda x: abs(x - y))
                year_cpi = sum(cpi_lookup[(closest_year, m)] for m in range(1, 13) if (closest_year, m) in cpi_lookup) / 12
            df.loc[(df['purchase_year'] == y) & (df['current_cpi'].isna()), 'current_cpi'] = year_cpi

    # Calculate deflator
    df['deflator'] = target_cpi / df['current_cpi']

    # Deflate each column
    for col in columns_to_deflate:
        if col in df.columns:
            real_col = f'{col}_real_{TARGET_YEAR}'
            df[real_col] = df[col] * df['deflator']
            print(f"  Deflated {col} -> {real_col}")

            # Summary stats
            if df[col].notna().sum() > 0:
                nominal_mean = df[col].mean()
                real_mean = df[real_col].mean()
                print(f"    Mean: ${nominal_mean:.2f} (nominal) -> ${real_mean:.2f} (real {TARGET_YEAR}$)")
        else:
            print(f"  WARNING: Column {col} not found in data")

    # Drop temporary columns
    df = df.drop(columns=['current_cpi', 'deflator', 'purchase_year', 'purchase_month'])

    return df


def main():
    """Main function to deflate all years of purchases data."""
    print("=" * 80)
    print("DEFLATING PRICES TO REAL VALUES")
    print("=" * 80)
    print(f"Target year: {TARGET_YEAR}")
    print(f"Columns to deflate: {COLUMNS_TO_DEFLATE}")

    # Load CPI data
    cpi_df, cpi_lookup = load_cpi_data()
    target_cpi = get_target_cpi(cpi_df, TARGET_YEAR)

    # Find all year partitions
    year_dirs = sorted(glob(os.path.join(PURCHASES_PATH, 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {PURCHASES_PATH}")
        return

    print(f"\nFound {len(year_dirs)} year partitions")

    # Create output directory
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # Process each year
    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        # Deflate this year's data
        df_deflated = deflate_year(year_dir, cpi_lookup, target_cpi)

        # Save to output directory
        output_year_dir = os.path.join(OUTPUT_PATH, f'panel_year={year}')
        os.makedirs(output_year_dir, exist_ok=True)
        output_path = os.path.join(output_year_dir, 'data.parquet')

        df_deflated.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
        file_size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f"  Saved: {output_path} ({file_size_mb:.1f} MB)")

        del df_deflated

    print("\n" + "=" * 80)
    print("DEFLATION COMPLETE")
    print("=" * 80)
    print(f"Output saved to: {OUTPUT_PATH}")
    print(f"\nNew columns added:")
    for col in COLUMNS_TO_DEFLATE:
        print(f"  - {col}_real_{TARGET_YEAR}")


if __name__ == "__main__":
    main()
