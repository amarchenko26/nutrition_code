#!/usr/bin/env python3
"""
Nielsen-USDA Merger with Year-Based Matching

This script merges Nielsen purchase data with USDA ingredients data based on:
1. UPC codes
2. Closest available USDA release year to the Nielsen purchase year

This allows tracking of ingredient reformulations over time.
"""

import os
import shutil
from glob import glob
import pandas as pd
import pyarrow.parquet as pq


def load_year_mapping(usda_dir):
    """
    Load the Nielsen year -> USDA release year mapping.

    Parameters:
    -----------
    usda_dir : str
        Path to USDA interim directory

    Returns:
    --------
    dict : Mapping of Nielsen year -> USDA release year
    """
    mapping_path = os.path.join(usda_dir, 'nielsen_usda_year_mapping.csv')

    if not os.path.exists(mapping_path):
        print(f"ERROR: Year mapping file not found: {mapping_path}")
        print("Run clean_usda.py first to generate this file.")
        return None

    mapping_df = pd.read_csv(mapping_path)
    year_mapping = dict(zip(mapping_df['nielsen_year'], mapping_df['usda_release_year']))

    print(f"Loaded year mapping for {len(year_mapping)} Nielsen years")
    return year_mapping


def load_usda_ingredients_by_year(usda_dir):
    """
    Load the time-varying USDA ingredients data.

    Parameters:
    -----------
    usda_dir : str
        Path to USDA interim directory

    Returns:
    --------
    DataFrame with ingredients indexed by upc_11 and usda_release_year
    """
    print("="*80)
    print("LOADING USDA INGREDIENTS DATA (TIME-VARYING)")
    print("="*80)

    parquet_path = os.path.join(usda_dir, 'usda_ingredients_by_year.parquet')

    if not os.path.exists(parquet_path):
        print(f"ERROR: Time-varying ingredients file not found: {parquet_path}")
        print("Run clean_usda.py first to generate this file.")
        return None

    print(f"\nReading: {parquet_path}")
    usda_df = pd.read_parquet(parquet_path)

    print(f"Rows loaded: {len(usda_df):,}")
    print(f"Unique UPCs: {usda_df['upc_11'].nunique():,}")
    print(f"USDA release years: {sorted(usda_df['usda_release_year'].unique())}")

    # Show distribution by year
    print("\nRows by USDA release year:")
    for year in sorted(usda_df['usda_release_year'].unique()):
        n = len(usda_df[usda_df['usda_release_year'] == year])
        print(f"  {year}: {n:,}")

    return usda_df


def get_usda_for_nielsen_year(usda_df, nielsen_year, year_mapping):
    """
    Get the appropriate USDA ingredients for a given Nielsen year.

    For each UPC, selects the USDA release year closest to the mapped year.
    If no exact match exists for that UPC, falls back to the next closest year.

    Parameters:
    -----------
    usda_df : DataFrame
        Time-varying USDA ingredients data
    nielsen_year : int
        Nielsen purchase year
    year_mapping : dict
        Mapping of Nielsen year -> USDA release year

    Returns:
    --------
    DataFrame with ingredients for the given Nielsen year
    """
    usda_year = year_mapping.get(nielsen_year)

    if usda_year is None:
        print(f"  WARNING: No USDA year mapping for Nielsen year {nielsen_year}")
        # Fall back to earliest available year
        usda_year = usda_df['usda_release_year'].min()

    # For each UPC, choose the closest USDA release year to the mapped year.
    # If there is a tie, prefer the earlier release year for determinism.
    year_specific = usda_df.assign(
        _year_diff=(usda_df['usda_release_year'] - usda_year).abs()
    ).sort_values(['upc_11', '_year_diff', 'usda_release_year']) \
     .groupby('upc_11', as_index=False).first()

    return year_specific.drop(columns=['_year_diff'])


def merge_year_with_usda(purchases_dir, year, usda_df, year_mapping):
    """
    Merge a single year of Nielsen purchases with USDA ingredients.

    Uses the appropriate USDA release for the given Nielsen year.

    Parameters:
    -----------
    purchases_dir : str
        Directory containing partitioned parquet files
    year : int
        Nielsen year to process
    usda_df : DataFrame
        Time-varying USDA ingredients dataframe
    year_mapping : dict
        Nielsen year -> USDA release year mapping

    Returns:
    --------
    tuple : (stats dict, matched DataFrame)
    """
    print(f"\n\n{'='*80}")
    print(f"PROCESSING NIELSEN YEAR {year}")
    print("="*80)

    # Get the appropriate USDA data for this year
    usda_year = year_mapping.get(year, min(usda_df['usda_release_year'].unique()))
    print(f"Target USDA release year: {usda_year}")

    usda_for_year = get_usda_for_nielsen_year(usda_df, year, year_mapping)
    print(f"USDA UPCs available (closest-year per UPC): {len(usda_for_year):,}")

    # Read Nielsen purchases for this year
    partition_path = os.path.join(purchases_dir, f'panel_year={year}')

    if not os.path.exists(partition_path):
        print(f"ERROR: Partition not found: {partition_path}")
        return None

    print(f"\nReading: {partition_path}")

    try:
        purchases_df = pd.read_parquet(partition_path)
    except Exception as e:
        print(f"ERROR reading parquet: {str(e)}")
        return None

    print(f"Purchases loaded: {len(purchases_df):,}")

    # Find UPC column
    upc_col = None
    for col in purchases_df.columns:
        if 'upc' in col.lower() and 'ver' not in col.lower():
            upc_col = col
            break

    if not upc_col:
        print(f"ERROR: No UPC column found in purchases data")
        return None

    print(f"UPC column: {upc_col}")
    print(f"Unique UPCs in purchases: {purchases_df[upc_col].nunique():,}")

    # Standardize Nielsen UPC to 11 digits
    purchases_df['upc_str'] = purchases_df[upc_col].astype(str)
    purchases_df['upc_11'] = purchases_df['upc_str'].str.zfill(11)

    # Before merge statistics
    n_purchases_before = len(purchases_df)
    n_unique_upcs = purchases_df[upc_col].nunique()

    # Select columns to merge from USDA
    usda_cols = ['upc_11', 'ingredients', 'brand_name', 'branded_food_category',
                 'usda_release_year', 'was_reformulated']
    usda_cols = [c for c in usda_cols if c in usda_for_year.columns]

    # Merge
    print(f"\nMerging with USDA ingredients (year-matched)...")
    merged_df = purchases_df.merge(
        usda_for_year[usda_cols],
        on='upc_11',
        how='left',
        indicator=True
    )

    # Rename merge indicator
    merged_df['_merge'] = merged_df['_merge'].replace({'left_only': 'no_match', 'both': 'matched'})

    # Calculate match statistics
    n_matched = (merged_df['_merge'] == 'matched').sum()
    n_unmatched = (merged_df['_merge'] == 'no_match').sum()
    match_rate = (n_matched / n_purchases_before) * 100 if n_purchases_before > 0 else 0

    matched_upcs = merged_df[merged_df['_merge'] == 'matched'][upc_col].nunique()
    upc_match_rate = (matched_upcs / n_unique_upcs) * 100 if n_unique_upcs > 0 else 0

    # Count reformulated products
    n_reformulated = 0
    if 'was_reformulated' in merged_df.columns:
        n_reformulated = merged_df[merged_df['was_reformulated'] == True][upc_col].nunique()

    print(f"\nMatch Results:")
    print(f"  Total purchases: {n_purchases_before:,}")
    print(f"  Matched purchases: {n_matched:,} ({match_rate:.2f}%)")
    print(f"  Unmatched purchases: {n_unmatched:,} ({100-match_rate:.2f}%)")
    print(f"\n  Unique UPCs: {n_unique_upcs:,}")
    print(f"  Matched UPCs: {matched_upcs:,} ({upc_match_rate:.2f}%)")
    print(f"  Reformulated UPCs in matches: {n_reformulated:,}")

    # Filter to only matched rows
    matched_df = merged_df[merged_df['_merge'] == 'matched'].copy()
    print(f"\nFiltered to matched rows: {len(matched_df):,}")

    # Drop temporary columns
    columns_to_drop = ['_merge', 'upc_str', 'upc_11']
    matched_df = matched_df.drop(columns=[col for col in columns_to_drop if col in matched_df.columns])

    # Add year column
    matched_df['panel_year'] = year

    # Return statistics
    stats = {
        'year': year,
        'usda_release_year_used': usda_year,
        'total_purchases': n_purchases_before,
        'matched_purchases': n_matched,
        'unmatched_purchases': n_unmatched,
        'purchase_match_rate': match_rate,
        'total_upcs': n_unique_upcs,
        'matched_upcs': matched_upcs,
        'unmatched_upcs': n_unique_upcs - matched_upcs,
        'upc_match_rate': upc_match_rate,
        'reformulated_upcs': n_reformulated,
    }

    return stats, matched_df


def main():
    """
    Main function to merge all years of Nielsen data with time-varying USDA ingredients.
    """
    print("NIELSEN-USDA INGREDIENTS MERGER (YEAR-MATCHED)")
    print("="*80)

    # Paths
    usda_dir = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/usda'
    purchases_dir = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_food'
    output_dir = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_with_ingredients'

    # Load year mapping
    year_mapping = load_year_mapping(usda_dir)
    if year_mapping is None:
        print("ERROR: Could not load year mapping. Run clean_usda.py first.")
        return

    # Load USDA data
    usda_df = load_usda_ingredients_by_year(usda_dir)
    if usda_df is None:
        print("ERROR: Could not load USDA data. Run clean_usda.py first.")
        return

    # Years to process (based on available partitions)
    year_dirs = sorted(glob(os.path.join(purchases_dir, 'panel_year=*')))
    if not year_dirs:
        print(f"ERROR: No year partitions found in {purchases_dir}")
        return

    years = [int(os.path.basename(d).replace('panel_year=', '')) for d in year_dirs]
    years = sorted(years)

    print(f"\n\nProcessing {len(years)} years: {years[0]}-{years[-1]}")

    # Clear and recreate output directory
    if os.path.exists(output_dir):
        print(f"\nClearing existing output directory: {output_dir}")
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Process each year
    all_stats = []

    for year in years:
        result = merge_year_with_usda(purchases_dir, year, usda_df, year_mapping)

        if result:
            stats, matched_df = result
            all_stats.append(stats)

            # Write this year's data
            year_output_path = os.path.join(output_dir, f'panel_year={year}', 'data.parquet')
            os.makedirs(os.path.dirname(year_output_path), exist_ok=True)
            matched_df.to_parquet(year_output_path, engine='pyarrow', compression='snappy', index=False)

            file_size_mb = os.path.getsize(year_output_path) / 1024 / 1024
            print(f"Saved: {year_output_path} ({file_size_mb:.1f} MB)")

            del matched_df

    # Summary report
    print("\n\n" + "="*80)
    print("MATCH RATE SUMMARY (2004-2023)")
    print("="*80)

    if all_stats:
        summary_df = pd.DataFrame(all_stats)

        print("\nYEAR-BY-YEAR RESULTS:")
        print("-" * 100)
        print(f"{'Year':<6} {'USDA Yr':<8} {'Purchases':>12} {'Matched':>12} {'Match %':>10} {'Reformulated':>12}")
        print("-" * 100)

        for _, row in summary_df.iterrows():
            print(f"{row['year']:<6} {row['usda_release_year_used']:<8} "
                  f"{row['total_purchases']:>12,} {row['matched_purchases']:>12,} "
                  f"{row['purchase_match_rate']:>9.2f}% {row['reformulated_upcs']:>12,}")

        print("-" * 100)

        # Totals
        total_purchases = summary_df['total_purchases'].sum()
        total_matched = summary_df['matched_purchases'].sum()
        total_reformulated = summary_df['reformulated_upcs'].sum()

        print(f"{'Total':<6} {'':<8} {total_purchases:>12,} {total_matched:>12,} "
              f"{total_matched/total_purchases*100:>9.2f}% {total_reformulated:>12,}")

        # Save summary
        summary_path = os.path.join(output_dir, 'match_rate_summary.csv')
        summary_df.to_csv(summary_path, index=False)
        print(f"\nSaved summary to: {summary_path}")

        print(f"\nAll output saved to: {output_dir}")
        print(f"\nTo read the data:")
        print(f"  df = pd.read_parquet('{output_dir}')")
    else:
        print("\n\nERROR: No data was successfully processed")


if __name__ == "__main__":
    main()
