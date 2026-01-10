#!/usr/bin/env python3
"""
Nielsen-USDA Merger
Merges Nielsen purchase data with USDA ingredients data based on UPC codes
Reports match rates for each year from 2004-2023
"""

import os
import pandas as pd
import pyarrow.parquet as pq


def load_usda_ingredients(usda_path):
    """
    Load USDA ingredients data

    Parameters:
    -----------
    usda_path : str
        Path to USDA branded food CSV file

    Returns:
    --------
    usda_df : DataFrame
        USDA data with gtin_upc and ingredients columns
    """
    print("="*80)
    print("LOADING USDA INGREDIENTS DATA")
    print("="*80)

    if not os.path.exists(usda_path):
        print(f"ERROR: USDA file not found: {usda_path}")
        return None

    print(f"\nReading: {usda_path}")

    # Load only the columns we need
    usda_df = pd.read_csv(usda_path, usecols=['gtin_upc', 'ingredients'])

    print(f"Rows loaded: {len(usda_df):,}")
    print(f"Unique UPCs: {usda_df['gtin_upc'].nunique():,}")

    # Remove rows with missing UPC or ingredients
    usda_df = usda_df.dropna(subset=['gtin_upc'])
    print(f"Rows after removing missing UPC: {len(usda_df):,}")

    # Standardize UPC format
    # Convert to string and remove decimals
    usda_df['gtin_upc'] = usda_df['gtin_upc'].astype(str).str.replace('.0', '', regex=False)

    # Pad to 12 digits (UPC-12 standard) - this handles both UPC-A and shorter codes
    usda_df['upc_12'] = usda_df['gtin_upc'].str.zfill(12)

    # Also create version with last 10 digits for matching
    usda_df['upc_10'] = usda_df['gtin_upc'].str.zfill(12).str[-10:]

    print(f"\nUPC standardization:")
    print(f"  Sample original: {usda_df['gtin_upc'].iloc[0]}")
    print(f"  Sample 12-digit: {usda_df['upc_12'].iloc[0]}")
    print(f"  Sample 10-digit: {usda_df['upc_10'].iloc[0]}")

    # Remove duplicates based on 12-digit UPC (keep first occurrence)
    n_before = len(usda_df)
    usda_df = usda_df.drop_duplicates(subset=['upc_12'], keep='first')
    n_after = len(usda_df)
    print(f"Duplicates removed: {n_before - n_after:,}")
    print(f"Final unique UPCs: {len(usda_df):,}")

    return usda_df


def merge_year_with_usda(purchases_dir, year, usda_df, output_dir):
    """
    Merge a single year of Nielsen purchases with USDA ingredients

    Parameters:
    -----------
    purchases_dir : str
        Directory containing partitioned parquet files
    year : int
        Year to process
    usda_df : DataFrame
        USDA ingredients dataframe
    output_dir : str
        Directory to save merged output

    Returns:
    --------
    match_stats : dict
        Dictionary with match statistics
    """
    print(f"\n\n{'='*80}")
    print(f"PROCESSING YEAR {year}")
    print("="*80)

    # Read the specific year partition
    partition_path = os.path.join(purchases_dir, f'panel_year={year}')

    if not os.path.exists(partition_path):
        print(f"ERROR: Partition not found: {partition_path}")
        return None

    print(f"\nReading: {partition_path}")

    # Read all parquet files in this partition
    try:
        purchases_df = pd.read_parquet(partition_path)
    except Exception as e:
        print(f"ERROR reading parquet: {str(e)}")
        return None

    print(f"Purchases loaded: {len(purchases_df):,}")
    print(f"Columns: {purchases_df.columns.tolist()}")

    # Check for UPC column
    upc_col = None
    for col in purchases_df.columns:
        if 'upc' in col.lower() and 'ver' not in col.lower():
            upc_col = col
            break

    if not upc_col:
        print(f"ERROR: No UPC column found in purchases data")
        return None

    print(f"\nUPC column: {upc_col}")
    print(f"Unique UPCs in purchases: {purchases_df[upc_col].nunique():,}")

    # Standardize Nielsen UPC to 12 digits
    purchases_df['upc_str'] = purchases_df[upc_col].astype(str)
    purchases_df['upc_12'] = purchases_df['upc_str'].str.zfill(12)
    purchases_df['upc_10'] = purchases_df['upc_str'].str.zfill(10)

    print(f"\nUPC standardization (Nielsen):")
    print(f"  Sample original: {purchases_df['upc_str'].iloc[0]}")
    print(f"  Sample 12-digit: {purchases_df['upc_12'].iloc[0]}")
    print(f"  Sample 10-digit: {purchases_df['upc_10'].iloc[0]}")

    # Before merge statistics
    n_purchases_before = len(purchases_df)
    n_unique_upcs = purchases_df[upc_col].nunique()

    # Try merging on 12-digit UPC first
    print(f"\nMerging with USDA ingredients (12-digit UPC match)...")
    merged_df = purchases_df.merge(
        usda_df[['upc_12', 'ingredients']],
        left_on='upc_12',
        right_on='upc_12',
        how='left',
        indicator=True
    )

    # For unmatched, try 10-digit UPC match
    unmatched_mask = merged_df['_merge'] == 'left_only'
    n_unmatched_12 = unmatched_mask.sum()

    if n_unmatched_12 > 0:
        print(f"  12-digit matches: {(~unmatched_mask).sum():,}")
        print(f"  Trying 10-digit match for remaining {n_unmatched_12:,} rows...")

        # Second attempt: match on last 10 digits
        unmatched_df = merged_df[unmatched_mask].copy()
        second_merge = unmatched_df.merge(
            usda_df[['upc_10', 'ingredients']],
            left_on='upc_10',
            right_on='upc_10',
            how='left',
            suffixes=('', '_10')
        )

        # Update ingredients for 10-digit matches
        has_10_match = second_merge['ingredients_10'].notna()
        if has_10_match.sum() > 0:
            print(f"  10-digit matches: {has_10_match.sum():,}")
            # Update the original merged_df with 10-digit matches
            matched_indices = unmatched_df.index[has_10_match]
            merged_df.loc[matched_indices, 'ingredients'] = second_merge.loc[has_10_match, 'ingredients_10'].values
            merged_df.loc[matched_indices, '_merge'] = 'both'

    # Rename merge indicator for clarity
    merged_df['_merge'] = merged_df['_merge'].replace({'left_only': 'no_match', 'both': 'matched'})

    # Calculate match statistics
    n_matched = (merged_df['_merge'] == 'matched').sum()
    n_unmatched = (merged_df['_merge'] == 'no_match').sum()
    match_rate = (n_matched / n_purchases_before) * 100 if n_purchases_before > 0 else 0

    # UPC-level match rate
    matched_upcs = merged_df[merged_df['_merge'] == 'matched'][upc_col].nunique()
    upc_match_rate = (matched_upcs / n_unique_upcs) * 100 if n_unique_upcs > 0 else 0

    print(f"\nMatch Results:")
    print(f"  Total purchases: {n_purchases_before:,}")
    print(f"  Matched purchases: {n_matched:,} ({match_rate:.2f}%)")
    print(f"  Unmatched purchases: {n_unmatched:,} ({100-match_rate:.2f}%)")
    print(f"\n  Unique UPCs: {n_unique_upcs:,}")
    print(f"  Matched UPCs: {matched_upcs:,} ({upc_match_rate:.2f}%)")
    print(f"  Unmatched UPCs: {n_unique_upcs - matched_upcs:,} ({100-upc_match_rate:.2f}%)")

    # Drop the merge indicator and temporary UPC columns
    columns_to_drop = ['_merge', 'upc_str', 'upc_10']
    # Only drop upc_12 if it exists (it might be the merge key)
    if 'upc_12' in merged_df.columns:
        # Keep one upc_12 column (from the merge)
        pass
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df.columns])

    # Save merged data
    output_partition_dir = os.path.join(output_dir, f'panel_year={year}')
    os.makedirs(output_partition_dir, exist_ok=True)

    # output_file = os.path.join(output_partition_dir, 'data.parquet')
    # print(f"\nSaving merged data to: {output_file}")

    # merged_df.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)

    # file_size_mb = os.path.getsize(output_file) / 1024 / 1024
    # print(f"✓ Saved successfully! File size: {file_size_mb:.2f} MB")

    # Return statistics
    return {
        'year': year,
        'total_purchases': n_purchases_before,
        'matched_purchases': n_matched,
        'unmatched_purchases': n_unmatched,
        'purchase_match_rate': match_rate,
        'total_upcs': n_unique_upcs,
        'matched_upcs': matched_upcs,
        'unmatched_upcs': n_unique_upcs - matched_upcs,
        'upc_match_rate': upc_match_rate
    }


def main():
    """
    Main function to merge all years of Nielsen data with USDA ingredients
    """
    print("NIELSEN-USDA INGREDIENTS MERGER")
    print("="*80)

    # Paths
    usda_path = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/usda/usda_branded_food_deduped.csv'
    purchases_dir = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_all_years_food_only'
    output_dir = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_with_ingredients'

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Load USDA ingredients data
    usda_df = load_usda_ingredients(usda_path)

    if usda_df is None:
        print("ERROR: Could not load USDA data. Exiting.")
        return

    # Years to process
    years = list(range(2004, 2024))  # 2004-2023

    print(f"\n\nProcessing {len(years)} years: {years[0]}-{years[-1]}")

    # Process each year
    all_stats = []

    for year in years:
        stats = merge_year_with_usda(purchases_dir, year, usda_df, output_dir)

        if stats:
            all_stats.append(stats)

    # Summary report
    print("\n\n" + "="*80)
    print("MATCH RATE SUMMARY (2004-2023)")
    print("="*80)

    if all_stats:
        # Create summary dataframe
        summary_df = pd.DataFrame(all_stats)

        print("\nPURCHASE-LEVEL MATCH RATES:")
        print("-" * 80)
        print(f"{'Year':<6} {'Total':>12} {'Matched':>12} {'Unmatched':>12} {'Match %':>10}")
        print("-" * 80)

        for _, row in summary_df.iterrows():
            print(f"{row['year']:<6} {row['total_purchases']:>12,} {row['matched_purchases']:>12,} "
                  f"{row['unmatched_purchases']:>12,} {row['purchase_match_rate']:>9.2f}%")

        print("-" * 80)
        print(f"{'Total':<6} {summary_df['total_purchases'].sum():>12,} "
              f"{summary_df['matched_purchases'].sum():>12,} "
              f"{summary_df['unmatched_purchases'].sum():>12,} "
              f"{(summary_df['matched_purchases'].sum() / summary_df['total_purchases'].sum() * 100):>9.2f}%")

        print("\n\nUPC-LEVEL MATCH RATES:")
        print("-" * 80)
        print(f"{'Year':<6} {'Total UPCs':>12} {'Matched':>12} {'Unmatched':>12} {'Match %':>10}")
        print("-" * 80)

        for _, row in summary_df.iterrows():
            print(f"{row['year']:<6} {row['total_upcs']:>12,} {row['matched_upcs']:>12,} "
                  f"{row['unmatched_upcs']:>12,} {row['upc_match_rate']:>9.2f}%")

        print("-" * 80)

        # Save summary to CSV
        summary_path = os.path.join(output_dir, 'match_rate_summary.csv')
        summary_df.to_csv(summary_path, index=False)
        print(f"\n✓ Summary saved to: {summary_path}")

        print(f"\n✓ All merged data saved to: {output_dir}")
        print(f"\nTo read the merged dataset:")
        print(f"  df = pd.read_parquet('{output_dir}')")
        print(f"\nTo read specific years:")
        print(f"  df = pd.read_parquet('{output_dir}', filters=[('panel_year', 'in', [2020, 2021])])")
    else:
        print("\n\nERROR: No data was successfully processed")


if __name__ == "__main__":
    main()
