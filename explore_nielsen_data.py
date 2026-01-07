"""
Nielsen Consumer Panel Data Explorer

This script efficiently explores Nielsen Consumer Panel datasets by:
1. Extracting nested .tsv files from .tgz archives
2. Merging purchases with product master data
3. Filtering out non-food categories
4. Analyzing corn-derived food consumption
"""

import pandas as pd
import tarfile
import os
from io import BytesIO
import numpy as np

def explore_tarball_structure(tarball_path):
    """
    Explore the structure of the tarball to understand nested folders
    """
    print(f"\nExploring tarball structure: {tarball_path}")
    print("=" * 80)

    with tarfile.open(tarball_path, 'r:gz') as tar:
        members = tar.getmembers()
        print(f"Total files in tarball: {len(members)}")

        # Group by directory structure
        for member in members:
            if member.isfile():
                print(f"  {member.name} ({member.size / 1024 / 1024:.2f} MB)")

def load_products_master(master_tarball_path):
    """
    Load the products master file from Master_Files2004-2020.tgz
    This contains UPC codes and product_group_code mappings for all years

    Parameters:
    -----------
    master_tarball_path : str
        Path to Master_Files2004-2020.tgz

    Returns:
    --------
    products_df : DataFrame
        Master products dataframe
    """
    print("\nLoading master products file (2004-2020)...")
    print("-" * 80)

    if not os.path.exists(master_tarball_path):
        print(f"ERROR: Master file not found: {master_tarball_path}")
        return None

    with tarfile.open(master_tarball_path, 'r:gz') as tar:
        # Find the products master file
        products_file = None
        for member in tar.getmembers():
            if 'Master_Files' in member.name and 'products.tsv' in member.name:
                products_file = member
                break

        if not products_file:
            print("ERROR: Could not find Master_Files/.../products.tsv")
            return None

        print(f"Found: {products_file.name}")

        # Extract and load
        f = tar.extractfile(products_file)
        products_df = pd.read_csv(f, delimiter='\t', low_memory=False, encoding='latin-1')

        print(f"Shape: {products_df.shape}")
        print(f"Columns: {products_df.columns.tolist()}")
        print(f"\nFirst few rows:")
        print(products_df.head())

        return products_df

def filter_products_by_department(products_df, drop_department_codes):
    """
    Filter products by department_code

    Parameters:
    -----------
    products_df : DataFrame
        Products master data
    drop_department_codes : list
        List of department_code values to exclude

    Returns:
    --------
    products_df_filtered : DataFrame
        Products dataframe with only food departments
    """
    print("\n" + "=" * 80)
    print("FILTERING BY DEPARTMENT CODE")
    print("=" * 80)

    if 'department_code' not in products_df.columns:
        print("ERROR: department_code column not found in products.tsv")
        print(f"Available columns: {products_df.columns.tolist()}")
        return None

    # Show department distribution
    dept_counts = products_df['department_code'].value_counts().sort_index()
    print(f"\nDepartment code distribution:")
    for dept_code, count in dept_counts.items():
        status = "DROP" if dept_code in drop_department_codes else "KEEP"
        print(f"  {dept_code}: {count:,} products [{status}]")

    # Filter out unwanted departments
    products_filtered = products_df[~products_df['department_code'].isin(drop_department_codes)]

    print(f"\n\nFiltering results:")
    print(f"  Original products: {len(products_df):,}")
    print(f"  Dropped products: {len(products_df) - len(products_filtered):,}")
    print(f"  Kept products: {len(products_filtered):,}")
    print(f"  Reduction: {(1 - len(products_filtered)/len(products_df))*100:.1f}%")

    return products_filtered

def load_trips(tarball_path, year):
    """
    Load trips file for a specific year to get household and trip information

    Parameters:
    -----------
    tarball_path : str
        Path to the .tgz file
    year : int
        Year to process

    Returns:
    --------
    trips_df : DataFrame
        Trips dataframe with household_code, purchase_date, etc.
    """
    print(f"\nLoading trips data for {year}...")
    print("-" * 80)

    with tarfile.open(tarball_path, 'r:gz') as tar:
        # Find the trips file for this year
        trips_file = None
        for member in tar.getmembers():
            if f'/{year}/' in member.name and f'trips_{year}.tsv' in member.name:
                trips_file = member
                break

        if not trips_file:
            # Try alternative naming
            for member in tar.getmembers():
                if f'{year}' in member.name and 'trip' in member.name.lower() and member.name.endswith('.tsv'):
                    trips_file = member
                    break

        if not trips_file:
            print(f"ERROR: Could not find trips file for {year}")
            return None

        print(f"Found: {trips_file.name}")

        # Load trips file
        f = tar.extractfile(trips_file)
        trips_df = pd.read_csv(f, delimiter='\t', low_memory=False, encoding='latin-1')

        print(f"Shape: {trips_df.shape}")
        print(f"Columns: {trips_df.columns.tolist()}")

        # Standardize column names to lowercase
        trips_df.columns = trips_df.columns.str.lower()

        # Keep only needed columns
        keep_cols = ['trip_code_uc', 'household_code', 'purchase_date', 'retailer_code',
                     'store_code_uc', 'panel_year', 'store_zip3', 'total_spent']

        # Check which columns exist
        available_cols = [col for col in keep_cols if col in trips_df.columns]
        missing_cols = [col for col in keep_cols if col not in trips_df.columns]

        if missing_cols:
            print(f"Warning: Some columns not found in trips file: {missing_cols}")

        trips_df = trips_df[available_cols]

        print(f"Loaded {len(trips_df):,} trips")
        print(f"Sample:")
        print(trips_df.head())

        return trips_df


def load_and_filter_purchases(tarball_path, year, products_df_filtered, products_df_full):
    """
    Load purchases file for a specific year and merge with filtered products and trips

    Parameters:
    -----------
    tarball_path : str
        Path to the .tgz file
    year : int
        Year to process
    products_df_filtered : DataFrame
        Products dataframe already filtered to only include food departments
    products_df_full : DataFrame
        Full products dataframe (before filtering) to identify food vs non-food
    """
    print(f"\n\n" + "=" * 80)
    print(f"LOADING PURCHASES DATA FOR {year}")
    print("=" * 80)

    # First, load trips data to get household information
    trips_df = load_trips(tarball_path, year)

    if trips_df is None:
        print("ERROR: Could not load trips data")
        return None

    with tarfile.open(tarball_path, 'r:gz') as tar:
        # Find the purchases file for this year
        purchases_file = None
        for member in tar.getmembers():
            if f'/{year}/' in member.name and f'purchases_{year}.tsv' in member.name:
                purchases_file = member
                break

        if not purchases_file:
            # Try alternative naming
            for member in tar.getmembers():
                if f'{year}' in member.name and 'purchase' in member.name.lower() and member.name.endswith('.tsv'):
                    purchases_file = member
                    break

        if not purchases_file:
            print(f"ERROR: Could not find purchases file for {year}")
            return None

        print(f"\nFound: {purchases_file.name}")
        print(f"Size: {purchases_file.size / 1024 / 1024:.2f} MB")

        # Load purchases in chunks to manage memory
        f = tar.extractfile(purchases_file)

        print("\nReading first chunk to explore structure...")
        df_sample = pd.read_csv(f, delimiter='\t', nrows=10000,
                               low_memory=False, encoding='latin-1')

        print(f"Columns: {df_sample.columns.tolist()}")
        print(f"\nSample data:")
        print(df_sample.head())

        # Check for key columns
        required_cols = ['household_cd', 'upc']
        missing_cols = [col for col in required_cols if col not in df_sample.columns]

        if missing_cols:
            # Try case-insensitive match
            print(f"\nColumn name variations found:")
            for req_col in required_cols:
                matches = [col for col in df_sample.columns if req_col.lower() in col.lower()]
                if matches:
                    print(f"  {req_col} -> {matches}")

        # Now load full file and merge with products
        print("\n\nLoading full purchases file and merging with filtered products...")
        f.seek(0)

        # Read in chunks and filter
        chunk_size = 100000
        filtered_chunks = []
        total_rows = 0
        kept_rows = 0

        print(f"\nProcessing in chunks of {chunk_size:,} rows...")

        # Prepare both filtered and full products dataframes
        products_df_food = products_df_filtered.copy()
        products_df_food.columns = products_df_food.columns.str.lower()

        products_df_all = products_df_full.copy()
        products_df_all.columns = products_df_all.columns.str.lower()

        # Find UPC column name
        upc_col_products = None
        for col in products_df_food.columns:
            if 'upc' in col.lower():
                upc_col_products = col
                break

        if upc_col_products is None:
            print("ERROR: No UPC column found in products dataframe")
            return None

        # Get sets of UPCs
        food_upcs = set(products_df_food[upc_col_products].dropna())
        all_upcs = set(products_df_all[upc_col_products].dropna())

        print(f"Food product UPCs: {len(food_upcs):,}")
        print(f"All product UPCs: {len(all_upcs):,}")

        # Track unmatched FOOD UPCs only
        unmatched_food_upcs = set()
        non_food_count = 0

        for i, chunk in enumerate(pd.read_csv(f, delimiter='\t', chunksize=chunk_size,
                                             low_memory=False, encoding='latin-1')):
            total_rows += len(chunk)

            # Standardize column names (handle case variations)
            chunk.columns = chunk.columns.str.lower()

            # Find UPC column in purchases
            upc_col = None
            for col in chunk.columns:
                if 'upc' in col:
                    upc_col = col
                    break

            if upc_col is None:
                print(f"Warning: No UPC column found in purchases. Columns: {chunk.columns.tolist()}")
                continue

            # Find trip_code column in purchases
            trip_col = None
            for col in chunk.columns:
                if 'trip_code' in col:
                    trip_col = col
                    break

            if trip_col is None:
                print(f"Warning: No trip_code column found in purchases. Columns: {chunk.columns.tolist()}")
                continue

            # Step 1: Identify food vs non-food purchases
            # First merge with FULL products to categorize
            chunk_with_full = chunk.merge(products_df_all[[upc_col_products, 'department_code']],
                                          on=upc_col, how='left', suffixes=('', '_full'))

            # Separate food purchases (those in food departments) from non-food
            is_in_all_products = chunk_with_full['department_code'].notna()

            # Count non-food items (in products file but filtered out)
            non_food_in_chunk = len(chunk_with_full[is_in_all_products & ~chunk_with_full[upc_col].isin(food_upcs)])
            non_food_count += non_food_in_chunk

            # Step 2: Merge with filtered products (food only)
            merged = chunk.merge(products_df_food, on=upc_col, how='left', indicator=True)

            # Track UPCs that didn't match in food products
            unmatched_in_chunk = merged[merged['_merge'] == 'left_only'][upc_col].unique()

            # Only track as "unmatched food" if the UPC is NOT in the full products file
            # (i.e., it's truly missing, not just filtered out)
            for upc in unmatched_in_chunk:
                if upc not in all_upcs:
                    unmatched_food_upcs.add(upc)

            # Keep only matched rows (food items)
            filtered = merged[merged['_merge'] == 'both'].drop(columns=['_merge'])

            # Step 3: Merge with trips data to get household and trip information
            filtered = filtered.merge(trips_df, on=trip_col, how='left')

            kept_rows += len(filtered)
            filtered_chunks.append(filtered)

            if (i + 1) % 10 == 0:
                print(f"  Processed {total_rows:,} rows, kept {kept_rows:,} ({kept_rows/total_rows*100:.1f}%)")

        print(f"\n\nFinal results:")
        print(f"  Total rows processed: {total_rows:,}")
        print(f"  Rows kept (food only): {kept_rows:,}")
        print(f"  Non-food rows dropped: {non_food_count:,}")
        print(f"  Rows dropped (other): {total_rows - kept_rows - non_food_count:,}")
        print(f"  Overall reduction: {(1 - kept_rows/total_rows)*100:.1f}%")

        print(f"\n\nUnmatched Food UPCs Analysis:")
        print(f"  Food product UPCs in master file: {len(food_upcs):,}")
        print(f"  Food UPCs NOT found in master file: {len(unmatched_food_upcs):,}")
        if len(food_upcs) > 0:
            print(f"  Food UPC match rate: {len(food_upcs)/(len(food_upcs) + len(unmatched_food_upcs))*100:.1f}%")
        print(f"\n  Note: These are food product UPCs in purchases that don't exist in the master products file.")
        print(f"        This could indicate discontinued products, regional products, or data errors.")
        print(f"        Non-food products were excluded from this count.")

        # Combine all filtered chunks
        if filtered_chunks:
            purchases_filtered = pd.concat(filtered_chunks, ignore_index=True)
            print(f"\nFiltered dataset shape: {purchases_filtered.shape}")
            print(f"\nSample of filtered data:")
            print(purchases_filtered.head(20))

            return purchases_filtered
        else:
            print("No data remained after filtering")
            return None

def process_year(base_path, year, products_df_filtered, products_df_full, explore_structure=False):
    """
    Process a single year of Nielsen data

    Parameters:
    -----------
    base_path : str
        Base directory containing the Consumer_Panel_Data_YYYY.tgz files
    year : int
        Year to process
    products_df_filtered : DataFrame
        Pre-loaded and filtered products master dataframe (food only)
    products_df_full : DataFrame
        Full products dataframe (before filtering)
    explore_structure : bool
        Whether to print detailed tarball structure (useful for first run)
    """
    print(f"\n\n{'=' * 80}")
    print(f"PROCESSING YEAR {year}")
    print("=" * 80)

    tarball_path = f'{base_path}/Consumer_Panel_Data_{year}.tgz'

    if not os.path.exists(tarball_path):
        print(f"ERROR: File not found: {tarball_path}")
        return None

    print(f"File: {tarball_path}")
    print(f"File size: {os.path.getsize(tarball_path) / 1024 / 1024 / 1024:.2f} GB")

    # Step 1: Explore structure (optional)
    if explore_structure:
        explore_tarball_structure(tarball_path)

    # Step 2: Load and filter purchases using pre-loaded products
    purchases_filtered = load_and_filter_purchases(tarball_path, year, products_df_filtered, products_df_full)

    # Step 3: Save filtered dataset
    if purchases_filtered is not None:
        output_path = f'/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_{year}_food_only.parquet'
        print(f"\n\nSaving filtered data to: {output_path}")
        purchases_filtered.to_parquet(output_path, index=False, engine='pyarrow', compression='snappy')

        # Calculate file size
        file_size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f"✓ Saved successfully! ({len(purchases_filtered):,} rows, {file_size_mb:.1f} MB)")

        return purchases_filtered
    else:
        return None


def main():
    """
    Main exploration and filtering function
    """
    print("NIELSEN CONSUMER PANEL DATA EXPLORER")
    print("=" * 80)

    base_path = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/consumer'

    # ========================================================================
    # CONFIGURATION: Edit these settings
    # ========================================================================

    # Path to master products file (covers 2004-2020)
    master_products_path = f'{base_path}/Master_Files2004-2020.tgz'

    # List of years to process
    years_to_process = [2004]  # Add more years like [2004, 2005, 2006, ...]

    # Department codes to DROP
    # 0 = HEALTH & BEAUTY CARE
    # 7 = NON-FOOD GROCERY
    # 8 = ALCOHOLIC BEVERAGES
    # 9 = GENERAL MERCHANDISE
    drop_department_codes = [0, 7, 8, 9]

    # Whether to show detailed tarball structure (useful for first run)
    explore_structure = True

    # ========================================================================

    print(f"\nProcessing {len(years_to_process)} year(s): {years_to_process}")
    print(f"Dropping department codes: {drop_department_codes}")
    print("  0 = HEALTH & BEAUTY CARE")
    print("  7 = NON-FOOD GROCERY")
    print("  8 = ALCOHOLIC BEVERAGES")
    print("  9 = GENERAL MERCHANDISE")

    # Load master products file ONCE for all years
    print("\n" + "=" * 80)
    print("LOADING MASTER PRODUCTS FILE")
    print("=" * 80)

    products_df = load_products_master(master_products_path)

    if products_df is None:
        print("ERROR: Could not load master products file. Exiting.")
        return

    # Filter products by department code
    products_df_filtered = filter_products_by_department(products_df, drop_department_codes)

    if products_df_filtered is None:
        print("ERROR: Could not filter products. Exiting.")
        return

    print(f"\n✓ Master products loaded and filtered: {len(products_df_filtered):,} food products ready")

    # Process each year using the same master products file
    results = {}
    for year in years_to_process:
        result = process_year(base_path, year, products_df_filtered, products_df, explore_structure)
        results[year] = result
        # Only explore structure for first year
        explore_structure = False

    # Summary
    print("\n\n" + "=" * 80)
    print("PROCESSING COMPLETE")
    print("=" * 80)
    for year, df in results.items():
        if df is not None:
            print(f"  {year}: ✓ {len(df):,} food purchase rows saved")
        else:
            print(f"  {year}: ✗ Failed")

    print("\n\nNext steps:")
    print("1. Review the filtered data")
    print("2. Update drop_department_codes list if needed (see CONFIGURATION section)")
    print("3. Add more years to years_to_process list")
    print("4. Identify corn-derived products using product descriptions/UPCs")
    print("5. Analyze consumption patterns over time")

if __name__ == "__main__":
    main()
