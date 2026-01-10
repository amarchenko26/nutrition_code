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
        df_sample = pd.read_csv(f, delimiter='\t', nrows=10000, low_memory=False, encoding='latin-1')
        print(f"Columns: {df_sample.columns.tolist()}")

        # Detect column names once
        df_sample.columns = df_sample.columns.str.lower()
        upc_col = next((col for col in df_sample.columns if 'upc' in col and 'ver' not in col), None)
        trip_col = next((col for col in df_sample.columns if 'trip_code' in col), None)

        if not upc_col or not trip_col:
            print(f"ERROR: Missing required columns. Found: {df_sample.columns.tolist()}")
            return None

        print(f"\nKey columns: upc={upc_col}, trip_code={trip_col}")
        print("\nProcessing full file...")
        f.seek(0)

        # Define standardized columns to keep from purchases
        standard_purchase_cols = ['trip_code_uc', 'upc', 'quantity', 'total_price_paid',
                                 'coupon_value', 'deal_flag_uc']

        # Prepare products dataframes - keep only needed columns
        products_df_food = products_df_filtered.copy()
        products_df_food.columns = products_df_food.columns.str.lower()
        upc_col_products = next(col for col in products_df_food.columns if 'upc' in col and 'ver' not in col)

        print(f"Processing in chunks of 100,000 rows...")
        print(f"Standardizing to columns: {standard_purchase_cols}")

        # Track stats
        filtered_chunks = []
        total_rows = 0
        kept_rows = 0

        for i, chunk in enumerate(pd.read_csv(f, delimiter='\t', chunksize=100000,
                                             low_memory=False, encoding='latin-1')):
            total_rows += len(chunk)
            chunk.columns = chunk.columns.str.lower()

            # Keep only standard purchase columns (drop everything else including HMS columns)
            available_std_cols = [col for col in standard_purchase_cols if col in chunk.columns]
            chunk = chunk[available_std_cols]

            # Merge with food products only (inner join = keep only matched)
            filtered = chunk.merge(products_df_food, left_on=upc_col, right_on=upc_col_products, how='inner')

            # Merge with trips to get household information
            filtered = filtered.merge(trips_df, on=trip_col, how='left')

            kept_rows += len(filtered)
            filtered_chunks.append(filtered)

            if (i + 1) % 10 == 0:
                print(f"  Processed {total_rows:,} rows, kept {kept_rows:,} ({kept_rows/total_rows*100:.1f}%)")

        print(f"\n\nFinal results:")
        print(f"  Total rows processed: {total_rows:,}")
        print(f"  Rows kept (food only): {kept_rows:,}")
        print(f"  Overall reduction: {(1 - kept_rows/total_rows)*100:.1f}%")

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

    return purchases_filtered


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
    # years_to_process = [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    years_to_process = [2021, 2022, 2023] 

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

    # Output directory for partitioned parquet dataset
    output_dir = f'/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_all_years_food_only'

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    print(f"\nOutput directory: {output_dir}")
    print("Each year will be saved as a separate partition")

    # Process each year using the same master products file
    # Write each year to its own partition (no memory accumulation)
    total_rows_written = 0
    years_processed = 0
    years_succeeded = []

    for year in years_to_process:
        result = process_year(base_path, year, products_df_filtered, products_df, explore_structure)

        if result is not None:
            # Ensure panel_year column exists for partitioning
            if 'panel_year' not in result.columns:
                print(f"Warning: panel_year not found, adding it manually as {year}")
                result['panel_year'] = year

            # Fix mixed-type columns that cause PyArrow errors
            # Convert object-dtype columns to string to handle mixed types
            for col in result.columns:
                if result[col].dtype == 'object':
                    result[col] = result[col].astype(str)

            # Write this year's data to a partition
            # Each partition is written independently without reading other partitions
            print(f"\nWriting year {year} to partition...")
            result.to_parquet(
                output_dir,
                partition_cols=['panel_year'],
                engine='pyarrow',
                compression='snappy',
                index=False
            )

            total_rows_written += len(result)
            years_processed += 1
            years_succeeded.append(year)
            print(f"✓ Year {year}: {len(result):,} rows written (Total so far: {total_rows_written:,})")

            # Free memory
            del result
        else:
            print(f"✗ Year {year}: Failed")

        # Only explore structure for first year
        explore_structure = False

    # Final summary
    if years_processed > 0:
        print("\n\n" + "=" * 80)
        print("PROCESSING COMPLETE")
        print("=" * 80)

        print(f"Successfully processed {years_processed} year(s): {years_succeeded}")
        print(f"Total rows written: {total_rows_written:,}")
        print(f"Output directory: {output_dir}")
        print("\nTo read the full dataset:")
        print(f"  df = pd.read_parquet('{output_dir}')")
        print("\nTo read specific years:")
        print(f"  df = pd.read_parquet('{output_dir}', filters=[('panel_year', 'in', [2020, 2021])])")
    else:
        print("\n\nERROR: No data was successfully processed")

if __name__ == "__main__":
    main()
