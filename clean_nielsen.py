"""
Nielsen Consumer Panel Data Explorer

This script efficiently explores Nielsen Consumer Panel datasets by:
1. Extracting nested .tsv files from .tgz archives
2. Merging purchases with product master data
3. Filtering out non-food categories
4. Deflating prices to real values using CPI data
5. Analyzing corn-derived food consumption
"""

import pandas as pd
import tarfile
import os
import shutil
from io import BytesIO
import numpy as np
import re


# ============================================================================
# PRODUCT MODULE NAME NORMALIZATION
# ============================================================================
# Nielsen changed their naming conventions in 2021. This function normalizes
# product module names to allow consistent matching across all years.

def normalize_module_name(name):
    """
    Normalize product module names to allow matching across 2020 and 2021+ formats.

    Key transformations:
    1. Replace hyphens/slashes with spaces
    2. Replace & with AND
    3. Remove quotes and periods
    4. Replace ORIENTAL with ASIAN
    5. Expand abbreviations used in 2021+ data
    6. Normalize whitespace

    Parameters:
    -----------
    name : str
        Original product module name

    Returns:
    --------
    str : Normalized product module name
    """
    if pd.isna(name) or name == 'nan':
        return name

    s = str(name).upper().strip()

    # Replace hyphens and dashes with spaces
    s = s.replace('-', ' ').replace('–', ' ').replace('—', ' ')

    # Remove special characters
    s = s.replace('/', ' ').replace('&', ' AND ')
    s = s.replace('"', '').replace("'", "").replace('\\', '')
    s = s.replace('.', '')

    # Replace "ORIENTAL" with "ASIAN" (Nielsen changed this terminology in 2021)
    s = s.replace('ORIENTAL', 'ASIAN')

    # Expand abbreviations used in 2021+ data
    abbreviations = [
        (r'\bRFRGR\b', 'REFRIGERATED'),
        (r'\bRFRGRTD\b', 'REFRIGERATED'),
        (r'\bFRZN\b', 'FROZEN'),
        (r'\bFRSH\b', 'FRESH'),
        (r'\bCNTNR\b', 'CONTAINER'),
        (r'\bCNTNRS\b', 'CONTAINERS'),
        (r'\bDHYDR\b', 'DEHYDRATED'),
        (r'\bSHLF STBL\b', 'SHELF STABLE'),
        (r'\bRMNNG\b', 'REMAINING'),
        (r'\bMXCN\b', 'MEXICAN'),
        (r'\bSWT RLS\b', 'SWEET ROLLS'),
        (r'\bSTRDL\b', 'STRUDEL'),
        (r'\bDGH\b', 'DOUGH'),
        (r'\bBRWNS\b', 'BROWNIES'),
        (r'\bENHNC\b', 'ENHANCERS'),
        (r'\bVGTRN\b', 'VEGETARIAN'),
        (r'\bWHT NRTHR NVY\b', 'WHITE NORTHERN NAVY'),
        (r'\bNRTHR\b', 'NORTHERN'),
        (r'\bNVY\b', 'NAVY'),
        (r'\bWHT\b', 'WHITE'),
        (r'\bCND\b', 'CANNED'),
        (r'\bTST\b', 'TOAST'),
        (r'\bSNCKS\b', 'SNACKS'),
        (r'\bFRTS\b', 'FRUITS'),
        (r'\bOTHR\b', 'OTHER'),
        (r'\bSRVD\b', 'SERVED'),
        (r'\bUNDR\b', 'UNDER'),
    ]

    for pattern, replacement in abbreviations:
        s = re.sub(pattern, replacement, s)

    # Normalize "ALL OTHR" -> "ALL OTHER"
    s = s.replace('ALL OTHR', 'ALL OTHER')

    # Normalize multiple spaces to single space
    s = ' '.join(s.split())

    return s


def add_normalized_module_column(df):
    """
    Add a normalized product module column to the dataframe.

    Parameters:
    -----------
    df : DataFrame
        DataFrame with product_module column

    Returns:
    --------
    DataFrame with added product_module_normalized column
    """
    if 'product_module' in df.columns:
        df['product_module_normalized'] = df['product_module'].apply(normalize_module_name)
        print(f"  Added normalized product module column")
        print(f"    Original unique modules: {df['product_module'].nunique()}")
        print(f"    Normalized unique modules: {df['product_module_normalized'].nunique()}")
    else:
        print("  WARNING: product_module column not found")
    return df


def standardize_product_columns(df):
    """
    Standardize product module/group columns to product_module/product_group.
    Only renames _descr/_desc columns (not _code columns) to avoid duplicates.
    """
    rename_map = {}
    for col in df.columns:
        if col in ['product_module_descr', 'product_module_desc']:
            rename_map[col] = 'product_module'
        if col in ['product_group_descr', 'product_group_desc']:
            rename_map[col] = 'product_group'
    if rename_map:
        df = df.rename(columns=rename_map)
    return df

# ============================================================================
# PRICE DEFLATION CONFIGURATION
# ============================================================================
# Target year for deflation (will use annual average CPI for this year)
TARGET_YEAR = 2013

# CPI data path (FRED CPIEBEV - Food and Beverages CPI)
CPI_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/price_deflator/CPIEBEV.csv'

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


def deflate_prices(df, cpi_lookup, target_cpi, year, columns_to_deflate=COLUMNS_TO_DEFLATE):
    """
    Deflate prices in a DataFrame using CPI data.

    Parameters:
    -----------
    df : DataFrame
        DataFrame with price columns and purchase_date
    cpi_lookup : dict
        Mapping of (year, month) -> CPI value
    target_cpi : float
        Target year CPI value
    year : int
        Panel year (fallback if purchase_date missing)
    columns_to_deflate : list
        List of column names to deflate

    Returns:
    --------
    DataFrame with deflated price columns added
    """
    n_rows = len(df)

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

def initialize_summary_stats():
    """Initialize dictionary to accumulate summary statistics across years."""
    return {
        'yearly_counts': {},  # {year: n_rows}
        'department_counts': {},  # {department: count}
        'product_group_counts': {},  # {product_group: count}
        'product_module_counts': {},  # {product_module: count}
        'missing_counts': {},  # {column: missing_count}
        'total_counts': {},  # {column: total_count}
        'column_names': None,
        'column_dtypes': None,
        'total_rows': 0,
        'total_spending': 0.0,
        'total_quantity': 0,
        'unique_households': set(),
        'unique_upcs': set(),
    }


def update_summary_stats(stats, df, year):
    """
    Update summary statistics with data from one year.
    This is memory-efficient since we only compute aggregates, not store raw data.
    """
    n_rows = len(df)
    stats['yearly_counts'][year] = n_rows
    stats['total_rows'] += n_rows

    # Column names and dtypes (only need to capture once)
    if stats['column_names'] is None:
        stats['column_names'] = df.columns.tolist()
        stats['column_dtypes'] = {col: str(df[col].dtype) for col in df.columns}

    # Department counts
    if 'department_descr' in df.columns:
        dept_counts = df['department_descr'].value_counts()
        for dept, count in dept_counts.items():
            stats['department_counts'][dept] = stats['department_counts'].get(dept, 0) + count

    # Product group counts
    if 'product_group' in df.columns:
        pg_counts = df['product_group'].value_counts()
        for pg, count in pg_counts.items():
            stats['product_group_counts'][pg] = stats['product_group_counts'].get(pg, 0) + count

    # Product module counts (top 100 only to avoid huge dict)
    if 'product_module' in df.columns:
        pm_counts = df['product_module'].value_counts()
        for pm, count in pm_counts.items():
            stats['product_module_counts'][pm] = stats['product_module_counts'].get(pm, 0) + count

    # Missing counts per column
    for col in df.columns:
        missing = df[col].isna().sum()
        stats['missing_counts'][col] = stats['missing_counts'].get(col, 0) + missing
        stats['total_counts'][col] = stats['total_counts'].get(col, 0) + n_rows

    # Spending and quantity totals
    if 'total_price_paid' in df.columns:
        stats['total_spending'] += df['total_price_paid'].sum()
    if 'quantity' in df.columns:
        stats['total_quantity'] += df['quantity'].sum()

    # Unique households and UPCs (sample to avoid memory issues)
    if 'household_code' in df.columns:
        stats['unique_households'].update(df['household_code'].dropna().unique()[:10000])
    if 'upc' in df.columns:
        stats['unique_upcs'].update(df['upc'].dropna().unique()[:50000])

    return stats


def save_summary_stats(stats, output_dir):
    """Save summary statistics to CSV files in the output directory."""
    print("\n" + "=" * 80)
    print("SAVING SUMMARY STATISTICS")
    print("=" * 80)

    # 2. Department counts
    dept_df = pd.DataFrame([
        {'department_descr': dept, 'n_purchases': count, 'pct_of_total': count / stats['total_rows'] * 100}
        for dept, count in sorted(stats['department_counts'].items(), key=lambda x: -x[1])
    ])
    dept_path = os.path.join(output_dir, 'summary_department_counts.csv')
    dept_df.to_csv(dept_path, index=False)
    print(f"✓ Saved department counts: {dept_path}")

    # 3. Product group counts
    pg_df = pd.DataFrame([
        {'product_group': pg, 'n_purchases': count, 'pct_of_total': count / stats['total_rows'] * 100}
        for pg, count in sorted(stats['product_group_counts'].items(), key=lambda x: -x[1])
    ])
    pg_path = os.path.join(output_dir, 'summary_product_group_counts.csv')
    pg_df.to_csv(pg_path, index=False)
    print(f"✓ Saved product group counts: {pg_path}")

    # 4. Product module counts (top 200)
    pm_sorted = sorted(stats['product_module_counts'].items(), key=lambda x: -x[1])[:200]
    pm_df = pd.DataFrame([
        {'product_module': pm, 'n_purchases': count, 'pct_of_total': count / stats['total_rows'] * 100}
        for pm, count in pm_sorted
    ])
    pm_path = os.path.join(output_dir, 'summary_product_module_counts_top200.csv')
    pm_df.to_csv(pm_path, index=False)
    print(f"✓ Saved product module counts (top 200): {pm_path}")

    # 5. Missing values per column
    missing_df = pd.DataFrame([
        {
            'column': col,
            'n_missing': stats['missing_counts'].get(col, 0),
            'n_total': stats['total_counts'].get(col, 0),
            'pct_missing': stats['missing_counts'].get(col, 0) / stats['total_counts'].get(col, 1) * 100,
            'dtype': stats['column_dtypes'].get(col, 'unknown')
        }
        for col in stats['column_names']
    ])
    missing_path = os.path.join(output_dir, 'summary_missing_values.csv')
    missing_df.to_csv(missing_path, index=False)
    print(f"✓ Saved missing values summary: {missing_path}")

    # 6. Overall summary stats
    overall = {
        'total_purchases': stats['total_rows'],
        'total_spending': stats['total_spending'],
        'total_quantity': stats['total_quantity'],
        'avg_price_per_purchase': stats['total_spending'] / stats['total_rows'] if stats['total_rows'] > 0 else 0,
        'n_unique_households_sampled': len(stats['unique_households']),
        'n_unique_upcs_sampled': len(stats['unique_upcs']),
        'n_years': len(stats['yearly_counts']),
        'year_min': min(stats['yearly_counts'].keys()) if stats['yearly_counts'] else None,
        'year_max': max(stats['yearly_counts'].keys()) if stats['yearly_counts'] else None,
        'n_departments': len(stats['department_counts']),
        'n_product_groups': len(stats['product_group_counts']),
        'n_product_modules': len(stats['product_module_counts']),
        'n_columns': len(stats['column_names']) if stats['column_names'] else 0,
    }
    overall_df = pd.DataFrame([overall])
    overall_path = os.path.join(output_dir, 'summary_overall.csv')
    overall_df.to_csv(overall_path, index=False)
    print(f"✓ Saved overall summary: {overall_path}")

    # Print summary to console
    print("\n" + "-" * 80)
    print("DATASET SUMMARY")
    print("-" * 80)
    print(f"Total purchases: {stats['total_rows']:,}")
    print(f"Total spending: ${stats['total_spending']:,.2f}")
    print(f"Years covered: {overall['year_min']} - {overall['year_max']} ({overall['n_years']} years)")
    print(f"Unique households (sampled): {len(stats['unique_households']):,}")
    print(f"Unique UPCs (sampled): {len(stats['unique_upcs']):,}")
    print(f"Departments: {overall['n_departments']}")
    print(f"Product groups: {overall['n_product_groups']}")
    print(f"Product modules: {overall['n_product_modules']}")
    print(f"Columns: {overall['n_columns']}")

    return overall


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

def load_products_2021_plus(tarball_path, year):
    """
    Load products from 2021+ annual files (productdesc.tsv + producthierarchy.tsv)
    These files have a different structure than the master file.

    Parameters:
    -----------
    tarball_path : str
        Path to Consumer_Panel_Data_YYYY.tgz
    year : int
        Year being processed

    Returns:
    --------
    products_df : DataFrame
        Products dataframe with standardized column names matching pre-2020 convention
    """
    print(f"\nLoading products from {year} annual files...")
    print("-" * 80)

    if not os.path.exists(tarball_path):
        print(f"ERROR: Tarball not found: {tarball_path}")
        return None

    with tarfile.open(tarball_path, 'r:gz') as tar:
        # Find productdesc.tsv
        productdesc_file = None
        producthierarchy_file = None

        for member in tar.getmembers():
            if 'productdesc.tsv' in member.name:
                productdesc_file = member
            if 'producthierarchy.tsv' in member.name:
                producthierarchy_file = member

        if not productdesc_file:
            print("ERROR: Could not find productdesc.tsv")
            return None
        if not producthierarchy_file:
            print("ERROR: Could not find producthierarchy.tsv")
            return None

        print(f"Found: {productdesc_file.name}")
        print(f"Found: {producthierarchy_file.name}")

        # Load productdesc.tsv
        f = tar.extractfile(productdesc_file)
        productdesc_df = pd.read_csv(f, delimiter='\t', low_memory=False, encoding='latin-1',
                                     dtype={'upc': str})
        print(f"productdesc shape: {productdesc_df.shape}")

        # Load producthierarchy.tsv
        f = tar.extractfile(producthierarchy_file)
        producthierarchy_df = pd.read_csv(f, delimiter='\t', low_memory=False, encoding='latin-1',
                                          dtype={'upc': str})
        print(f"producthierarchy shape: {producthierarchy_df.shape}")

        # Keep only needed columns from each file
        module_col = 'product_module_descr'
        if not module_col in productdesc_df.columns:
            print("ERROR: product module column not found in productdesc.tsv")
            print(f"Available columns: {productdesc_df.columns.tolist()}")
            return None

        productdesc_cols = ['upc',
                            'product_descr', module_col,
                            'multi',
                            'year']
        producthierarchy_cols = ['upc', 
                                 'department', 
                                 'super_category']

        productdesc_df = productdesc_df[productdesc_cols]
        producthierarchy_df = producthierarchy_df[producthierarchy_cols]

        # Merge on upc
        products_df = productdesc_df.merge(producthierarchy_df, on='upc', how='left')
        print(f"Merged products shape: {products_df.shape}")

        # Rename columns to standardized naming
        products_df = products_df.rename(columns={
            'product_descr': 'upc_descr',
            module_col: 'product_module',
            'department': 'department_descr',
            'super_category': 'product_group'
        })
        products_df = standardize_product_columns(products_df)

        print(f"Columns after rename: {products_df.columns.tolist()}")
        print(f"\nFirst few rows:")
        print(products_df.head())

        return products_df


def filter_products_2021_plus(products_df, drop_departments, drop_product_group, drop_product_module):
    """
    Filter 2021+ products by department, product group, and product module

    Parameters:
    -----------
    products_df : DataFrame
        Products dataframe from 2021+ files
    drop_departments : list
        List of department_descr values to exclude (e.g., ['ALCOHOL', 'BABY CARE', ...])
    drop_product_group : list
        List of product_group values to exclude
    drop_product_module : list
        List of product_module values to exclude

    Returns:
    --------
    products_df_filtered : DataFrame
        Filtered products dataframe
    """
    print("\n" + "=" * 80)
    print("FILTERING 2021+ PRODUCTS")
    print("=" * 80)

    if 'department_descr' not in products_df.columns:
        print("ERROR: department_descr column not found")
        print(f"Available columns: {products_df.columns.tolist()}")
        return None

    # Show department distribution
    dept_counts = products_df['department_descr'].value_counts()
    print(f"\nDepartment distribution:")
    for dept, count in dept_counts.items():
        status = "DROP" if dept in drop_departments else "KEEP"
        print(f"  {dept}: {count:,} products [{status}]")

    # Filter out unwanted departments
    products_filtered = products_df[~products_df['department_descr'].isin(drop_departments)]

    print(f"\nAfter department filtering:")
    print(f"  Original products: {len(products_df):,}")
    print(f"  Kept products: {len(products_filtered):,}")

    # Filter by product_group
    if 'product_group' in products_filtered.columns:
        initial_count = len(products_filtered)
        products_filtered = products_filtered[~products_filtered['product_group'].isin(drop_product_group)]
        dropped_count = initial_count - len(products_filtered)

        print(f"\nAfter product_group filtering:")
        print(f"  Dropped products: {dropped_count:,}")
        print(f"  Kept products: {len(products_filtered):,}")
    else:
        print("Warning: product_group column not found; skipping product group filtering.")

    # Filter by product_module
    if 'product_module' in products_filtered.columns:
        initial_count = len(products_filtered)
        products_filtered = products_filtered[~products_filtered['product_module'].isin(drop_product_module)]
        dropped_count = initial_count - len(products_filtered)

        print(f"\nAfter product_module filtering:")
        print(f"  Dropped products: {dropped_count:,}")
        print(f"  Kept products: {len(products_filtered):,}")
    else:
        print("Warning: product_module column not found; skipping product module filtering.")

    # Keep only the standardized columns (matching pre-2020 structure where possible)
    keep_product_cols = ['upc',
                         'upc_descr',
                         'product_module',
                         'product_group',
                         'department_descr',
                         'multi']

    products_filtered = products_filtered[keep_product_cols]

    print(f"\nFinal filtering results:")
    print(f"  Original products: {len(products_df):,}")
    print(f"  Dropped products: {len(products_df) - len(products_filtered):,}")
    print(f"  Kept products: {len(products_filtered):,}")
    print(f"  Reduction: {(1 - len(products_filtered)/len(products_df))*100:.1f}%")

    return products_filtered


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
        products_df = pd.read_csv(f, delimiter='\t', low_memory=False, encoding='latin-1',
                                  dtype={'upc': str})
        products_df = products_df.reset_index(drop=True)
        products_df = standardize_product_columns(products_df)

        print(f"Shape: {products_df.shape}")
        print(f"Columns: {products_df.columns.tolist()}")
        print(f"\nFirst few rows:")
        print(products_df.head())

        return products_df

def filter_products_by_department(products_df, drop_department_desc_pre_2021, drop_product_group, drop_product_module):
    """
    Filter products by department_code

    Parameters:
    -----------
    products_df : DataFrame
        Products master data
    drop_department_desc_pre_2021 : list
        List of department_desc values to exclude
    drop_product_group : list
        List of product_group values to exclude
    drop_product_module : list
        List of product_module values to exclude

    Returns:
    --------
    products_df_filtered : DataFrame
        Products dataframe with only food departments
    """
    print("\n" + "=" * 80)
    print("FILTERING BY DEPARTMENT")
    print("=" * 80)

    if 'department_descr' not in products_df.columns:
        print("ERROR: department_descr column not found in products.tsv")
        print(f"Available columns: {products_df.columns.tolist()}")
        return None

    # Show department distribution
    dept_counts = products_df['department_descr'].value_counts().sort_index()
    print(f"\nDepartment distribution:")
    for dept, count in dept_counts.items():
        status = "DROP" if dept in drop_department_desc_pre_2021 else "KEEP"
        print(f"  {dept}: {count:,} products [{status}]")

    # Filter out unwanted departments
    products_filtered = products_df[~products_df['department_descr'].isin(drop_department_desc_pre_2021)].reset_index(drop=True)
    # Further filter by product_group
    if 'product_group' in products_filtered.columns:
        initial_count = len(products_filtered)
        products_filtered = products_filtered[~products_filtered['product_group'].isin(drop_product_group)]
        dropped_count = initial_count - len(products_filtered)

        print(f"\nAdditional filtering by product_group:")
        print(f"  Dropped products: {dropped_count:,}")
        print(f"  Kept products: {len(products_filtered):,}")
        print(f"  Additional reduction: {(dropped_count/initial_count)*100:.1f}%")
    else:
        print("Warning: product_group column not found; skipping additional filtering.")

    # Further filter by product_module
    if 'product_module' in products_filtered.columns:
        initial_count = len(products_filtered)
        products_filtered = products_filtered[~products_filtered['product_module'].isin(drop_product_module)]
        dropped_count = initial_count - len(products_filtered)

        print(f"\nAdditional filtering by product_module:")
        print(f"  Dropped products: {dropped_count:,}")
        print(f"  Kept products: {len(products_filtered):,}")
        print(f"  Additional reduction: {(dropped_count/initial_count)*100:.1f}%")
    else:
        print("Warning: product_module column not found; skipping additional filtering.")

    # Now filter to only the columns we're keeping 
    keep_product_cols = ['upc',
                         'upc_ver_uc',
                         'upc_descr',
                         'product_module',
                         'product_module_code',
                         'product_group',
                         'product_group_code',
                         'department_descr',
                         'brand_descr',
                         'multi',
                         'size1_amount',
                         'size1_units'] 

    products_filtered = products_filtered[keep_product_cols]
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


def load_and_filter_purchases(tarball_path, year, products_df_filtered):
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
        df_sample = pd.read_csv(f, delimiter='\t', nrows=10000, low_memory=False, encoding='latin-1',
                                dtype={'upc': str})
        print(f"Columns: {df_sample.columns.tolist()}")

        # Detect column names once
        df_sample.columns = df_sample.columns.str.lower()
        upc_col = next((col for col in df_sample.columns if 'upc' in col and 'ver' not in col), None)
        upc_ver_col = next((col for col in df_sample.columns if 'upc_ver' in col), None)
        trip_col = next((col for col in df_sample.columns if 'trip_code' in col), None)

        if not upc_col or not trip_col:
            print(f"ERROR: Missing required columns. Found: {df_sample.columns.tolist()}")
            return None

        print(f"\nKey columns: upc={upc_col}, trip_code={trip_col}")
        print("\nProcessing full file...")
        f.seek(0)

        # Define standardized columns to keep from purchases
        standard_purchase_cols = ['trip_code_uc', 'upc', 'upc_ver_uc',
                                 'quantity', 'total_price_paid',
                                 'coupon_value'] #, 'deal_flag_uc'
        if year >= 2021:
            standard_purchase_cols += ['size1_amount_hms', 'size1_unit_hms', 'product_module_code_hms']

        # Prepare products dataframes - keep only needed columns
        products_df_food = products_df_filtered.copy()
        products_df_food.columns = products_df_food.columns.str.lower()
        upc_col_products = next(col for col in products_df_food.columns if 'upc' in col and 'ver' not in col)
        upc_ver_col_products = next((col for col in products_df_food.columns if 'upc_ver' in col), None)

        print(f"Processing in chunks of 500,000 rows...")
        print(f"Standardizing to columns: {standard_purchase_cols}")

        # Track stats
        filtered_chunks = []
        total_rows = 0
        kept_rows = 0

        for i, chunk in enumerate(pd.read_csv(f, delimiter='\t', chunksize=500000,
                                             low_memory=False, encoding='latin-1',
                                             dtype={'upc': str})):
            total_rows += len(chunk)
            chunk.columns = chunk.columns.str.lower()

            # Keep only standard purchase columns (drop everything else including HMS columns)
            available_std_cols = [col for col in standard_purchase_cols if col in chunk.columns]
            chunk = chunk[available_std_cols]

            # Merge with food products only (inner join = keep only matched)
            # Per Nielsen manual: use both upc and upc_ver_uc when available
            if upc_ver_col and upc_ver_col_products and upc_ver_col in chunk.columns:
                filtered = chunk.merge(
                    products_df_food,
                    left_on=[upc_col, upc_ver_col],
                    right_on=[upc_col_products, upc_ver_col_products],
                    how='inner'
                )
                if i == 0:
                    print(f"  Merging on [{upc_col}, {upc_ver_col}] (both UPC and version)")
                
                # Drop upc_ver_uc after merge - only needed as a merge key
                for col in [upc_ver_col, upc_ver_col_products, 'upc_ver_uc']:
                    if col in filtered.columns:
                        filtered = filtered.drop(columns=col)
            else:
                filtered = chunk.merge(products_df_food, left_on=upc_col, right_on=upc_col_products, how='inner')
                if i == 0:
                    print(f"  Merging on [{upc_col}] only (upc_ver_uc not available in product data)")

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
            if 'size1_amount_hms' in purchases_filtered.columns and 'size1_unit_hms' in purchases_filtered.columns:
                purchases_filtered = purchases_filtered.rename(columns={
                    'size1_amount_hms': 'size1_amount',
                    'size1_unit_hms': 'size1_units',
                    'product_module_code_hms': 'product_module_code'
                })
            print(f"\nFiltered dataset shape: {purchases_filtered.shape}")
            print(f"\nSample of filtered data:")
            print(purchases_filtered.head(20))

            return purchases_filtered
        else:
            print("No data remained after filtering")
            return None


def process_year(base_path, year, products_df_filtered, explore_structure=False):
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
    purchases_filtered = load_and_filter_purchases(tarball_path, year, products_df_filtered)

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
    # years_to_process = [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    years_to_process = [2021, 2022, 2023, 2024] 

    # Departments to DROP (for pre-2021 master file)
    drop_department_desc_pre_2021 = [
        'HEALTH & BEAUTY CARE', 
        'NON-FOOD GROCERY', 
        'ALCOHOLIC BEVERAGES', 
        'GENERAL MERCHANDISE']
    
        # Departments to DROP for 2021+ 
    drop_departments_2021_plus = [
        'ALCOHOL',
        'BABY CARE',
        'GENERAL MERCHANDISE',
        'HEALTH & BEAUTY CARE',
        'HOUSEHOLD CARE',
        'PET CARE',
        'TOBACCO AND TOBACCO ALTERNATIVES', 
        'FLORAL',
        'DO NOT RELEASE',
        'NOT APPLICABLE',
        'MISCELLANEOUS FRESH'
    ]

    # Product groups to DROP across both pre-2021 and 2021+
    drop_product_group = [
        'PET FOOD', 
        'BABY FOOD', 
        'GUM', 
        'nan', 
        'ICE', 
        'TEA', 
        'SPICES, SEASONING, EXTRACTS', #pre 2021
        'EXTRACTS, HERBS, SPICES AND SEASONINGS', #2021+
        'SHORTENING, OIL',
        'YEAST',
        'COFFEE']

    # Product modules to DROP across both pre-2021 and 2021+
    drop_product_module = [
        'ACNE REMEDIES',
        'ADHESIVE BANDAGES',
        'ADHESIVE NOTE PADS',
        'ADULT INCONTINENCE',
        'AIR CONDITIONER APPLIANCE',
        'AIR PURIFIER AND CLEANER APPLIANCE',
        'AIR SPECIALTY FRESHENERS REMAINING',
        'AIR SPECIALTY FRESHENERS SOLID',
        'AIR SPECIALTY FRSHN ARSL SPRY PMP',
        'ALCOHOLIC COCKTAILS',
        'ALE',
        'ALUMINUM FOIL',
        'ANALGESIC AND CHEST RUBS',
        'ARTIST AND HOBBY PAINT AND SUPPLY',
        'AUTOMATIC DISHWASHER COMPOUNDS',
        'BABY CARE PRODUCTS BATH',
        'BABY CARE PRODUCTS POWDER',
        'BABY FOOD JUNIOR',
        'BABY FOOD STRAINED',
        'BABY JUICE',
        'BABY PACFR TEETHR BOTTLE NPL BRSH',
        'BAGS FOOD STORAGE',
        'BAGS OVEN',
        'BAGS TALL KITCHEN',
        'BAGS TRASH TRASH COMPACTOR',
        'BAGS WASTE',
        'BATH ADDITIVES DRY',
        'BATH ADDITIVES LIQUID',
        'BATHROOM ACCESSORY',
        'BATHROOM SCALE',
        'BATTERIES',
        'BEER',
        'BEVERAGE STORAGE CONTAINER',
        'BLANK COMPACT DISC AND DVD',
        'BLEACH DRY',
        'BLEACH LIQUID GEL',
        'BLENDER APPLIANCE',
        'BLOOD PRESSURE KIT AND ACCESSORY',
        'BLOOD URINE STOOL TEST PRODUCTS',
        'BODY MASSAGER APPLIANCE ACCESSORY',
        'BOURBON STRAIGHT BONDED',
        'BREATH FRESHENERS',
        'BREATH SWEETENERS',
        'BROOMS MOPS AND WAX APPLICATORS',
        'BRUSHES AUTOMOTIVE',
        'BRUSHES MISCELLANEOUS', 
        'BURNER AND RANGE APPLIANCE', 
        'CAMERAS', 
        'CANDLE AND CANDLE IN HOLDER', 
        'CANDLE HOLDER AND ACCESSORY', 
        'CHARCOAL WOOD LIGHTERS', 
        'CHILDREN\'S COLOGNE AND GIFT SETS', 
        'CIDER', 
        'CIGARETTE AND CIGAR PAPER', 
        'CIGARETTES', 
        'CIGARS', 
        'CLEANERS DISINFECTANTS', 
        'CLEANERS METAL', 
        'CLEANERS NONDISINFECTANT', 
        'CLEANERS SEPTIC TANK', 
        'CLEANERS WINDOW', 
        'CLOTH POLISHING CLEANING', 
        'COCKTAIL MIXES DRY', 
        'COCKTAIL MIXES LIQUID', 
        'COCKTAIL MIXES-DRY', 
        'COCKTAIL MIXES-LIQUID', 
        'COCKTAIL PRODUCTS-BITTERS & HEADS', 
        'COFFEE AND TEA MAKER APPLIANCE', 
        'COFFEE LIQUID', 
        'COFFEE SOLUBLE', 
        'COFFEE SOLUBLE FLAVORED', 
        'COFFEE SUBSTITUTES', 
        'COLOGNE AND PERFUME WOMEN\'S',
        'COMPUTER SOFTWARE', 'CONTACT LENS SOLUTION', 'COOKER STEAMER DEHYDRATOR APPLIANCE', 'COOKING BAGS W SEASONING', 'COOKING BAGS W/SEASONING', 'COOKING SPRAYS', 'COOKING WINE & SHERRY', 'COOKING WINE AND SHERRY', 'COOKWARE PRODUCT', 'COOLERS REMAINING', 'CORDIALS AND PROPRIETARY LIQUEURS', 'CORRECTION FLUID AND ERASERS', 'COSMETIC NAIL GROOMING ACCESSORY', 'COSMETICS APPLICATOR BRUSHES', 'COSMETICS BLUSHERS', 'COSMETICS CONCEALERS', 'COSMETICS EYE SHADOWS', 'COSMETICS EYEBROW AND EYE LINER', 'COSMETICS FACE POWDER', 'COSMETICS FOUNDATION CREAM POWDER', 'COSMETICS FOUNDATION LIQUID', 'COSMETICS LIPSTICKS', 'COSMETICS MASCARA', 'COSMETICS NAIL POLISH', 'COSMETICS NONCOTTON APLCT PFS ETC', 'COSMETICS REMAINING', 'COTTON SWABS BALLS ROLLS APLCT ETC', 'CRAYONS', 'DENTAL ACCESSORIES', 'DENTAL FLOSS', 'DENTURE CLEANSERS', 'DEODORANTS COLOGNE TYPE', 'DEODORANTS PERSONAL', 'DESCRIPTION BASE CODE', 'DETERGENTS HEAVY DUTY LIQUID', 'DETERGENTS LIGHT DUTY', 'DETERGENTS PACKAGED', 'DIARRHEA REMEDIES', 'DIETING AIDS APPETITE SUPPRESSANT', 'DIETING AIDS COMPLETE NUTRITIONAL', 'DISK DISKETTE AND DATA CARTRIDGE', 'DISPOSABLE CUPS', 'DISPOSABLE DIAPERS', 'DISPOSABLE DISHES', 'DIVIDERS TABS LABELS AND TAGS', 'DOG AND CAT TREATS', 'DOG FOOD DRY TYPE', 'DOG FOOD WET TYPE', 'DRAIN PIPE OPENERS', 'DRINKWARE CONTAINER AND SET', 'DRY ERASE BULLETIN BOARD ACCESORY', 'EAR DROPS', 'ECOMM PET FOOD AND CARE', 'ELECTRONIC CIGARETTES SMOKING', 'ENGINE TREATMENT AND ADDITIVE', 'FABRIC SOFTENERS DRY', 'FABRIC SOFTENERS LIQUID', 'FACE CLEANSERS AND CREAMS AND LTN', 'FALSE EYELASH AND ACCESSORY', 'FALSE NAIL AND NAIL DECORATION', 'FAN AND CEILING FAN APPLIANCE', 'FEMININE HYGIENE MISCELLANEOUS', 'FILM', 'FIREPLACE LOGS', 'FIRST AID ICE AND HEAT PACK', 'FIRST AID THERMOMETERS', 'FIRST AID TREATMENTS', 'FISH AND REPTILE SUPPLY', 'FLASHLIGHTS', 'FLOOR CARE CLEANERS', 'FOOD PROCESSOR GRINDER APPLIANCE', 'FOOD STORAGE CONTAINERS', 'FOOT COMFORTS PRODUCTS', 'FOOT PREPARATIONS ATHLETE\'S FOOT', 'FOOT PREPARATIONS REMAINING', 'FRYER SKILLET WOK APPLIANCE', 'FURNITURE POLISH', 'GARMENT STEAMER AND IRON APPLIANCE', 'GERMICIDAL ANTISEPTICS', 'GIFT PACKAGE WITH CANDY OR GUM', 'GLOVES', 'GLUE', 'GUM BUBBLE', 'GUM BUBBLE SUGARFREE', 'GUM CHEWING', 'GUM CHEWING SUGARFREE', 'HAIR CARE AND FASHION ACCESSORY', 'HAIR COLORING WOMEN\'S', 'HAIR PREPARATIONS MEN\'S', 
        'HAIR PREPARATIONS OTHER THAN MEN\'S', 'HAIR STYLING APPLIANCE ACCESSORY', 'HAND CLEANERS AND HAND SANITIZERS', 'HAND CREAM', 'HOME CANNING SUPPLY', 'HOUSEHOLD SCISSOR', 'HOUSEHOLD SMART PRODUCT', 'HOUSEHOLD SPECIALTY APPLIANCE', 'HUMIDIFIER AND VAPORIZER APPLIANCE', 'ICE', 'KITCHEN ACCESSORY PRODUCT', 'KITCHEN CUTLERY AND FLATWARE', 'KITCHEN UTENSIL AND GADGET', 'LABEL MAKER TAPE', 'LAMPS INCANDESCENT', 'LAUNDRY AND IRONING ACCESSORIES', 'LAWN AND SOIL FERTILIZER TREATMENT', 'LAXATIVES', 'LIGHTERS', 'LIP REMEDIES REMAINING', 'LIP REMEDIES SOLID', 'LT IM PRODUCT CLASS VALUE', 'MANICURING NEEDS', 'MARKERS', 'MEASURE MIXING UTENSIL CONTAINER', 'MEDICAL ACCESSORY REMAINING', 'MEDICAL WRAP AND BRACE', 'MEDICATED PRODUCTS', 'MEN\'S SETS', 'MOTH PREVENTATIVES', 'MOTOR OIL FLUID AND LUBE', 'MOTORIZED VEHICLE CLEANER PRTCT', 'MUSICAL INSTRUMENTS AND ACCESSORIES', 'NASAL PRODUCT INTERNAL', 'NUTRITIONAL SUPPLEMENTS', 'ORAL CARE COMBINATIONS TRTMN PRGRM', 'ORAL HYGIENE APPLIANCE ACCESSORY', 'ORAL HYGIENE BRUSHES', 'ORAL HYGIENE TRAVEL PACKS', 'ORAL RINSE AND ANTISEPTIC', 'OVEN CLEANERS', 'PAIN REMEDIES HEADACHE', 'PAPER NAPKINS', 'PAPER TOWELS', 'PET ACCESSORY', 'PET APPAREL', 'PET CARE DOMESTIC BIRD FOOD', 'PET CARE PET FOOD', 'PET CARE RAWHIDE AND CHEW PRODUCTS', 'PET CARE WILD BIRD FOOD', 'PET COLLARS AND LEASHES', 'PET HEALTH AND GROOMING', 'PET SUPPLY REMAINING', 'PET TOY', 'PET TREATMENTS EXTERNAL', 'PET TREATMENTS INTERNAL', 'PETROLEUM JELLY', 'PRINTERS', 'RAZOR TRIMMER APPLIANCE ACCESSORY', 'RAZORS NON DISPOSABLE', 'REFERENCE CARD APPAREL', 'REMAINING WHISKEY', 'REPORT COVERS AND SHEET PROTECTORS', 'RETORT POUCH BAGS', 'RUG AND ROOM DEODORIZERS', 'RUG CLEANERS', 'SALT - TABLE', 'SCENT HOLDER', 'SCHOOL AND OFFICE BASICS', 'SCHOOL AND OFFICE FASTENER PUNCH', 'SCHOOL AND OFFICE PAPER AND FORMS', 'SCHOOL AND OFFICE STORAGE DISPENSER', 'SCOURING PADS', 'SCRAPPLE & MUSH', 'SCRAPPLE AND MUSH', 'SHAMPOO AEROSOL LIQUID LOTION PWDR', 'SHORTENING', 'SKIN CREAM ALL PURPOSE', 'SMOKING ACCESSORY', 'SOAP BAR', 'SOAP LIQUID', 'SOAP SPECIALTY', 'SODA STRAWS', 'SPONGES AND SQUEEGEES HOUSEHOLD', 'SPONGES PERSONAL', 'SPORTS AND NOVELTY CARDS', 'SPOT AND STAIN REMOVERS', 'STARCH AEROSOL AND SPRAY', 'SUNTAN PREPARATIONS LTNS OLS ETC', 'SUNTAN PREPARATIONS SNSCR SNBLC', 'TAPE MISCELLANEOUS', 'TEA BAGS', 'TEA HERBAL BAGS', 'TEA HERBAL INSTANT', 'TEA HERBAL PACKAGED', 'TEA INSTANT', 'TEA LIQUID', 'TEA MIXES', 'TEA PACKAGED', 'TELEPHONE AND ACCESSORY', 'TEQUILA', 'TOASTER AND TOASTER OVEN APPLIANCE', 'TOBACCO CHEWING', 'TOBACCO SMOKING', 'TOILET BOWL CLEANERS', 'TOILET TISSUE', 'TOOTH CLEANERS', 'UNCLASSIFIED COUGH AND COLD REMIDIES', 'UNCLASSIFIED DETERGENTS', 'UNCLASSIFIED DIET AIDS', 'UNCLASSIFIED FRESHENERS AND DEODORIZERS', 'UNCLASSIFIED GROOMING AIDS', 'UNCLASSIFIED HARDWARE,TOOLS', 'UNCLASSIFIED MENS TOILETRIES', 'UNCLASSIFIED PAPER PRODUCTS', 'UNCLASSIFIED TOBACCO & ACCESSORIES', 'UNCLASSIFIED TOYS & SPORTING GOODS', 'UPHOLSTERY CLEANERS', 'VACUUM AND CARPET CLEANER APPLIANCE', 'VARNISH AND SHELLAC', 'VINEGAR', 'VITAMINS MULTIPLE', 'VITAMINS REMAINING', 'VITAMINS TONICS LIQUID AND POWDER', 'VODKA', 'WATER BOTTLED', 'WATER-BOTTLED', 'WINE DOMESTIC DRY TABLE', 'WINE FLAVORED REFRESHMENT', 'WINE IMPORTED DRY TABLE', 'WINE NON ALCOHOLIC', 'WINE SAKE', 'WINE SPARKLING', 
        'WOMEN\'S GIFT SETS SKIN CARE PCKGS',
        'NUTRITIONAL SUPPLEMENTS',
        'PROTEIN SUPPLEMENTS',
        'DIETING AIDS COMPLETE NUTRITIONAL',
        'COOKING SPRAYS',
        'BAKING SODA',
        'BAKING POWDER',
        'COOKING WINE & SHERRY',
        'FOOD COLORING',
        'FRUIT PECTINS',
        'CONFECTIONERY PASTE',
        'SALT SUBSTITUTES',
        'GELATIN - DIET - MIX',
        'YEAST-REFRIGERATED',
        'UNCLASSIFIED PAPER PRODUCTS',
        'UNCLASSIFIED HARDWARE,TOOLS',
        'UNCLASSIFIED GROOMING AIDS',
        'UNCLASSIFIED MENS TOILETRIES',
        'UNCLASSIFIED COUGH AND COLD REMIDIES',
        'UNCLASSIFIED DIET AIDS',
        'UNCLASSIFIED TOBACCO & ACCESSORIES',
        'UNCLASSIFIED DETERGENTS',
        'SALT - COOKING/EDIBLE/SEASONED',
        'SALT TABLE',
        'SANITARY NAPKINS',
        'REPORTED UNCLASSIFIABLE UPCS',
        'EXTRACTS',
        'YEAST - DRY',
        'UNCLASSIFIED COOKWARE',
        'UNCLASSIFIED STATIONARY, SCHOOL SUPPLIES',
        'UNCLASSIFIED COSMETICS',
        'CAT FOOD - MOIST TYPE',
        'UNCLASSIFIED PHOTOGRAPHIC SUPPLIES',
        'UNCLASSIFIED LAUNDRY SUPPLIES',
        'UNCLASSIFIED INSECTICDS/PESTICDS/RODENTICDS',
        'UNCLASSIFIED GLASSWARE, TABLEW',
        'UNCLASSIFIED KITCHEN GADGETS',
        'UNCLASSIFIED BABY NEEDS',
        'UNCLASSIFIED HAIR CARE',
        'UNCLASSIFIED HOUSEWARES, APPLIANCES',
        'UNCLASSIFIED ORAL HYGIENE',
        'DETERGENTS LIGHT DUTY',
        'UNCLASSIFIED PET CARE',
        'UNCLASSIFIED AUTOMOTIVE',
        'UNCLASSIFIED SANITARY PROTECTION',
        'UNCLASSIFIED SHAVING NEEDS',
        'UNCLASSIFIED FEMININE HYGIENE',
        'UNCLASSIFIED PERSONAL SOAP AND BATH ADDITIV',
        'TOILET TISSUE',
        # 'MAGNET DATA',
        # 'REFERENCE CARD VEGETABLES',
        # 'REFERENCE CARD FRUITS',
        # 'REFERENCE CARD MEAT',
        'REFERENCE CARD TAKE OUT',
        # 'REFERENCE CARD PREPARED FOODS',
        # 'REFERENCE CARD POULTRY',
        # 'REFERENCE CARD BAKED GOODS - ALL OTHER',
        'REFERENCE CARD GAS',
        # 'REFERENCE CARD COLD CUTS - CLERK SERVED',
        'REFERENCE CARD COFFEE',
        'REFERENCE CARD FOUNTAIN BEVERAGE',
        # 'REFERENCE CARD BAKED GOODS ALL OTHR',
        # 'REFERENCE CARD SEAFOOD',
        'REFERENCE CARD APPAREL',
        # 'REFERENCE CARD CANDY/NUTS/SEEDS',
        # 'REFERENCE CARD CHEESE - CLERK SERVED',
        'PET CARE - WILD BIRD FOOD',
        'REFERENCE CARD RX',
        # 'REFERENCE CARD COLD CUTS CLERK SRVD',
        # 'REFERENCE CARD CHEESE - SELF SERVED',
        # 'REFERENCE CARD COLD CUTS - SELF SERVED',
        'PET CARE - PET FOOD',
        # 'REFERENCE CARD BAKED GOODS - COOKIES',
        # 'REFERENCE CARD CANDY NUTS SEEDS',
        # 'REFERENCE CARD BAKED GOODS - CAKES',
        # 'REFERENCE CARD CHEESE CLERK SERVED',
        'REFERENCE CARD FLORAL',
        'PET CARE - DOMESTIC BIRD FOOD',
        # 'REFERENCE CARD COLD CUTS SELF SRVD',
        # 'REFERENCE CARD CHEESE SELF SERVED',
        # 'REFERENCE CARD BAKED GOODS - PIES',
        # 'REFERENCE CARD BAKED GOODS COOKIES',
        'DOG FOOD - MOIST TYPE',
        # 'REFERENCE CARD BAKED GOODS CAKES',
        'PREPAID GIFT CARDS',
        'REFERENCE CARD',
        'REFERENCE CARD DVD VIDEO',
        # 'REFERENCE CARD BAKED GOODS PIES',
        'UNCLASSIFIED HOUSEHOLD CLEANERS',
        'REFERENCE CARD MEAL KIT',
        'UNCLASSIFIED HOUSEHOLD SUPPLIES',
        'REFERENCE CARD PHOTO',
        'UNCLASSIFIED FLORAL GARDENING',
        'BAKING CUPS AND LINERS',
        'UNCLASSIFIED MEDICATIONS/REMEDIES/HEALTH AI']

    # Whether to show detailed tarball structure (useful for first run)
    explore_structure = False

    # ========================================================================

    # Split years into pre-2021 (use master file) and 2021+ (use annual files)
    years_pre_2021 = [y for y in years_to_process if y < 2021]
    years_2021_plus = [y for y in years_to_process if y >= 2021]

    print(f"\nProcessing {len(years_to_process)} year(s): {years_to_process}")
    print(f"  Pre-2021 years (using master file): {years_pre_2021}")
    print(f"  2021+ years (using annual files): {years_2021_plus}")
    print(f"\nDropping department codes (pre-2021):")
    print("  0 = HEALTH & BEAUTY CARE")
    print("  7 = NON-FOOD GROCERY")
    print("  8 = ALCOHOLIC BEVERAGES")
    print("  9 = GENERAL MERCHANDISE")
    print(f"\nDropping departments (2021+): {drop_departments_2021_plus}")

    # Load CPI data for price deflation
    print("\n" + "=" * 80)
    print("LOADING CPI DATA FOR PRICE DEFLATION")
    print("=" * 80)
    cpi_df, cpi_lookup = load_cpi_data()
    target_cpi = get_target_cpi(cpi_df, TARGET_YEAR)

    # Load master products file for pre-2021 years
    products_df_filtered_master = None
    if years_pre_2021:
        print("\n" + "=" * 80)
        print("LOADING MASTER PRODUCTS FILE (for pre-2021 years)")
        print("=" * 80)

        products_df = load_products_master(master_products_path)

        if products_df is None:
            print("ERROR: Could not load master products file. Exiting.")
            return

        # Filter products by department code
        products_df_filtered_master = filter_products_by_department(products_df, drop_department_desc_pre_2021, drop_product_group, drop_product_module)

        if products_df_filtered_master is None:
            print("ERROR: Could not filter products. Exiting.")
            return

        print(f"\n✓ Master products loaded and filtered: {len(products_df_filtered_master):,} food products ready")

    # Output directory for partitioned parquet dataset
    output_dir = f'/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_food'
    
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nOutput directory: {output_dir}")
    print("Each year will be saved as a separate partition")

    # Process each year
    total_rows_written = 0
    years_processed = 0
    years_succeeded = []

    # Initialize summary statistics tracker
    summary_stats = initialize_summary_stats()

    for year in years_to_process:
        # For 2021+, load and filter products from annual files
        if year >= 2021:
            print("\n" + "=" * 80)
            print(f"LOADING PRODUCTS FOR {year} (from annual files)")
            print("=" * 80)

            tarball_path = f'{base_path}/Consumer_Panel_Data_{year}.tgz'
            products_df_year = load_products_2021_plus(tarball_path, year)

            if products_df_year is None:
                print(f"ERROR: Could not load products for {year}. Skipping.")
                continue

            products_df_filtered = filter_products_2021_plus(products_df_year, drop_departments_2021_plus, drop_product_group, drop_product_module)

            if products_df_filtered is None:
                print(f"ERROR: Could not filter products for {year}. Skipping.")
                continue
        else:
            products_df_filtered = products_df_filtered_master

        result = process_year(base_path, year, products_df_filtered, explore_structure)

        if result is not None:
            # Ensure panel_year column exists for partitioning
            if 'panel_year' not in result.columns:
                print(f"Warning: panel_year not found, adding it manually as {year}")
                result['panel_year'] = year

            # Subtract coupon value from total_price_paid to get net price paid by panelist
            if 'coupon_value' in result.columns and 'total_price_paid' in result.columns:
                result['total_price_paid'] = result['total_price_paid'] - result['coupon_value'].fillna(0)
                print(f"\nSubtracted coupon_value from total_price_paid")

            # Deflate prices before saving
            print(f"\nDeflating prices for year {year}...")
            result = deflate_prices(result, cpi_lookup, target_cpi, year)

            # Drop coupon_value column
            if 'coupon_value' in result.columns:
                result = result.drop(columns=['coupon_value'])

            # Add normalized product module column for cross-year consistency
            print(f"\nNormalizing product module names for year {year}...")
            result = add_normalized_module_column(result)

            # Fix mixed-type columns that cause PyArrow errors
            # Convert object-dtype columns to string to handle mixed types
            for col in result.columns:
                if result[col].dtype == 'object':
                    result[col] = result[col].astype(str)

            # Delete existing partition if it exists (to avoid appending duplicates)
            partition_path = os.path.join(output_dir, f'panel_year={year}')
            if os.path.exists(partition_path):
                print(f"Removing existing partition: {partition_path}")
                shutil.rmtree(partition_path)

            # Update summary statistics before writing (while data is in memory)
            summary_stats = update_summary_stats(summary_stats, result, year)

            # Write this year's data to a partition
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
        print(f"Prices deflated to {TARGET_YEAR} dollars using CPI for Food and Beverages")
        print(f"Deflated columns: {COLUMNS_TO_DEFLATE}")

        # Save summary statistics
        save_summary_stats(summary_stats, output_dir)

        print("\nTo read the full dataset:")
        print(f"  df = pd.read_parquet('{output_dir}')")
        print("\nTo read specific years:")
        print(f"  df = pd.read_parquet('{output_dir}', filters=[('panel_year', 'in', [2020, 2021])])")
    else:
        print("\n\nERROR: No data was successfully processed")

if __name__ == "__main__":
    main()
