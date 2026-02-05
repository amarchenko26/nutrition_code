"""
Plot trends in corn-derived food consumption over time using Nielsen Consumer Panel data.

Processes each year's parquet file individually to avoid loading entire dataset into memory.
"""

import pandas as pd
import matplotlib.pyplot as plt
import os
import re
import tarfile
import hashlib
import json
from glob import glob


# ============================================================================
# CONFIGURATION
# ============================================================================
# Set to True to load from cache where available (faster)
# Set to False to always recalculate from raw data (ignores all caches)
USE_CACHE = True

# Presentation-friendly font sizes
plt.rcParams.update({
    'axes.titlesize': 18,
    'axes.labelsize': 14,
    'legend.fontsize': 12,
    'legend.title_fontsize': 12,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
})

# ============================================================================
# CACHE PATHS
# ============================================================================
# Cache files go in the data directory, not the code directory
CACHE_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_with_corn_classification/cache'
YEARLY_TRENDS_CACHE = os.path.join(CACHE_DIR, 'yearly_trends_cache.csv')
HFCS_TRENDS_CACHE = os.path.join(CACHE_DIR, 'hfcs_trends_cache.csv')
DEMOGRAPHIC_TRENDS_CACHE = os.path.join(CACHE_DIR, 'demographic_trends_cache.csv')
PANELISTS_CACHE_DIR = os.path.join(CACHE_DIR, 'panelists_cache')
DEMOGRAPHIC_CACHE_DIR = os.path.join(CACHE_DIR, 'demographic_trends_cache')
MODULE_TRENDS_CACHE = os.path.join(CACHE_DIR, 'module_trends_cache.csv')
BALANCED_PANEL_UPCS_CACHE = os.path.join(CACHE_DIR, 'balanced_panel_upcs.parquet')
BALANCED_PANEL_TRENDS_CACHE = os.path.join(CACHE_DIR, 'balanced_panel_trends.csv')
EXPENDITURE_TRENDS_CACHE = os.path.join(CACHE_DIR, 'expenditure_trends_cache.csv')
WEIGHT_TRENDS_CACHE = os.path.join(CACHE_DIR, 'weight_trends_cache.csv')
HH_SPENDING_TRENDS_CACHE = os.path.join(CACHE_DIR, 'hh_spending_trends_cache.csv')

# ============================================================================
# DATA PATHS
# ============================================================================
NIELSEN_RAW_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/consumer'
PURCHASES_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_with_corn_classification'
PURCHASES_DEFLATED_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_deflated'
CPI_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/price_deflator/CPIEBEV.csv'
TARGET_YEAR = 2013  # Target year for deflation


# HFCS variations to exclude
HFCS_PATTERNS = [
    'high fructose corn syrup',
    'high-fructose corn syrup',
    'hfcs',
    'corn syrup high fructose',
]

# Nielsen income code to bracket mapping
# Codes: 3, 4, 6, 8, 10, 11, 13, 15, 16, 17, 18, 19, 21, 23, 26, 27
INCOME_CODE_TO_GROUP = {
    3: 'Under $25k',
    4: 'Under $25k',
    6: 'Under $25k',
    8: 'Under $25k',
    10: 'Under $25k',
    11: 'Under $25k',
    13: 'Under $25k',
    15: '$25k-$50k',
    16: '$25k-$50k',
    17: '$25k-$50k',
    18: '$25k-$50k',
    19: '$25k-$50k',
    21: '$50k-$100k',
    23: '$50k-$100k',
    26: '$50k-$100k',
    27: '$100k+',
}


def map_income_to_group(income_code):
    """Map Nielsen income code to broader income group."""
    return INCOME_CODE_TO_GROUP.get(income_code, None)


def get_first_ingredient(ingredients_str):
    """Extract the first ingredient from a comma-separated ingredients string."""
    if pd.isna(ingredients_str) or not ingredients_str:
        return None
    # Split by comma and get first
    first = ingredients_str.split(',')[0].strip().lower()
    # Normalize spaces
    first = re.sub(r'\s+', ' ', first)
    return first


def is_hfcs(ingredient):
    """Check if an ingredient is high fructose corn syrup."""
    if not ingredient:
        return False
    ingredient_lower = ingredient.lower()
    for pattern in HFCS_PATTERNS:
        if pattern in ingredient_lower:
            return True
    return False


def _normalize_column_list(columns, lowercase=True):
    """Normalize column input to a list, optionally lowercasing names."""
    if columns is None:
        return []
    if isinstance(columns, str):
        columns = [columns]
    else:
        columns = list(columns)
    if lowercase:
        columns = [c.lower() for c in columns]
    return columns


def _panelists_cache_path(year, panelists_cols):
    os.makedirs(PANELISTS_CACHE_DIR, exist_ok=True)
    cols_key = '|'.join(sorted(panelists_cols))
    key = hashlib.md5(cols_key.encode('utf-8')).hexdigest()[:10]
    return os.path.join(PANELISTS_CACHE_DIR, f'panelists_{year}_{key}.parquet')


def _demographic_cache_paths(demographic_vars, panelists_cols, years_to_process):
    os.makedirs(DEMOGRAPHIC_CACHE_DIR, exist_ok=True)
    years_key = 'all' if years_to_process is None else ','.join(map(str, sorted(years_to_process)))
    key_payload = {
        'demographic_vars': sorted(demographic_vars),
        'panelists_cols': sorted(panelists_cols),
        'years': years_key,
        'purchases_path': PURCHASES_PATH,
    }
    key = hashlib.md5(json.dumps(key_payload, sort_keys=True).encode('utf-8')).hexdigest()[:12]
    cache_path = os.path.join(DEMOGRAPHIC_CACHE_DIR, f'demographic_trends_{key}.csv')
    meta_path = os.path.join(DEMOGRAPHIC_CACHE_DIR, f'demographic_trends_{key}.json')
    return cache_path, meta_path, key_payload


def _standardize_panelists_columns(panelists_df):
    panelists_df.columns = panelists_df.columns.str.lower()
    if 'household_code' not in panelists_df.columns and 'household_cd' in panelists_df.columns:
        panelists_df = panelists_df.rename(columns={'household_cd': 'household_code'})
    return panelists_df


def load_panelists_for_year(year, panelists_cols=None, use_cache=None):
    """
    Load panelists (household demographics) data for a given year from tarball.

    Parameters:
    -----------
    year : int
        Year to load panelists for

    Returns:
    --------
    DataFrame with household_code and demographic columns, or None if not found
    """
    if use_cache is None:
        use_cache = USE_CACHE

    # Check cache first
    panelists_cols_normalized = _normalize_column_list(panelists_cols)
    if use_cache and panelists_cols_normalized:
        cache_path = _panelists_cache_path(year, panelists_cols_normalized)
        if os.path.exists(cache_path):
            print(f"  Loading panelists from cache: {cache_path}")
            return pd.read_parquet(cache_path)

    # Path to tarball: {NIELSEN_RAW_PATH}/Consumer_Panel_Data_{year}.tgz
    tarball_path = os.path.join(NIELSEN_RAW_PATH, f'Consumer_Panel_Data_{year}.tgz')

    if not os.path.exists(tarball_path):
        print(f"WARNING: Tarball not found: {tarball_path}")
        return None

    with tarfile.open(tarball_path, 'r:gz') as tar:
        # Find the panelists file
        panelists_file = None
        for member in tar.getmembers():
            if 'panelists' in member.name.lower() and member.name.endswith('.tsv'):
                panelists_file = member
                break

        if panelists_file is None:
            print(f"WARNING: No panelists file found in {tarball_path}")
            return None

        # Extract and read the panelists file
        f = tar.extractfile(panelists_file)
        if f is None:
            print(f"WARNING: Could not extract panelists file")
            return None

        panelists_df = pd.read_csv(f, sep='\t', low_memory=False)

    panelists_df = _standardize_panelists_columns(panelists_df)

    panelists_cols = _normalize_column_list(panelists_cols)
    if panelists_cols:
        if 'household_code' not in panelists_cols:
            panelists_cols = ['household_code'] + panelists_cols
        available_cols = [c for c in panelists_cols if c in panelists_df.columns]
        missing_cols = [c for c in panelists_cols if c not in panelists_df.columns]
        if missing_cols:
            print(f"  WARNING: Panelists missing columns: {missing_cols}")
        panelists_df = panelists_df[available_cols]

    if use_cache is not None and panelists_cols:
        cache_path = _panelists_cache_path(year, panelists_cols)
        panelists_df.to_parquet(cache_path, index=False)

    return panelists_df


def compute_trends_by_demographic(demographic_vars='household_income', panelists_cols=None,
                                  years_to_process=None, use_cache=None):
    """
    Compute yearly corn trends broken down by demographic variable(s).

    This function loads panelists data on-the-fly from Nielsen tarballs and merges
    with the processed purchase data to compute trends by demographic group.

    Parameters:
    -----------
    demographic_vars : str or list of str
        Column name(s) of the demographic variable(s) to group by (e.g., 'household_income',
        'race', 'hispanic_origin', 'household_size', 'region_code'). Case-insensitive.
    panelists_cols : str or list of str, optional
        Additional columns to load from panelists data. If None, uses demographic_vars.
    years_to_process : list, optional
        List of years to include. If None, processes all available years.
    use_cache : bool, optional
        Whether to use/save cached results. If None, uses global USE_CACHE setting.

    Returns:
    --------
    DataFrame with year, demographic group(s), and corn trend percentages
    """
    if use_cache is None:
        use_cache = USE_CACHE

    demographic_vars = _normalize_column_list(demographic_vars)
    if panelists_cols is None:
        panelists_cols = demographic_vars
    panelists_cols = _normalize_column_list(panelists_cols)
    if 'household_code' not in panelists_cols:
        panelists_cols = ['household_code'] + panelists_cols

    print(f"\n{'='*80}")
    print(f"COMPUTING CORN TRENDS BY {', '.join(demographic_vars).upper()}")
    print("="*80)

    cache_path, meta_path, cache_payload = _demographic_cache_paths(
        demographic_vars, panelists_cols, years_to_process
    )
    if use_cache and os.path.exists(cache_path):
        print(f"Loading demographic trends from cache: {cache_path}")
        return pd.read_csv(cache_path)

    corn_vars = [
        'first_ing_is_corn_literal',
        'first_ing_is_corn_usual_or_literal',
        'any_ing_is_corn_literal',
        'any_ing_is_corn_usual_or_literal',
        'any_ing_is_corn_any',
    ]

    # Find all year partitions
    year_dirs = sorted(glob(os.path.join(PURCHASES_PATH, 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {PURCHASES_PATH}")
        return None

    print(f"Found {len(year_dirs)} year partitions")

    results = []

    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        if years_to_process is not None and year not in years_to_process:
            continue

        print(f"\nProcessing year {year}...")

        # Load panelists for this year
        panelists_cache_path = _panelists_cache_path(year, panelists_cols)
        if use_cache and os.path.exists(panelists_cache_path):
            panelists_df = pd.read_parquet(panelists_cache_path)
        else:
            panelists_df = load_panelists_for_year(year, panelists_cols=panelists_cols, use_cache=use_cache)
        if panelists_df is None:
            print(f"  Skipping year {year} - no panelists data")
            continue

        # Check if demographic variables exist
        missing_demo = [c for c in demographic_vars if c not in panelists_df.columns]
        if missing_demo:
            print(f"  WARNING: Missing demographic columns: {missing_demo}")
            print(f"  Available columns: {panelists_df.columns.tolist()[:20]}...")
            continue

        # Keep only needed columns from panelists
        panelists_subset = panelists_df[panelists_cols].copy()

        # Load purchases for this year (including quantity)
        try:
            df_year = pd.read_parquet(year_dir, columns=['household_code', 'quantity'] + corn_vars)
        except Exception as e:
            print(f"  Error loading purchases: {e}")
            continue

        df_year['quantity'] = df_year['quantity'].fillna(1)

        print(f"  Loaded {len(df_year):,} purchases, {len(panelists_subset):,} households")

        # Merge purchases with panelists
        df_merged = df_year.merge(panelists_subset, on='household_code', how='left')

        n_matched = df_merged[demographic_vars].notna().all(axis=1).sum()
        print(f"  Matched {n_matched:,} purchases ({n_matched/len(df_merged)*100:.1f}%)")

        # Compute quantity-weighted means by demographic group
        def weighted_agg(group):
            total_qty = group['quantity'].sum()
            result = {'n_purchases': len(group), 'n_units': total_qty}
            for var in corn_vars:
                result[var] = (group[var] * group['quantity']).sum() / total_qty * 100
            return pd.Series(result)

        grouped = df_merged.groupby(demographic_vars).apply(weighted_agg).reset_index()
        grouped['panel_year'] = year

        results.append(grouped)

        # Free memory
        del df_year, df_merged, panelists_df

    if not results:
        print("ERROR: No data processed")
        return None

    # Combine all years
    trends_df = pd.concat(results, ignore_index=True)

    print(f"\n{'='*80}")
    print(f"SUMMARY: Trends by {', '.join(demographic_vars)}")
    print("="*80)
    print(f"Total rows: {len(trends_df):,}")
    print(f"Years: {sorted(trends_df['panel_year'].unique())}")
    if len(demographic_vars) == 1:
        print(f"Demographic groups: {sorted(trends_df[demographic_vars[0]].unique())}")
    else:
        print(f"Demographic groups: {len(trends_df[demographic_vars].drop_duplicates()):,}")

    if use_cache is not None:
        trends_df.to_csv(cache_path, index=False)
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(cache_payload, f, indent=2, sort_keys=True)
        print(f"Saved demographic trends cache to: {cache_path}")

    return trends_df


def compute_trends_by_product_module(years_to_process=None, use_cache=None, min_purchases=10000):
    """
    Compute yearly corn trends broken down by product module (category).

    Parameters:
    -----------
    years_to_process : list, optional
        List of years to include. If None, processes all available years.
    use_cache : bool, optional
        Whether to use/save cached results. If None, uses global USE_CACHE setting.
    min_purchases : int
        Minimum purchases per module per year to include (filters noisy estimates).

    Returns:
    --------
    DataFrame with year, product_module_descr, and corn trend percentages
    """
    if use_cache is None:
        use_cache = USE_CACHE

    print(f"\n{'='*80}")
    print("COMPUTING CORN TRENDS BY PRODUCT MODULE")
    print("="*80)

    # Check cache
    if use_cache and os.path.exists(MODULE_TRENDS_CACHE):
        print(f"Loading module trends from cache: {MODULE_TRENDS_CACHE}")
        return pd.read_csv(MODULE_TRENDS_CACHE)

    corn_vars = [
        'first_ing_is_corn_literal',
        'first_ing_is_corn_usual_or_literal',
        'any_ing_is_corn_literal',
        'any_ing_is_corn_usual_or_literal',
        'any_ing_is_corn_any',
    ]

    # Find all year partitions
    year_dirs = sorted(glob(os.path.join(PURCHASES_PATH, 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {PURCHASES_PATH}")
        return None

    print(f"Found {len(year_dirs)} year partitions")

    results = []

    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        if years_to_process is not None and year not in years_to_process:
            continue

        print(f"Processing year {year}...", end=' ')

        try:
            df_year = pd.read_parquet(year_dir, columns=['product_module_descr', 'quantity'] + corn_vars)
        except Exception as e:
            print(f"Error: {e}")
            continue

        df_year['quantity'] = df_year['quantity'].fillna(1)

        # Group by product module with quantity weighting
        def weighted_agg(group):
            total_qty = group['quantity'].sum()
            result = {'n_purchases': len(group), 'n_units': total_qty}
            for var in corn_vars:
                result[var] = (group[var] * group['quantity']).sum() / total_qty * 100
            return pd.Series(result)

        grouped = df_year.groupby('product_module_descr').apply(weighted_agg).reset_index()

        # Filter by minimum units (not purchases)
        grouped = grouped[grouped['n_units'] >= min_purchases]

        grouped['panel_year'] = year
        results.append(grouped)

        print(f"{len(df_year):,} purchases, {int(df_year['quantity'].sum()):,} units, {len(grouped)} modules")

        del df_year

    if not results:
        print("ERROR: No data processed")
        return None

    trends_df = pd.concat(results, ignore_index=True)

    print(f"\n{'='*80}")
    print("SUMMARY: Trends by Product Module")
    print("="*80)
    print(f"Total rows: {len(trends_df):,}")
    print(f"Years: {sorted(trends_df['panel_year'].unique())}")
    print(f"Unique modules: {trends_df['product_module_descr'].nunique()}")

    # Save to cache
    if use_cache is not None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        trends_df.to_csv(MODULE_TRENDS_CACHE, index=False)
        print(f"Saved module trends cache to: {MODULE_TRENDS_CACHE}")

    return trends_df


def compute_balanced_panel_upcs(start_year=2004, end_year=2020, use_cache=None):
    """
    Find UPCs that appear in every year from start_year to end_year.

    This creates a "balanced panel" of products that existed throughout the entire
    time period, allowing us to distinguish reformulation effects from new product
    introduction effects.

    Parameters:
    -----------
    start_year : int
        First year of the balanced panel
    end_year : int
        Last year of the balanced panel
    use_cache : bool, optional
        Whether to use/save cached results. If None, uses global USE_CACHE setting.

    Returns:
    --------
    set : Set of UPC codes that appear in all years
    """
    if use_cache is None:
        use_cache = USE_CACHE

    print(f"\n{'='*80}")
    print(f"COMPUTING BALANCED PANEL OF UPCs ({start_year}-{end_year})")
    print("="*80)

    # Check cache
    if use_cache and os.path.exists(BALANCED_PANEL_UPCS_CACHE):
        print(f"Loading balanced panel UPCs from cache: {BALANCED_PANEL_UPCS_CACHE}")
        cached_df = pd.read_parquet(BALANCED_PANEL_UPCS_CACHE)
        balanced_upcs = set(cached_df['upc'].tolist())
        print(f"Loaded {len(balanced_upcs):,} UPCs in balanced panel")
        return balanced_upcs

    years = list(range(start_year, end_year + 1))
    print(f"Finding UPCs present in all {len(years)} years...")

    # For each year, get the set of unique UPCs
    upc_sets_by_year = {}

    for year in years:
        year_dir = os.path.join(PURCHASES_PATH, f'panel_year={year}')
        if not os.path.exists(year_dir):
            print(f"  WARNING: Year {year} not found, skipping")
            continue

        print(f"  Loading UPCs for {year}...", end=' ')
        try:
            df_year = pd.read_parquet(year_dir, columns=['upc'])
            # Get unique UPCs for this year
            unique_upcs = set(df_year['upc'].unique())
            upc_sets_by_year[year] = unique_upcs
            print(f"{len(unique_upcs):,} unique UPCs")
            del df_year
        except Exception as e:
            print(f"Error: {e}")
            continue

    if len(upc_sets_by_year) < len(years):
        print(f"WARNING: Only found data for {len(upc_sets_by_year)} of {len(years)} years")

    if not upc_sets_by_year:
        print("ERROR: No data found for any year")
        return set()

    # Find intersection of all years
    print(f"\nFinding UPCs present in ALL {len(upc_sets_by_year)} years...")
    balanced_upcs = None
    for year, upc_set in sorted(upc_sets_by_year.items()):
        if balanced_upcs is None:
            balanced_upcs = upc_set.copy()
        else:
            balanced_upcs = balanced_upcs.intersection(upc_set)
        print(f"  After {year}: {len(balanced_upcs):,} UPCs remain")

    if balanced_upcs is None:
        balanced_upcs = set()

    print(f"\nBalanced panel: {len(balanced_upcs):,} UPCs present in all years")

    # Save to cache
    if use_cache is not None and balanced_upcs:
        os.makedirs(CACHE_DIR, exist_ok=True)
        # Convert to DataFrame for parquet storage
        cache_df = pd.DataFrame({'upc': list(balanced_upcs)})
        cache_df.to_parquet(BALANCED_PANEL_UPCS_CACHE, index=False)
        print(f"Saved balanced panel UPCs to: {BALANCED_PANEL_UPCS_CACHE}")

    return balanced_upcs


def compute_balanced_panel_trends(balanced_upcs=None, start_year=2004, end_year=2020, use_cache=None):
    """
    Compute cornification trends for the balanced panel of UPCs.

    Compares trends for:
    1. All purchases (original analysis)
    2. Balanced panel only (same UPCs in all years)

    This helps distinguish between:
    - New product introduction effects (would show in "all" but not "balanced")
    - Reformulation effects (would show in both)

    Parameters:
    -----------
    balanced_upcs : set, optional
        Set of UPC codes in the balanced panel.
        If None, computes it first.
    start_year : int
        First year of the balanced panel
    end_year : int
        Last year of the balanced panel
    use_cache : bool, optional
        Whether to use/save cached results. If None, uses global USE_CACHE setting.

    Returns:
    --------
    DataFrame with year and cornification rates for all purchases and balanced panel
    """
    if use_cache is None:
        use_cache = USE_CACHE

    print(f"\n{'='*80}")
    print(f"COMPUTING BALANCED PANEL TRENDS ({start_year}-{end_year})")
    print("="*80)

    # Check cache
    if use_cache and os.path.exists(BALANCED_PANEL_TRENDS_CACHE):
        print(f"Loading balanced panel trends from cache: {BALANCED_PANEL_TRENDS_CACHE}")
        return pd.read_csv(BALANCED_PANEL_TRENDS_CACHE)

    # Get balanced panel UPCs if not provided
    if balanced_upcs is None:
        balanced_upcs = compute_balanced_panel_upcs(start_year, end_year, use_cache=use_cache)

    if not balanced_upcs:
        print("ERROR: No balanced panel UPCs found")
        return None

    corn_vars = [
        'first_ing_is_corn_literal',
        'first_ing_is_corn_usual_or_literal',
        'any_ing_is_corn_literal',
        'any_ing_is_corn_usual_or_literal',
        'any_ing_is_corn_any',
    ]

    years = list(range(start_year, end_year + 1))
    results = []

    for year in years:
        year_dir = os.path.join(PURCHASES_PATH, f'panel_year={year}')
        if not os.path.exists(year_dir):
            continue

        print(f"Processing {year}...", end=' ')

        try:
            df_year = pd.read_parquet(year_dir, columns=['upc', 'quantity'] + corn_vars)
        except Exception as e:
            print(f"Error: {e}")
            continue

        df_year['quantity'] = df_year['quantity'].fillna(1)

        n_all = len(df_year)
        n_all_units = df_year['quantity'].sum()
        n_all_upcs = df_year['upc'].nunique()

        # Filter to balanced panel
        df_balanced = df_year[df_year['upc'].isin(balanced_upcs)]

        n_balanced = len(df_balanced)
        n_balanced_units = df_balanced['quantity'].sum() if len(df_balanced) > 0 else 0
        n_balanced_upcs = df_balanced['upc'].nunique()

        # Compute quantity-weighted means for all purchases
        all_means = pd.Series()
        for var in corn_vars:
            all_means[var] = (df_year[var] * df_year['quantity']).sum() / n_all_units * 100

        # Compute quantity-weighted means for balanced panel
        if len(df_balanced) > 0 and n_balanced_units > 0:
            balanced_means = pd.Series()
            for var in corn_vars:
                balanced_means[var] = (df_balanced[var] * df_balanced['quantity']).sum() / n_balanced_units * 100
        else:
            balanced_means = pd.Series({v: None for v in corn_vars})

        year_result = {
            'panel_year': year,
            'n_purchases_all': n_all,
            'n_units_all': int(n_all_units),
            'n_purchases_balanced': n_balanced,
            'n_units_balanced': int(n_balanced_units),
            'n_upcs_all': n_all_upcs,
            'n_upcs_balanced': n_balanced_upcs,
            'pct_units_in_balanced': n_balanced_units / n_all_units * 100 if n_all_units > 0 else 0,
            'pct_upcs_in_balanced': n_balanced_upcs / n_all_upcs * 100 if n_all_upcs > 0 else 0,
        }

        # Add corn variables for both panels
        for var in corn_vars:
            year_result[f'{var}_all'] = all_means[var]
            year_result[f'{var}_balanced'] = balanced_means[var]

        results.append(year_result)

        print(f"All: {int(n_all_units):,} units ({n_all_upcs:,} UPCs), "
              f"Balanced: {int(n_balanced_units):,} ({n_balanced_units/n_all_units*100:.1f}%) units ({n_balanced_upcs:,} UPCs)")

        del df_year, df_balanced

    trends_df = pd.DataFrame(results)

    # Summary statistics
    print(f"\n{'='*80}")
    print("BALANCED PANEL SUMMARY")
    print("="*80)
    print(f"Total unique UPCs in balanced panel: {len(balanced_upcs):,}")
    print(f"\nCoverage by year:")
    print(f"{'Year':<6} {'All Units':>15} {'Balanced Units':>15} {'% Units':>12} {'% UPCs':>10}")
    print("-" * 65)
    for _, row in trends_df.iterrows():
        print(f"{int(row['panel_year']):<6} {int(row['n_units_all']):>15,} {int(row['n_units_balanced']):>15,} "
              f"{row['pct_units_in_balanced']:>11.1f}% {row['pct_upcs_in_balanced']:>9.1f}%")

    # Overall averages
    avg_pct_units = trends_df['pct_units_in_balanced'].mean()
    avg_pct_upcs = trends_df['pct_upcs_in_balanced'].mean()
    print("-" * 65)
    print(f"{'Average':<6} {'':<15} {'':<15} {avg_pct_units:>11.1f}% {avg_pct_upcs:>9.1f}%")

    # Save to cache
    if use_cache is not None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        trends_df.to_csv(BALANCED_PANEL_TRENDS_CACHE, index=False)
        print(f"\nSaved balanced panel trends to: {BALANCED_PANEL_TRENDS_CACHE}")

    return trends_df


def load_cpi_data():
    """
    Load CPI data and return target year CPI and lookup dictionary.

    Returns:
    --------
    tuple: (target_cpi, cpi_lookup dict mapping (year, month) -> CPI)
    """
    cpi_df = pd.read_csv(CPI_PATH)
    cpi_df['date'] = pd.to_datetime(cpi_df['observation_date'])
    cpi_df['year'] = cpi_df['date'].dt.year
    cpi_df['month'] = cpi_df['date'].dt.month
    cpi_df = cpi_df.dropna(subset=['CPIEBEV'])

    cpi_lookup = dict(zip(
        zip(cpi_df['year'], cpi_df['month']),
        cpi_df['CPIEBEV']
    ))

    target_cpi = cpi_df[cpi_df['year'] == TARGET_YEAR]['CPIEBEV'].mean()

    return target_cpi, cpi_lookup


def deflate_column(df, col_name, target_cpi, cpi_lookup, year_col='panel_year'):
    """
    Deflate a price column in place using CPI data.

    Parameters:
    -----------
    df : DataFrame
        Data with price column and date info
    col_name : str
        Name of column to deflate
    target_cpi : float
        CPI value for target year
    cpi_lookup : dict
        Mapping of (year, month) -> CPI
    year_col : str
        Name of year column in df

    Returns:
    --------
    Series with deflated values
    """
    if 'purchase_date' in df.columns:
        df['_purchase_date'] = pd.to_datetime(df['purchase_date'])
        df['_year'] = df['_purchase_date'].dt.year
        df['_month'] = df['_purchase_date'].dt.month
    else:
        df['_year'] = df[year_col]
        df['_month'] = 6  # Use June as midpoint

    # Map to CPI
    df['_cpi'] = df.apply(
        lambda row: cpi_lookup.get((row['_year'], row['_month'])),
        axis=1
    )

    # Fill missing with annual average
    for y in df[df['_cpi'].isna()]['_year'].unique():
        avg_cpi = sum(cpi_lookup.get((y, m), 0) for m in range(1, 13)) / 12
        if avg_cpi == 0:
            # Use closest available year
            available_years = sorted(set(k[0] for k in cpi_lookup.keys()))
            closest = min(available_years, key=lambda x: abs(x - y))
            avg_cpi = sum(cpi_lookup.get((closest, m), 0) for m in range(1, 13)) / 12
        df.loc[df['_cpi'].isna() & (df['_year'] == y), '_cpi'] = avg_cpi

    # Calculate deflated values
    deflated = df[col_name] * (target_cpi / df['_cpi'])

    # Clean up temp columns
    df.drop(columns=['_year', '_month', '_cpi'], inplace=True, errors='ignore')
    if '_purchase_date' in df.columns:
        df.drop(columns=['_purchase_date'], inplace=True)

    return deflated


def compute_expenditure_weighted_trends(years_to_process=None, use_cache=None, use_deflated=True):
    """
    Compute cornification trends weighted by expenditure (price paid).

    This answers: "What share of household food spending goes to corn products?"

    Parameters:
    -----------
    years_to_process : list, optional
        List of years to include. If None, processes all available years.
    use_cache : bool, optional
        Whether to use/save cached results. If None, uses global USE_CACHE setting.
    use_deflated : bool
        If True, use deflated prices. If False, use nominal prices.

    Returns:
    --------
    DataFrame with year and expenditure-weighted cornification rates
    """
    if use_cache is None:
        use_cache = USE_CACHE

    print(f"\n{'='*80}")
    print("COMPUTING EXPENDITURE-WEIGHTED CORNIFICATION TRENDS")
    print("="*80)

    # Check cache
    if use_cache and os.path.exists(EXPENDITURE_TRENDS_CACHE):
        print(f"Loading expenditure trends from cache: {EXPENDITURE_TRENDS_CACHE}")
        return pd.read_csv(EXPENDITURE_TRENDS_CACHE)

    # Determine which price column to use
    if use_deflated:
        price_col = f'total_price_paid_real_{TARGET_YEAR}'
        data_path = PURCHASES_DEFLATED_PATH
        print(f"Using deflated prices (real {TARGET_YEAR} $)")
    else:
        price_col = 'total_price_paid'
        data_path = PURCHASES_PATH
        print("Using nominal prices")

    # Check if deflated data exists
    if use_deflated and not os.path.exists(data_path):
        print(f"WARNING: Deflated data not found at {data_path}")
        print("Falling back to nominal prices with in-memory deflation")
        use_deflated = False
        price_col = 'total_price_paid'
        data_path = PURCHASES_PATH

    corn_var = 'first_ing_is_corn_usual_or_literal'

    # Find all year partitions
    year_dirs = sorted(glob(os.path.join(data_path, 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {data_path}")
        return None

    print(f"Found {len(year_dirs)} year partitions")

    # Load CPI if we need to deflate in memory
    target_cpi, cpi_lookup = None, None
    if not use_deflated:
        target_cpi, cpi_lookup = load_cpi_data()
        print(f"Loaded CPI data, target year {TARGET_YEAR} CPI: {target_cpi:.2f}")

    results = []

    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        if years_to_process is not None and year not in years_to_process:
            continue

        print(f"Processing year {year}...", end=' ')

        # Columns to load (always include quantity for count-based rate)
        cols_to_load = [corn_var, 'quantity']
        if use_deflated:
            cols_to_load.append(price_col)
        else:
            cols_to_load.extend(['total_price_paid', 'purchase_date'])

        try:
            df_year = pd.read_parquet(year_dir, columns=cols_to_load)
        except Exception as e:
            print(f"Error: {e}")
            continue

        df_year['quantity'] = df_year['quantity'].fillna(1)

        # Deflate in memory if needed
        if not use_deflated:
            df_year['panel_year'] = year
            df_year[f'total_price_paid_real_{TARGET_YEAR}'] = deflate_column(
                df_year, 'total_price_paid', target_cpi, cpi_lookup
            )
            price_col = f'total_price_paid_real_{TARGET_YEAR}'

        # Filter to rows with valid prices
        df_valid = df_year[(df_year[price_col] > 0) & (df_year[price_col].notna())].copy()

        n_total = len(df_valid)
        total_units = df_valid['quantity'].sum()
        total_spending = df_valid[price_col].sum()

        # Calculate corn vs non-corn spending
        corn_spending = df_valid[df_valid[corn_var] == True][price_col].sum()
        non_corn_spending = total_spending - corn_spending

        # Quantity-weighted count rate (for fair comparison with expenditure)
        corn_units = df_valid[df_valid[corn_var] == True]['quantity'].sum()
        count_rate = (corn_units / total_units) * 100 if total_units > 0 else 0

        # Expenditure-weighted rate
        expenditure_rate = (corn_spending / total_spending) * 100 if total_spending > 0 else 0

        year_result = {
            'panel_year': year,
            'n_purchases': n_total,
            'n_units': int(total_units),
            'total_spending_real_2013': total_spending,
            'corn_spending_real_2013': corn_spending,
            'non_corn_spending_real_2013': non_corn_spending,
            'count_based_rate': count_rate,
            'expenditure_weighted_rate': expenditure_rate,
        }

        results.append(year_result)

        print(f"{int(total_units):,} units, Count: {count_rate:.1f}%, Expenditure: {expenditure_rate:.1f}%")

        del df_year, df_valid

    trends_df = pd.DataFrame(results)

    # Save to cache
    if use_cache is not None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        trends_df.to_csv(EXPENDITURE_TRENDS_CACHE, index=False)
        print(f"\nSaved expenditure trends to: {EXPENDITURE_TRENDS_CACHE}")

    return trends_df


def compute_weight_based_trends(years_to_process=None, use_cache=None):
    """
    Compute cornification trends weighted by product weight/size.

    This answers: "What share of food weight/volume purchased contains corn?"

    Parameters:
    -----------
    years_to_process : list, optional
        List of years to include. If None, processes all available years.
    use_cache : bool, optional
        Whether to use/save cached results. If None, uses global USE_CACHE setting.

    Returns:
    --------
    DataFrame with year and weight-based cornification rates
    """
    if use_cache is None:
        use_cache = USE_CACHE

    print(f"\n{'='*80}")
    print("COMPUTING WEIGHT-BASED CORNIFICATION TRENDS")
    print("="*80)

    # Check cache
    if use_cache and os.path.exists(WEIGHT_TRENDS_CACHE):
        print(f"Loading weight trends from cache: {WEIGHT_TRENDS_CACHE}")
        return pd.read_csv(WEIGHT_TRENDS_CACHE)

    corn_var = 'first_ing_is_corn_usual_or_literal'
    weight_col = 'size1_amount'

    # Find all year partitions
    year_dirs = sorted(glob(os.path.join(PURCHASES_PATH, 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {PURCHASES_PATH}")
        return None

    print(f"Found {len(year_dirs)} year partitions")

    results = []

    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        if years_to_process is not None and year not in years_to_process:
            continue

        print(f"Processing year {year}...", end=' ')

        try:
            df_year = pd.read_parquet(year_dir, columns=[corn_var, weight_col, 'size1_units', 'quantity'])
        except Exception as e:
            print(f"Error: {e}")
            continue

        df_year['quantity'] = df_year['quantity'].fillna(1)

        # Calculate total weight per purchase (size * quantity)
        df_year['total_weight'] = df_year[weight_col] * df_year['quantity']

        # Filter to rows with valid weights
        df_valid = df_year[(df_year['total_weight'] > 0) & (df_year['total_weight'].notna())].copy()

        n_total = len(df_valid)
        total_units = df_valid['quantity'].sum()
        total_weight = df_valid['total_weight'].sum()

        # Calculate corn vs non-corn weight
        corn_weight = df_valid[df_valid[corn_var] == True]['total_weight'].sum()
        non_corn_weight = total_weight - corn_weight

        # Quantity-weighted count rate (for fair comparison)
        corn_units = df_valid[df_valid[corn_var] == True]['quantity'].sum()
        count_rate = (corn_units / total_units) * 100 if total_units > 0 else 0

        # Weight-based rate
        weight_rate = (corn_weight / total_weight) * 100 if total_weight > 0 else 0

        # Unit breakdown
        unit_counts = df_valid['size1_units'].value_counts().head(5).to_dict()

        year_result = {
            'panel_year': year,
            'n_purchases': n_total,
            'n_units': int(total_units),
            'total_weight': total_weight,
            'corn_weight': corn_weight,
            'non_corn_weight': non_corn_weight,
            'count_based_rate': count_rate,
            'weight_based_rate': weight_rate,
            'top_units': str(unit_counts),
        }

        results.append(year_result)

        print(f"{int(total_units):,} units, Count: {count_rate:.1f}%, Weight: {weight_rate:.1f}%")

        del df_year, df_valid

    trends_df = pd.DataFrame(results)

    # Save to cache
    if use_cache is not None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        trends_df.to_csv(WEIGHT_TRENDS_CACHE, index=False)
        print(f"\nSaved weight trends to: {WEIGHT_TRENDS_CACHE}")

    return trends_df


def compute_household_spending_trends(years_to_process=None, use_cache=None, use_deflated=True):
    """
    Compute household-level annual spending on corn vs non-corn products.

    This answers: "What share of each household's annual grocery budget goes to corn products?"

    Parameters:
    -----------
    years_to_process : list, optional
        List of years to include. If None, processes all available years.
    use_cache : bool, optional
        Whether to use/save cached results. If None, uses global USE_CACHE setting.
    use_deflated : bool
        If True, use deflated prices. If False, use nominal prices.

    Returns:
    --------
    DataFrame with year, mean HH corn share, median HH corn share, etc.
    """
    if use_cache is None:
        use_cache = USE_CACHE

    print(f"\n{'='*80}")
    print("COMPUTING HOUSEHOLD-LEVEL SPENDING TRENDS")
    print("="*80)

    # Check cache
    if use_cache and os.path.exists(HH_SPENDING_TRENDS_CACHE):
        print(f"Loading HH spending trends from cache: {HH_SPENDING_TRENDS_CACHE}")
        return pd.read_csv(HH_SPENDING_TRENDS_CACHE)

    # Determine which price column to use
    if use_deflated:
        price_col = f'total_price_paid_real_{TARGET_YEAR}'
        data_path = PURCHASES_DEFLATED_PATH
        print(f"Using deflated prices (real {TARGET_YEAR} $)")
    else:
        price_col = 'total_price_paid'
        data_path = PURCHASES_PATH
        print("Using nominal prices")

    # Check if deflated data exists
    if use_deflated and not os.path.exists(data_path):
        print(f"WARNING: Deflated data not found at {data_path}")
        print("Falling back to nominal prices with in-memory deflation")
        use_deflated = False
        price_col = 'total_price_paid'
        data_path = PURCHASES_PATH

    corn_var = 'first_ing_is_corn_usual_or_literal'

    # Find all year partitions
    year_dirs = sorted(glob(os.path.join(data_path, 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {data_path}")
        return None

    print(f"Found {len(year_dirs)} year partitions")

    # Load CPI if we need to deflate in memory
    target_cpi, cpi_lookup = None, None
    if not use_deflated:
        target_cpi, cpi_lookup = load_cpi_data()
        print(f"Loaded CPI data, target year {TARGET_YEAR} CPI: {target_cpi:.2f}")

    results = []

    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        if years_to_process is not None and year not in years_to_process:
            continue

        print(f"Processing year {year}...", end=' ')

        # Columns to load (include quantity for count-based comparison)
        cols_to_load = ['household_code', corn_var, 'quantity']
        if use_deflated:
            cols_to_load.append(price_col)
        else:
            cols_to_load.extend(['total_price_paid', 'purchase_date'])

        try:
            df_year = pd.read_parquet(year_dir, columns=cols_to_load)
        except Exception as e:
            print(f"Error: {e}")
            continue

        df_year['quantity'] = df_year['quantity'].fillna(1)

        # Deflate in memory if needed
        if not use_deflated:
            df_year['panel_year'] = year
            df_year[f'total_price_paid_real_{TARGET_YEAR}'] = deflate_column(
                df_year, 'total_price_paid', target_cpi, cpi_lookup
            )
            price_col = f'total_price_paid_real_{TARGET_YEAR}'

        # Filter to valid prices
        df_valid = df_year[(df_year[price_col] > 0) & (df_year[price_col].notna())].copy()

        # Calculate corn quantity for each row
        df_valid['corn_quantity'] = df_valid['quantity'] * df_valid[corn_var].astype(int)

        # Aggregate to household level
        hh_spending = df_valid.groupby('household_code').agg(
            total_spending=(price_col, 'sum'),
            corn_spending=(price_col, lambda x: x[df_valid.loc[x.index, corn_var] == True].sum()),
            n_units=('quantity', 'sum'),
            n_corn_units=('corn_quantity', 'sum'),
        ).reset_index()

        hh_spending['corn_share'] = hh_spending['corn_spending'] / hh_spending['total_spending'] * 100
        hh_spending['corn_share_count'] = hh_spending['n_corn_units'] / hh_spending['n_units'] * 100

        # Summary statistics across households
        n_households = len(hh_spending)

        year_result = {
            'panel_year': year,
            'n_households': n_households,
            'mean_hh_total_spending_real_2013': hh_spending['total_spending'].mean(),
            'median_hh_total_spending_real_2013': hh_spending['total_spending'].median(),
            'mean_hh_corn_share': hh_spending['corn_share'].mean(),
            'median_hh_corn_share': hh_spending['corn_share'].median(),
            'p10_hh_corn_share': hh_spending['corn_share'].quantile(0.10),
            'p90_hh_corn_share': hh_spending['corn_share'].quantile(0.90),
            'mean_hh_corn_share_count': hh_spending['corn_share_count'].mean(),
        }

        results.append(year_result)

        print(f"{n_households:,} HHs, Mean corn share: {year_result['mean_hh_corn_share']:.1f}%, "
              f"Median: {year_result['median_hh_corn_share']:.1f}%")

        del df_year, df_valid, hh_spending

    trends_df = pd.DataFrame(results)

    # Save to cache
    if use_cache is not None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        trends_df.to_csv(HH_SPENDING_TRENDS_CACHE, index=False)
        print(f"\nSaved HH spending trends to: {HH_SPENDING_TRENDS_CACHE}")

    return trends_df


def plot_expenditure_and_weight_trends(expenditure_df, weight_df, hh_spending_df=None,
                                        output_path_exp=None, output_path_weight=None,
                                        output_path_hh=None):
    """
    Plot expenditure-weighted and weight-based cornification trends.

    Parameters:
    -----------
    expenditure_df : DataFrame
        Output from compute_expenditure_weighted_trends()
    weight_df : DataFrame
        Output from compute_weight_based_trends()
    hh_spending_df : DataFrame, optional
        Output from compute_household_spending_trends()
    output_path_exp : str, optional
        Path to save expenditure figure
    output_path_weight : str, optional
        Path to save weight figure
    output_path_hh : str, optional
        Path to save household spending figure
    """
    figures = []

    # --- Figure 1: Expenditure-weighted only ---
    if expenditure_df is not None:
        fig1, ax1 = plt.subplots(figsize=(12, 7))

        ax1.plot(expenditure_df['panel_year'], expenditure_df['expenditure_weighted_rate'],
                 marker='s', linewidth=2, label='Expenditure-weighted (% of $)', color='#ff7f0e')

        ax1.set_xlabel('Year')
        ax1.set_ylabel('% of Units / Spending')
        ax1.set_title(f'Cornification Trends: Expenditure-Weighted\n(Real {TARGET_YEAR} dollars, quantity-weighted)')
        ax1.legend(loc='best')
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
        ax1.set_ylim(2, 3.5)

        plt.tight_layout()

        if output_path_exp:
            plt.savefig(output_path_exp, dpi=150, bbox_inches='tight')
            print(f"\nSaved expenditure trends to: {output_path_exp}")

        plt.show()
        figures.append(fig1)

        # Print interpretation
        start_exp = expenditure_df['expenditure_weighted_rate'].iloc[0]
        end_exp = expenditure_df['expenditure_weighted_rate'].iloc[-1]

        print(f"\nEXPENDITURE-WEIGHTED INTERPRETATION:")
        print(f"  Expenditure: {start_exp:.1f}% -> {end_exp:.1f}% (change: {end_exp - start_exp:+.1f} pp)")

    # --- Figure 2: Weight-based only ---
    if weight_df is not None:
        fig2, ax2 = plt.subplots(figsize=(12, 7))

        ax2.plot(weight_df['panel_year'], weight_df['weight_based_rate'],
                 marker='s', linewidth=2, label='Weight-based (% of volume)', color='#2ca02c')

        ax2.set_xlabel('Year')
        ax2.set_ylabel('% of Units / Weight')
        ax2.set_title('Cornification Trends: Weight-Based\n(Quantity-weighted)')
        ax2.legend(loc='best')
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)
        ax2.set_ylim(2, 3.5)

        plt.tight_layout()

        if output_path_weight:
            plt.savefig(output_path_weight, dpi=150, bbox_inches='tight')
            print(f"\nSaved weight trends to: {output_path_weight}")

        plt.show()
        figures.append(fig2)

        # Print interpretation
        start_wt = weight_df['weight_based_rate'].iloc[0]
        end_wt = weight_df['weight_based_rate'].iloc[-1]

        print(f"\nWEIGHT-BASED INTERPRETATION:")
        print(f"  Weight-based: {start_wt:.1f}% -> {end_wt:.1f}% (change: {end_wt - start_wt:+.1f} pp)")

    # --- Figure 3: Household-level spending ---
    if hh_spending_df is not None:
        fig3, ax3 = plt.subplots(figsize=(12, 7))

        ax3.plot(hh_spending_df['panel_year'], hh_spending_df['mean_hh_corn_share'],
                 marker='o', linewidth=2, label='Mean HH corn share', color='#1f77b4')
        ax3.plot(hh_spending_df['panel_year'], hh_spending_df['median_hh_corn_share'],
                 marker='s', linewidth=2, label='Median HH corn share', color='#ff7f0e')

        # Add shaded area for 10th-90th percentile
        ax3.fill_between(hh_spending_df['panel_year'],
                         hh_spending_df['p10_hh_corn_share'],
                         hh_spending_df['p90_hh_corn_share'],
                         alpha=0.2, color='#1f77b4', label='10th-90th percentile')

        ax3.set_xlabel('Year')
        ax3.set_ylabel('% of HH Annual Spending on Corn Products')
        ax3.set_title(f'Household Corn Spending Share\n(Real {TARGET_YEAR} dollars)')
        ax3.legend(loc='best')
        ax3.grid(True, alpha=0.3)
        ax3.tick_params(axis='x', rotation=45)

        plt.tight_layout()

        if output_path_hh:
            plt.savefig(output_path_hh, dpi=150, bbox_inches='tight')
            print(f"\nSaved HH spending trends to: {output_path_hh}")

        plt.show()
        figures.append(fig3)

        # Print summary
        print(f"\nHOUSEHOLD-LEVEL SUMMARY (Real {TARGET_YEAR}$):")
        print(f"  Average HH annual food spending: ${hh_spending_df['mean_hh_total_spending_real_2013'].mean():.0f}")
        print(f"  Average share going to corn: {hh_spending_df['mean_hh_corn_share'].mean():.1f}%")

    return figures


def plot_balanced_panel_comparison(trends_df, corn_var='first_ing_is_corn_usual_or_literal',
                                    output_path_trends=None, output_path_coverage=None):
    """
    Plot cornification trends comparing all purchases vs balanced panel.
    Creates two separate figures.

    Parameters:
    -----------
    trends_df : DataFrame
        Output from compute_balanced_panel_trends()
    corn_var : str
        Which corn variable to plot
    output_path_trends : str, optional
        Path to save the trends figure
    output_path_coverage : str, optional
        Path to save the coverage figure
    """
    figures = []
    var_all = f'{corn_var}_all'
    var_balanced = f'{corn_var}_balanced'

    # --- Figure 1: Cornification trends ---
    fig1, ax1 = plt.subplots(figsize=(12, 7))

    ax1.plot(trends_df['panel_year'], trends_df[var_all],
             marker='o', linewidth=2, label='All purchases', color='#1f77b4')
    ax1.plot(trends_df['panel_year'], trends_df[var_balanced],
             marker='s', linewidth=2, label='Balanced panel (same UPCs)', color='#ff7f0e')

    ax1.set_xlabel('Year')
    ax1.set_ylabel('% of Units with Corn Ingredient (quantity-weighted)')
    ax1.set_title('Cornification Trends: All Units vs Balanced Panel\n'
                  '(Any ingredient is corn - usual or literal, quantity-weighted)')
    ax1.legend(loc='best')
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)

    plt.tight_layout()

    if output_path_trends:
        plt.savefig(output_path_trends, dpi=150, bbox_inches='tight')
        print(f"\nSaved balanced panel trends to: {output_path_trends}")

    plt.show()
    figures.append(fig1)

    # --- Figure 2: Coverage statistics ---
    fig2, ax2 = plt.subplots(figsize=(12, 7))

    ax2.plot(trends_df['panel_year'], trends_df['pct_units_in_balanced'],
             marker='o', linewidth=2, label='% of Units', color='#2ca02c')
    ax2.plot(trends_df['panel_year'], trends_df['pct_upcs_in_balanced'],
             marker='s', linewidth=2, label='% of UPCs', color='#d62728')

    ax2.set_xlabel('Year')
    ax2.set_ylabel('Percentage')
    ax2.set_title('Balanced Panel Coverage\n(UPCs present in all years 2004-2020, quantity-weighted)')
    ax2.legend(loc='best')
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)
    ax2.set_ylim(0, 100)

    plt.tight_layout()

    if output_path_coverage:
        plt.savefig(output_path_coverage, dpi=150, bbox_inches='tight')
        print(f"Saved balanced panel coverage to: {output_path_coverage}")

    plt.show()
    figures.append(fig2)

    # Print interpretation
    start_all = trends_df[var_all].iloc[0]
    end_all = trends_df[var_all].iloc[-1]
    start_balanced = trends_df[var_balanced].iloc[0]
    end_balanced = trends_df[var_balanced].iloc[-1]

    change_all = end_all - start_all
    change_balanced = end_balanced - start_balanced

    print(f"\n{'='*80}")
    print("INTERPRETATION: New Products vs Reformulation")
    print("="*80)
    print(f"\nChange in cornification ({int(trends_df['panel_year'].iloc[0])} to {int(trends_df['panel_year'].iloc[-1])}):")
    print(f"  All purchases:    {start_all:.1f}% -> {end_all:.1f}% (change: {change_all:+.1f} pp)")
    print(f"  Balanced panel:   {start_balanced:.1f}% -> {end_balanced:.1f}% (change: {change_balanced:+.1f} pp)")
    print(f"\nDecomposition:")
    print(f"  Reformulation effect (balanced panel change):  {change_balanced:+.1f} pp")
    print(f"  Composition effect (difference):               {change_all - change_balanced:+.1f} pp")

    if abs(change_balanced) > abs(change_all - change_balanced):
        print(f"\n-> The majority of the cornification change appears to be due to REFORMULATION")
        print(f"   of existing products rather than introduction of new products.")
    else:
        print(f"\n-> The majority of the cornification change appears to be due to NEW PRODUCT")
        print(f"   introduction rather than reformulation of existing products.")

    return figures


def get_top_cornified_modules(module_trends_df, corn_var='first_ing_is_corn_usual_or_literal', top_n=10):
    """
    Get the top N most cornified product modules (weighted average across years).

    Parameters:
    -----------
    module_trends_df : DataFrame
        Output from compute_trends_by_product_module()
    corn_var : str
        Which corn variable to rank by
    top_n : int
        Number of top modules to return

    Returns:
    --------
    list of module names (most cornified first)
    """
    # Compute weighted average cornification rate across all years
    def weighted_avg(group):
        total_purchases = group['n_purchases'].sum()
        weighted_rate = (group[corn_var] * group['n_purchases']).sum() / total_purchases
        return pd.Series({
            'weighted_rate': weighted_rate,
            'total_purchases': total_purchases
        })

    module_avg = module_trends_df.groupby('product_module_descr').apply(weighted_avg).reset_index()
    module_avg = module_avg.sort_values('weighted_rate', ascending=False)

    top_modules = module_avg.head(top_n)['product_module_descr'].tolist()

    print(f"\nTop {top_n} most cornified modules ({corn_var}):")
    for i, row in module_avg.head(top_n).iterrows():
        print(f"  {row['product_module_descr']}: {row['weighted_rate']:.1f}%")

    return top_modules


def get_modules_with_biggest_changes(module_trends_df, corn_var='first_ing_is_corn_usual_or_literal',
                                     top_n=10, start_year=2004, end_year=2020, direction='both'):
    """
    Get modules with the biggest changes (increase or decrease) in cornification.

    Parameters:
    -----------
    module_trends_df : DataFrame
        Output from compute_trends_by_product_module()
    corn_var : str
        Which corn variable to measure change in
    top_n : int
        Number of modules to return
    start_year : int
        Starting year for comparison
    end_year : int
        Ending year for comparison
    direction : str
        'both' for biggest absolute changes, 'increase' for top increases, 'decrease' for top decreases

    Returns:
    --------
    DataFrame with module names, start/end rates, and change values
    """
    # Filter to modules that exist in both start and end years
    start_data = module_trends_df[module_trends_df['panel_year'] == start_year][['product_module_descr', corn_var, 'n_purchases']]
    end_data = module_trends_df[module_trends_df['panel_year'] == end_year][['product_module_descr', corn_var, 'n_purchases']]

    start_data = start_data.rename(columns={corn_var: 'start_rate', 'n_purchases': 'start_purchases'})
    end_data = end_data.rename(columns={corn_var: 'end_rate', 'n_purchases': 'end_purchases'})

    # Merge to get modules present in both years
    merged = start_data.merge(end_data, on='product_module_descr', how='inner')

    # Calculate change
    merged['change'] = merged['end_rate'] - merged['start_rate']
    merged['abs_change'] = merged['change'].abs()

    # Sort based on direction
    if direction == 'increase':
        top_changers = merged.nlargest(top_n, 'change')
        direction_label = "increases"
    elif direction == 'decrease':
        top_changers = merged.nsmallest(top_n, 'change')
        direction_label = "decreases"
    else:
        top_changers = merged.nlargest(top_n, 'abs_change')
        direction_label = "changes"

    print(f"\nTop {top_n} modules with biggest cornification {direction_label} ({start_year} to {end_year}):")
    print(f"{'Module':<45} {'Start':>8} {'End':>8} {'Change':>10}")
    print("-" * 75)
    for _, row in top_changers.iterrows():
        direction_sign = "+" if row['change'] > 0 else ""
        print(f"{row['product_module_descr'][:44]:<45} {row['start_rate']:>7.1f}% {row['end_rate']:>7.1f}% {direction_sign}{row['change']:>8.1f}pp")

    return top_changers


def plot_cornification_by_module(module_trends_df, top_modules=None, corn_var='first_ing_is_corn_usual_or_literal',
                                  top_n=10, output_path_bar=None, output_path_timeseries=None):
    """
    Create plots showing cornification by product module.

    Parameters:
    -----------
    module_trends_df : DataFrame
        Output from compute_trends_by_product_module()
    top_modules : list, optional
        List of modules to include. If None, uses top_n most cornified.
    corn_var : str
        Which corn variable to plot
    top_n : int
        Number of top modules to show (if top_modules not provided)
    output_path_bar : str, optional
        Path to save bar chart
    output_path_timeseries : str, optional
        Path to save time series plot
    """
    # Get top modules if not provided
    if top_modules is None:
        top_modules = get_top_cornified_modules(module_trends_df, corn_var=corn_var, top_n=top_n)

    # --- Plot A: Bar chart of overall cornification rates ---
    fig1, ax1 = plt.subplots(figsize=(12, 8))

    # Compute weighted average for each module
    def weighted_avg(group):
        total_purchases = group['n_purchases'].sum()
        weighted_rate = (group[corn_var] * group['n_purchases']).sum() / total_purchases
        return weighted_rate

    module_rates = module_trends_df.groupby('product_module_descr').apply(weighted_avg)
    top_rates = module_rates[module_rates.index.isin(top_modules)].sort_values(ascending=True)

    # Shorten long module names for display
    display_names = [name[:40] + '...' if len(name) > 40 else name for name in top_rates.index]

    bars = ax1.barh(display_names, top_rates.values, color='steelblue')
    ax1.set_xlabel('% of Purchases with Corn Ingredient')
    ax1.set_title(f'Top {len(top_modules)} Most Cornified Product Categories\n(Any ingredient is corn - usual or literal)')
    ax1.grid(True, alpha=0.3, axis='x')

    # Add value labels on bars
    for bar, val in zip(bars, top_rates.values):
        ax1.text(val + 0.5, bar.get_y() + bar.get_height()/2, f'{val:.1f}%',
                 va='center', fontsize=9)

    plt.tight_layout()

    if output_path_bar:
        plt.savefig(output_path_bar, dpi=150, bbox_inches='tight')
        print(f"\nSaved bar chart to: {output_path_bar}")

    plt.show()

    # --- Plot B: Time series ---
    fig2, ax2 = plt.subplots(figsize=(14, 8))

    # Filter to top modules
    filtered_df = module_trends_df[module_trends_df['product_module_descr'].isin(top_modules)]

    for module in top_modules:
        module_data = filtered_df[filtered_df['product_module_descr'] == module].sort_values('panel_year')
        # Shorten name for legend
        display_name = module[:30] + '...' if len(module) > 30 else module
        ax2.plot(module_data['panel_year'], module_data[corn_var],
                 marker='o', linewidth=2, label=display_name)

    ax2.set_xlabel('Year')
    ax2.set_ylabel('% of Purchases with Corn Ingredient')
    ax2.set_title(f'Cornification Trends Over Time by Product Category\n(Any ingredient is corn - usual or literal)')
    ax2.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)

    plt.tight_layout()

    if output_path_timeseries:
        plt.savefig(output_path_timeseries, dpi=150, bbox_inches='tight')
        print(f"Saved time series to: {output_path_timeseries}")

    plt.show()

    return fig1, fig2


def plot_biggest_cornification_changes(module_trends_df, corn_var='first_ing_is_corn_usual_or_literal',
                                        top_n=10, start_year=2004, end_year=2020,
                                        output_path_increases_bar=None, output_path_increases_ts=None,
                                        output_path_decreases_bar=None, output_path_decreases_ts=None):
    """
    Plot modules with the biggest increases and decreases in cornification over time.
    Creates separate plots for increases and decreases.

    Parameters:
    -----------
    module_trends_df : DataFrame
        Output from compute_trends_by_product_module()
    corn_var : str
        Which corn variable to plot
    top_n : int
        Number of top changers to show in each direction
    start_year : int
        Starting year for comparison
    end_year : int
        Ending year for comparison
    output_path_increases_bar : str, optional
        Path to save increases bar chart
    output_path_increases_ts : str, optional
        Path to save increases time series
    output_path_decreases_bar : str, optional
        Path to save decreases bar chart
    output_path_decreases_ts : str, optional
        Path to save decreases time series
    """
    # Get modules with biggest increases
    top_increases = get_modules_with_biggest_changes(
        module_trends_df, corn_var=corn_var, top_n=top_n,
        start_year=start_year, end_year=end_year, direction='increase'
    )

    # Get modules with biggest decreases
    top_decreases = get_modules_with_biggest_changes(
        module_trends_df, corn_var=corn_var, top_n=top_n,
        start_year=start_year, end_year=end_year, direction='decrease'
    )

    figures = []

    # --- Plot 1: Bar chart of INCREASES ---
    fig1, ax1 = plt.subplots(figsize=(12, 8))
    plot_data = top_increases.sort_values('change', ascending=True)
    display_names = [name[:40] + '...' if len(name) > 40 else name for name in plot_data['product_module_descr']]

    bars = ax1.barh(display_names, plot_data['change'], color='#2ca02c')
    ax1.set_xlabel(f'Change in Cornification Rate (percentage points, {start_year} to {end_year})')
    ax1.set_title(f'Top {top_n} Product Categories with Biggest Cornification INCREASES\n({start_year} to {end_year})')
    ax1.grid(True, alpha=0.3, axis='x')

    for bar, val in zip(bars, plot_data['change']):
        ax1.text(val + 0.5, bar.get_y() + bar.get_height()/2, f'+{val:.1f}',
                 va='center', ha='left', fontsize=9)

    plt.tight_layout()
    if output_path_increases_bar:
        plt.savefig(output_path_increases_bar, dpi=150, bbox_inches='tight')
        print(f"\nSaved increases bar chart to: {output_path_increases_bar}")
    plt.show()
    figures.append(fig1)

    # --- Plot 2: Time series for INCREASES ---
    fig2, ax2 = plt.subplots(figsize=(14, 8))
    increase_modules = top_increases['product_module_descr'].tolist()
    filtered_df = module_trends_df[module_trends_df['product_module_descr'].isin(increase_modules)]

    for module in increase_modules:
        module_data = filtered_df[filtered_df['product_module_descr'] == module].sort_values('panel_year')
        display_name = module[:30] + '...' if len(module) > 30 else module
        ax2.plot(module_data['panel_year'], module_data[corn_var],
                 marker='o', linewidth=2, label=display_name)

    ax2.set_xlabel('Year')
    ax2.set_ylabel('% of Purchases with Corn Ingredient')
    ax2.set_title(f'Cornification Trends for Categories with Biggest INCREASES\n({start_year} to {end_year})')
    ax2.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)

    plt.tight_layout()
    if output_path_increases_ts:
        plt.savefig(output_path_increases_ts, dpi=150, bbox_inches='tight')
        print(f"Saved increases time series to: {output_path_increases_ts}")
    plt.show()
    figures.append(fig2)

    # --- Plot 3: Bar chart of DECREASES ---
    fig3, ax3 = plt.subplots(figsize=(12, 8))
    plot_data = top_decreases.sort_values('change', ascending=False)
    display_names = [name[:40] + '...' if len(name) > 40 else name for name in plot_data['product_module_descr']]

    bars = ax3.barh(display_names, plot_data['change'], color='#d62728')
    ax3.set_xlabel(f'Change in Cornification Rate (percentage points, {start_year} to {end_year})')
    ax3.set_title(f'Top {top_n} Product Categories with Biggest Cornification DECREASES\n({start_year} to {end_year})')
    ax3.grid(True, alpha=0.3, axis='x')

    for bar, val in zip(bars, plot_data['change']):
        ax3.text(val - 0.5, bar.get_y() + bar.get_height()/2, f'{val:.1f}',
                 va='center', ha='right', fontsize=9)

    plt.tight_layout()
    if output_path_decreases_bar:
        plt.savefig(output_path_decreases_bar, dpi=150, bbox_inches='tight')
        print(f"\nSaved decreases bar chart to: {output_path_decreases_bar}")
    plt.show()
    figures.append(fig3)

    # --- Plot 4: Time series for DECREASES ---
    fig4, ax4 = plt.subplots(figsize=(14, 8))
    decrease_modules = top_decreases['product_module_descr'].tolist()
    filtered_df = module_trends_df[module_trends_df['product_module_descr'].isin(decrease_modules)]

    for module in decrease_modules:
        module_data = filtered_df[filtered_df['product_module_descr'] == module].sort_values('panel_year')
        display_name = module[:30] + '...' if len(module) > 30 else module
        ax4.plot(module_data['panel_year'], module_data[corn_var],
                 marker='o', linewidth=2, label=display_name)

    ax4.set_xlabel('Year')
    ax4.set_ylabel('% of Purchases with Corn Ingredient')
    ax4.set_title(f'Cornification Trends for Categories with Biggest DECREASES\n({start_year} to {end_year})')
    ax4.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize=9)
    ax4.grid(True, alpha=0.3)
    ax4.tick_params(axis='x', rotation=45)

    plt.tight_layout()
    if output_path_decreases_ts:
        plt.savefig(output_path_decreases_ts, dpi=150, bbox_inches='tight')
        print(f"Saved decreases time series to: {output_path_decreases_ts}")
    plt.show()
    figures.append(fig4)

    return figures


def plot_trends_by_demographic(trends_df, demographic_var, corn_var='first_ing_is_corn_usual_or_literal',
                                title=None, output_path=None, n_bins=None, bin_labels=None, y_limits=None):
    """
    Plot corn trends over time, with separate lines for each demographic group.

    Parameters:
    -----------
    trends_df : DataFrame
        Output from compute_trends_by_demographic()
    demographic_var : str or list of str
        Name of the demographic variable(s) (for labeling)
    corn_var : str
        Which corn variable to plot
    title : str, optional
        Plot title (auto-generated if None)
    output_path : str, optional
        Path to save the figure
    n_bins : int, optional
        If provided, bin the demographic variable into this many quantile groups.
        Useful for continuous variables like income. If None, treats as categorical.
    bin_labels : list of str, optional
        Custom labels for bins (must match n_bins length). If None, auto-generates
        labels like 'Q1 (lowest)', 'Q2', etc.
    """
    # Work on a copy to avoid modifying the original
    trends_df = trends_df.copy()

    fig, ax = plt.subplots(figsize=(12, 7))

    if isinstance(demographic_var, str):
        group_cols = [demographic_var]
    else:
        group_cols = list(demographic_var)

    # Handle binning for continuous variables
    if n_bins is not None and len(group_cols) == 1:
        col = group_cols[0]
        # Check if variable looks continuous (many unique values)
        n_unique = trends_df[col].nunique()
        if n_unique > n_bins:
            print(f"Binning {col} into {n_bins} quantile groups (had {n_unique} unique values)")
            if bin_labels is None:
                bin_labels = [f'Q{i+1}' for i in range(n_bins)]
                bin_labels[0] = f'Q1 (lowest)'
                bin_labels[-1] = f'Q{n_bins} (highest)'
            trends_df['demographic_group'] = pd.qcut(
                trends_df[col], q=n_bins, labels=bin_labels, duplicates='drop'
            )
        else:
            trends_df['demographic_group'] = trends_df[col]
    elif len(group_cols) == 1:
        trends_df['demographic_group'] = trends_df[group_cols[0]]
    else:
        trends_df['demographic_group'] = trends_df[group_cols].astype(str).agg(' | '.join, axis=1)

    # Get unique demographic groups
    groups = trends_df['demographic_group'].dropna().unique()

    # Define custom sort order for income groups
    income_group_order = ['Under $25k', '$25k-$50k', '$50k-$100k', '$100k+']

    # Sort groups - use income order if applicable, otherwise try numeric, then string
    if all(g in income_group_order for g in groups):
        groups = [g for g in income_group_order if g in groups]
    else:
        try:
            groups = sorted(groups, key=lambda x: float(x) if not isinstance(x, str) else x)
        except (ValueError, TypeError):
            groups = sorted(groups, key=str)

    for group in groups:
        group_data = trends_df[trends_df['demographic_group'] == group].sort_values('panel_year')
        ax.plot(group_data['panel_year'], group_data[corn_var],
                marker='o', linewidth=2, label=f'{group}')

    ax.set_xlabel('Year')
    ax.set_ylabel('% of Purchases')

    if title is None:
        if len(group_cols) == 1:
            title_var = group_cols[0].replace("_", " ").title()
        else:
            title_var = " x ".join([c.replace("_", " ").title() for c in group_cols])
        title = f'Corn-Derived Food Purchases by {title_var}'
    ax.set_title(title)

    if len(group_cols) == 1:
        legend_title = group_cols[0].replace('_', ' ').title()
    else:
        legend_title = " | ".join([c.replace('_', ' ').title() for c in group_cols])
    ax.legend(loc='best', title=legend_title)
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis='x', rotation=45)
    if y_limits is not None:
        ax.set_ylim(y_limits)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"\nSaved plot to: {output_path}")

    plt.show()

    return fig


def compute_yearly_trends(years_to_process=None, use_cache=None):
    """
    Compute yearly averages for corn classification variables.

    Parameters:
    -----------
    years_to_process : list, optional
        List of years to include (e.g., [2004, 2005, 2006]).
        If None, processes all available years.
    use_cache : bool, optional
        Whether to use/save cached results. If None, uses global USE_CACHE setting.

    Processes each year's partition individually to minimize memory usage.
    Returns a DataFrame with year as index and corn variables as columns.
    """
    if use_cache is None:
        use_cache = USE_CACHE

    # Try to load from cache
    if use_cache and os.path.exists(YEARLY_TRENDS_CACHE):
        print(f"Loading yearly trends from cache: {YEARLY_TRENDS_CACHE}")
        return pd.read_csv(YEARLY_TRENDS_CACHE, index_col='panel_year')

    corn_vars = [
        'first_ing_is_corn_literal',
        'first_ing_is_corn_usual_or_literal',
        'any_ing_is_corn_literal',
        'any_ing_is_corn_usual_or_literal',
        'any_ing_is_corn_any',
    ]

    # Find all year partitions
    year_dirs = sorted(glob(os.path.join(PURCHASES_PATH, 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {PURCHASES_PATH}")
        return None

    print(f"Found {len(year_dirs)} year partitions")

    # Store results: {year: {var: mean_value}}
    results = []

    for year_dir in year_dirs:
        # Extract year from directory name
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        # Skip if not in requested years
        if years_to_process is not None and year not in years_to_process:
            continue

        print(f"Processing year {year}...", end=' ')

        # Load only the columns we need for this year
        try:
            df_year = pd.read_parquet(year_dir, columns=corn_vars + ['quantity'])
        except Exception as e:
            print(f"Error: {e}")
            continue

        # Fill missing quantity with 1
        df_year['quantity'] = df_year['quantity'].fillna(1)

        # Compute quantity-weighted means for this year
        total_quantity = df_year['quantity'].sum()
        year_means = pd.Series()
        for var in corn_vars:
            # Weighted mean: sum(corn_flag * quantity) / sum(quantity)
            weighted_sum = (df_year[var] * df_year['quantity']).sum()
            year_means[var] = (weighted_sum / total_quantity) * 100

        year_means['panel_year'] = year
        year_means['n_purchases'] = len(df_year)
        year_means['n_units'] = int(total_quantity)
        results.append(year_means)

        print(f"{len(df_year):,} purchases, {int(total_quantity):,} units")

        # Free memory
        del df_year

    # Combine into DataFrame
    yearly_trends = pd.DataFrame(results).set_index('panel_year').sort_index()

    print("\nYearly trends (% of units, quantity-weighted):")
    print(yearly_trends.drop(columns=['n_purchases', 'n_units'], errors='ignore').round(2))

    # Save to cache
    if use_cache is not None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        yearly_trends.to_csv(YEARLY_TRENDS_CACHE)
        print(f"Saved yearly trends cache to: {YEARLY_TRENDS_CACHE}")

    return yearly_trends


def compute_yearly_trends_excluding_hfcs(years_to_process=None, use_cache=None):
    """
    Compute yearly averages for first ingredient corn classification,
    excluding high fructose corn syrup.

    This requires loading the ingredients column and checking the first ingredient.
    """
    if use_cache is None:
        use_cache = USE_CACHE

    # Try to load from cache
    if use_cache and os.path.exists(HFCS_TRENDS_CACHE):
        print(f"Loading HFCS trends from cache: {HFCS_TRENDS_CACHE}")
        return pd.read_csv(HFCS_TRENDS_CACHE, index_col='panel_year')

    # Find all year partitions
    year_dirs = sorted(glob(os.path.join(PURCHASES_PATH, 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {PURCHASES_PATH}")
        return None

    print(f"\nComputing trends excluding HFCS...")
    print(f"Found {len(year_dirs)} year partitions")

    results = []

    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        if years_to_process is not None and year not in years_to_process:
            continue

        print(f"Processing year {year} (excluding HFCS)...", end=' ')

        try:
            # Load corn vars plus ingredients column and quantity
            df_year = pd.read_parquet(year_dir, columns=[
                'first_ing_is_corn_literal',
                'first_ing_is_corn_usual_or_literal',
                'ingredients',
                'quantity'
            ])
        except Exception as e:
            print(f"Error: {e}")
            continue

        n_total = len(df_year)
        df_year['quantity'] = df_year['quantity'].fillna(1)
        total_quantity = df_year['quantity'].sum()

        # Get first ingredient and check if HFCS
        df_year['first_ingredient'] = df_year['ingredients'].apply(get_first_ingredient)
        df_year['first_ing_is_hfcs'] = df_year['first_ingredient'].apply(is_hfcs)

        # Compute: first ingredient is corn BUT NOT HFCS
        df_year['first_ing_corn_literal_no_hfcs'] = (
            df_year['first_ing_is_corn_literal'] & ~df_year['first_ing_is_hfcs']
        )
        df_year['first_ing_corn_usual_no_hfcs'] = (
            df_year['first_ing_is_corn_usual_or_literal'] & ~df_year['first_ing_is_hfcs']
        )

        # Also compute HFCS-only rate
        df_year['first_ing_is_hfcs_and_corn'] = (
            df_year['first_ing_is_corn_literal'] & df_year['first_ing_is_hfcs']
        )

        # Compute quantity-weighted means
        def weighted_mean(col):
            return (df_year[col] * df_year['quantity']).sum() / total_quantity * 100

        year_results = {
            'panel_year': year,
            'n_purchases': n_total,
            'n_units': int(total_quantity),
            'first_ing_corn_literal': weighted_mean('first_ing_is_corn_literal'),
            'first_ing_corn_literal_no_hfcs': weighted_mean('first_ing_corn_literal_no_hfcs'),
            'first_ing_corn_usual': weighted_mean('first_ing_is_corn_usual_or_literal'),
            'first_ing_corn_usual_no_hfcs': weighted_mean('first_ing_corn_usual_no_hfcs'),
            'first_ing_hfcs': weighted_mean('first_ing_is_hfcs_and_corn'),
        }
        results.append(year_results)

        print(f"{n_total:,} purchases, {int(total_quantity):,} units, HFCS: {year_results['first_ing_hfcs']:.2f}%")

        del df_year

        

    yearly_trends = pd.DataFrame(results).set_index('panel_year').sort_index()

    print("\nFirst ingredient trends (with/without HFCS):")
    print(yearly_trends[['first_ing_corn_literal', 'first_ing_corn_literal_no_hfcs', 'first_ing_hfcs']].round(2))

    # Save to cache
    if use_cache is not None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        yearly_trends.to_csv(HFCS_TRENDS_CACHE)
        print(f"Saved HFCS trends cache to: {HFCS_TRENDS_CACHE}")

    return yearly_trends


def plot_trends(yearly_trends, hfcs_trends=None):
    """Plot corn classification trends over time."""

    figures = []

    # Define labels for readability
    labels = {
        'first_ing_is_corn_literal': 'First ing. is corn (literal)',
        'first_ing_is_corn_usual_or_literal': 'First ing. is corn (usual/literal)',
        'any_ing_is_corn_literal': 'Any ing. is corn (literal)',
        'any_ing_is_corn_usual_or_literal': 'Any ing. is corn (usual/literal)',
        'any_ing_is_corn_any': 'Any ing. has any corn classification',
    }

    # Plot 1: First ingredient trends
    fig1, ax1 = plt.subplots(figsize=(12, 7))
    first_ing_vars = [v for v in yearly_trends.columns if 'first_ing' in v]
    for var in first_ing_vars:
        ax1.plot(yearly_trends.index, yearly_trends[var], marker='o', linewidth=2, label=labels.get(var, var))

    ax1.set_xlabel('Year')
    ax1.set_ylabel('% of Purchases')
    ax1.set_title('First Ingredient is Corn-Derived')
    ax1.legend(loc='best')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(yearly_trends.index)
    ax1.tick_params(axis='x', rotation=45)
    ax1.set_ylim(1, 4)
    plt.tight_layout()
    output_path_first = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/corn_trends_first_ingredient.png'
    plt.savefig(output_path_first, dpi=150, bbox_inches='tight')
    print(f"\nSaved plot to: {output_path_first}")
    plt.show()
    figures.append(fig1)

    # Plot 2: Any ingredient trends
    fig2, ax2 = plt.subplots(figsize=(12, 7))
    any_ing_vars = [v for v in yearly_trends.columns if 'any_ing' in v]
    for var in any_ing_vars:
        ax2.plot(yearly_trends.index, yearly_trends[var], marker='o', linewidth=2, label=labels.get(var, var))

    ax2.set_xlabel('Year')
    ax2.tick_params(axis='x', rotation=45)
    ax2.set_ylabel('% of Purchases')
    ax2.set_title('Any Ingredient is Corn-Derived')
    ax2.legend(loc='best')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(yearly_trends.index)
    plt.tight_layout()
    output_path_any = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/corn_trends_any_ingredient.png'
    plt.savefig(output_path_any, dpi=150, bbox_inches='tight')
    print(f"\nSaved plot to: {output_path_any}")
    plt.show()
    figures.append(fig2)

    # Plot 3: First ingredient excluding HFCS
    if hfcs_trends is not None:
        fig3, ax3 = plt.subplots(figsize=(12, 7))

        ax3.plot(hfcs_trends.index, hfcs_trends['first_ing_corn_literal'],
                 marker='o', linewidth=2, label='All corn (literal)', color='C0')
        ax3.plot(hfcs_trends.index, hfcs_trends['first_ing_corn_literal_no_hfcs'],
                 marker='s', linewidth=2, label='Excluding HFCS', color='C1')
        ax3.plot(hfcs_trends.index, hfcs_trends['first_ing_hfcs'],
                 marker='^', linewidth=2, label='HFCS only', color='C2', linestyle='--')

        ax3.set_xlabel('Year')
        ax3.tick_params(axis='x', rotation=45)
        ax3.set_ylabel('% of Purchases')
        ax3.set_title('First Ingredient is Corn (Literal)\nWith vs Without HFCS')
        ax3.legend(loc='best')
        ax3.grid(True, alpha=0.3)
        ax3.set_xticks(hfcs_trends.index)
        ax3.set_ylim(0, 4)
        plt.tight_layout()
        output_path_hfcs = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/corn_trends_first_ingredient_hfcs.png'
        plt.savefig(output_path_hfcs, dpi=150, bbox_inches='tight')
        print(f"\nSaved plot to: {output_path_hfcs}")
        plt.show()
        figures.append(fig3)

    return figures


def main():
    """Main function to compute trends and plot."""
    print("=" * 80)
    print("CORN-DERIVED FOOD TRENDS OVER TIME")
    print("=" * 80)
    print(f"USE_CACHE = {USE_CACHE}")

    # Years to process (only used when computing from raw data)
    # Set to None to process all available years, or specify a list:
    # years_to_process = None  # All years
    years_to_process = [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]

    # Compute trends (will use cache if USE_CACHE=True and cache exists)
    yearly_trends = compute_yearly_trends(years_to_process)

    if yearly_trends is None:
        print("ERROR: Could not compute yearly trends")
        return

    # Compute HFCS breakdown
    hfcs_trends = compute_yearly_trends_excluding_hfcs(years_to_process)

    # Plot trends
    plot_trends(yearly_trends, hfcs_trends)

    # Compute and plot trends by income
    print("\n" + "=" * 80)
    print("TRENDS BY HOUSEHOLD INCOME")
    print("=" * 80)
    income_trends = compute_trends_by_demographic('household_income', years_to_process=years_to_process)
    if income_trends is not None:
        # Map income codes to broader groups
        income_trends['income_group'] = income_trends['household_income'].map(map_income_to_group)

        # Aggregate by income group and year (weighted mean by n_purchases)
        corn_vars = ['first_ing_is_corn_literal', 'any_ing_is_corn_literal', 'any_ing_is_corn_usual_or_literal']

        def weighted_mean(group):
            total_purchases = group['n_purchases'].sum()
            result = {'n_purchases': total_purchases}
            for var in corn_vars:
                result[var] = (group[var] * group['n_purchases']).sum() / total_purchases
            return pd.Series(result)

        grouped = income_trends.groupby(['panel_year', 'income_group']).apply(weighted_mean).reset_index()

        # Plot three different corn variables
        plot_trends_by_demographic(
            grouped, 'income_group',
            corn_var='first_ing_is_corn_literal',
            title='First Ingredient is Corn (Literal) by Household Income',
            y_limits=(1, 3.5),
            output_path='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/income_trends_first_ing_literal.png'
        )
        plot_trends_by_demographic(
            grouped, 'income_group',
            corn_var='any_ing_is_corn_literal',
            title='Any Ingredient is Corn (Literal) by Household Income',
            y_limits=(0, 4),
            output_path='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/income_trends_any_ing_literal.png'
        )
        plot_trends_by_demographic(
            grouped, 'income_group',
            corn_var='any_ing_is_corn_usual_or_literal',
            title='Any Ingredient is Corn (Usual or Literal) by Household Income',
            y_limits=(0, 4),
            output_path='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/income_trends_any_ing_usual_or_literal.png'
        )

    # Compute and plot trends by product module (biggest changes)
    print("\n" + "=" * 80)
    print("TRENDS BY PRODUCT MODULE - BIGGEST CHANGES")
    print("=" * 80)
    module_trends = compute_trends_by_product_module(years_to_process=years_to_process)
    if module_trends is not None:
        plot_biggest_cornification_changes(
            module_trends,
            top_n=10,
            corn_var='any_ing_is_corn_usual_or_literal',
            start_year=2004,
            end_year=2020,
            output_path_increases_bar='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/cornification_increases_bar.png',
            output_path_increases_ts='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/cornification_increases_timeseries.png',
            output_path_decreases_bar='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/cornification_decreases_bar.png',
            output_path_decreases_ts='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/cornification_decreases_timeseries.png'
        )

    # Compute and plot balanced panel analysis (new products vs reformulation)
    # print("\n" + "=" * 80)
    # print("BALANCED PANEL ANALYSIS: NEW PRODUCTS VS REFORMULATION")
    # print("=" * 80)
    # balanced_trends = compute_balanced_panel_trends(start_year=2004, end_year=2020)
    # if balanced_trends is not None:
    #     plot_balanced_panel_comparison(
    #         balanced_trends,
    #         corn_var='any_ing_is_corn_usual_or_literal',
    #         output_path_trends='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/balanced_panel_trends.png',
    #         output_path_coverage='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/balanced_panel_coverage.png'
    #     )

    # Compute and plot expenditure-weighted and weight-based trends
    print("\n" + "=" * 80)
    print("EXPENDITURE AND WEIGHT-BASED CORNIFICATION ANALYSIS")
    print("=" * 80)

    # Expenditure-weighted trends
    expenditure_trends = compute_expenditure_weighted_trends(years_to_process=years_to_process, use_deflated=True)

    # Weight-based trends
    weight_trends = compute_weight_based_trends(years_to_process=years_to_process)

    # Household-level spending trends
    hh_spending_trends = compute_household_spending_trends(years_to_process=years_to_process, use_deflated=True)

    # Plot all three
    if expenditure_trends is not None or weight_trends is not None:
        plot_expenditure_and_weight_trends(
            expenditure_trends,
            weight_trends,
            hh_spending_trends,
            output_path_exp='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/cornification_expenditure_weighted.png',
            output_path_weight='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/cornification_weight_based.png',
            output_path_hh='/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/cornification_hh_spending.png'
        )

    print("\nDone!")


if __name__ == "__main__":
    main()
