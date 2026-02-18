#!/usr/bin/env python3
"""
Analyze Product Proliferation Over Time

Tracks the number of unique products by category over time, distinguishing
between true new products vs. size/pack variants of existing products.

Approach for removing size variation:
1. Raw UPC count: Total unique UPCs per category per year
2. Normalized product count: Strip size/pack suffixes from upc_descr and count
   unique (brand_name, normalized_descr) combinations

This helps distinguish:
- Dasani 6-pack and Dasani 12-pack = 1 product (2 SKUs)
- Dasani and Smartwater = 2 products

NOTE: Generates TWO sets of graphs:
1. Raw Nielsen data (purchases_food) - no USDA merge, includes all products
2. USDA-Matched data (purchases_with_corn_classification) - only products with
   USDA ingredient matches (match rate increases over time mechanically)
"""

import os
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from glob import glob
from scipy import stats
from matplotlib.ticker import FuncFormatter


# ============================================================================
# CONFIGURATION
# ============================================================================
# Set to True to use sample data (faster iteration during development)
# Set to False to use full data (for production runs)
USE_SAMPLE = True

# Base paths
BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim'


def get_proliferation_paths():
    """Get input/output paths based on USE_SAMPLE setting."""
    suffix = '_sample' if USE_SAMPLE else ''
    return {
        'purchases_raw_path': os.path.join(BASE_DATA_DIR, f'purchases_food{suffix}'),
        'purchases_matched_path': os.path.join(BASE_DATA_DIR, f'purchases_with_corn_classification{suffix}'),
        'cache_dir_raw': os.path.join(BASE_DATA_DIR, f'purchases_food{suffix}', 'cache'),
        'cache_dir_matched': os.path.join(BASE_DATA_DIR, f'purchases_with_corn_classification{suffix}', 'cache'),
        'output_dir': '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs',
    }


# Initialize paths (will be updated when module loads or main() is called)
_paths = get_proliferation_paths()
PURCHASES_RAW_PATH = _paths['purchases_raw_path']
PURCHASES_MATCHED_PATH = _paths['purchases_matched_path']
CACHE_DIR_RAW = _paths['cache_dir_raw']
CACHE_DIR_MATCHED = _paths['cache_dir_matched']
OUTPUT_DIR = _paths['output_dir']

# Data source labels for plots
DATA_SOURCE_LABELS = {
    'raw': 'Raw Nielsen Data (All Products)',
    'matched': 'USDA-Matched Data'
}

USE_CACHE = True

# ============================================================================
# ULTRA-PROCESSED FOOD (UPF) CLASSIFICATION
# ============================================================================
# Keywords that indicate ultra-processed food categories
# Based on NOVA classification principles: industrial formulations with additives
UPF_KEYWORDS = [
    # Snacks
    'CHIP', 'SNACK', 'POPCORN', 'PRETZEL', 'CRACKER', 'PORK RIND',
    # Cookies, cakes, candy
    'COOKIE', 'CAKE', 'CANDY', 'CHOCOLATE', 'GUM', 'MINT', 'LICORICE',
    'MARSHMALLOW', 'CARAMEL', 'FUDGE', 'BROWNIE', 'PASTRY', 'DONUT',
    'DANISH', 'PIE', 'TART', 'MUFFIN',
    # Sweetened cereals and bars
    'CEREAL', 'GRANOLA BAR', 'HEALTH BAR', 'NUTRITION BAR', 'PROTEIN BAR',
    # Sweetened beverages
    'SOFT DRINK', 'SODA', 'CARBONATED', 'ENERGY DRINK', 'SPORT DRINK',
    'FRUIT DRINK', 'PUNCH', 'LEMONADE', 'ICED TEA', 'POWDERED DRINK',
    # Ready meals and processed meats
    'DINNER', 'ENTREE', 'FROZEN MEAL', 'PIZZA', 'HOT DOG', 'SAUSAGE',
    'BACON', 'DELI MEAT', 'LUNCH MEAT', 'BOLOGNA', 'SALAMI', 'HAM',
    'HOT POCKET', 'BURRITO', 'TAQUITO', 'CORN DOG',
    # Instant/processed foods
    'INSTANT', 'RAMEN', 'NOODLE SOUP', 'CUP SOUP', 'BOUILLON',
    'MAC.*CHEESE', 'MACARONI.*CHEESE', 'PASTA DINNER', 'RICE DINNER',
    'HELPER', 'HAMBURGER HELPER', 'TUNA HELPER',
    # Spreads and sauces (highly processed)
    'MAYONNAISE', 'SALAD DRESSING', 'WHIPPED TOPPING', 'COOL WHIP',
    'FROSTING', 'ICING',
    # Processed dairy
    'ICE CREAM', 'FROZEN DESSERT', 'PUDDING', 'GELATIN', 'JELLO',
    'CHEESE SPREAD', 'CHEESE DIP', 'VELVEETA', 'PROCESSED CHEESE',
    # Bread products (highly processed)
    'WHITE BREAD', 'HAMBURGER BUN', 'HOT DOG BUN', 'BREAD STICK',
    # Other UPF
    'NUGGET', 'FISH STICK', 'CHICKEN TENDER', 'BREADED',
    'TOASTER', 'POP TART', 'WAFFLE', 'PANCAKE MIX',
]

# Keywords that indicate less-processed/whole foods
NON_UPF_KEYWORDS = [
    # Fresh produce (usually not in Nielsen packaged goods, but just in case)
    'FRESH', 'PRODUCE', 'VEGETABLE', 'FRUIT',
    # Minimally processed dairy
    'MILK', 'BUTTER', 'CREAM', 'YOGURT', 'CHEESE',  # plain versions
    # Whole grains and legumes
    'RICE', 'BEAN', 'LENTIL', 'QUINOA', 'OATMEAL', 'FLOUR',
    # Plain proteins
    'EGG', 'BEEF', 'CHICKEN', 'PORK', 'TURKEY', 'FISH', 'SEAFOOD',
    # Canned/preserved basics
    'CANNED VEGETABLE', 'CANNED FRUIT', 'TOMATO', 'SAUCE',
    # Condiments and basics
    'OIL', 'VINEGAR', 'SPICE', 'SEASONING', 'HERB', 'SALT', 'SUGAR',
    # Beverages (less processed)
    'JUICE', 'WATER', 'COFFEE', 'TEA',
    # Baking basics
    'BAKING', 'YEAST',
]


def classify_upf(product_module_normalized):
    """
    Classify a product module as Ultra-Processed Food (UPF) or not.

    Parameters:
    -----------
    product_module_normalized : str
        The product module description

    Returns:
    --------
    str: 'UPF' for ultra-processed, 'Non-UPF' for less processed, 'Unknown' otherwise
    """
    if pd.isna(product_module_normalized) or not product_module_normalized:
        return 'Unknown'

    descr_upper = product_module_normalized.upper()

    # Check for UPF keywords first (more specific)
    for keyword in UPF_KEYWORDS:
        if re.search(keyword, descr_upper):
            return 'UPF'

    # Check for non-UPF keywords
    for keyword in NON_UPF_KEYWORDS:
        if re.search(keyword, descr_upper):
            return 'Non-UPF'

    return 'Unknown'


def get_paths_for_source(data_source):
    """
    Get the appropriate paths for the given data source.

    Parameters:
    -----------
    data_source : str
        Either 'raw' (Nielsen only) or 'matched' (USDA-merged)

    Returns:
    --------
    dict with keys: purchases_path, cache_dir, proliferation_cache, proliferation_normalized_cache
    """
    # Get current paths based on USE_SAMPLE setting
    current_paths = get_proliferation_paths()

    if data_source == 'raw':
        purchases_path = current_paths['purchases_raw_path']
        cache_dir = current_paths['cache_dir_raw']
    elif data_source == 'matched':
        purchases_path = current_paths['purchases_matched_path']
        cache_dir = current_paths['cache_dir_matched']
    else:
        raise ValueError(f"Invalid data_source: {data_source}. Must be 'raw' or 'matched'")

    return {
        'purchases_path': purchases_path,
        'cache_dir': cache_dir,
        'proliferation_cache': os.path.join(cache_dir, 'proliferation_by_module.csv'),
        'proliferation_normalized_cache': os.path.join(cache_dir, 'proliferation_normalized_by_module.csv'),
    }


def normalize_product_name(upc_descr):
    """
    Remove size/pack variation from product description to get base product.

    Examples:
    - "DS PFD NBP E/M 6P" -> "DS PFD NBP E/M"
    - "COKE CLS R CL CN FP 12P" -> "COKE CLS R CL CN FP"
    - "COCA-COLA R CL NB 6P" -> "COCA-COLA R CL NB"

    Returns:
    --------
    str: Normalized product description without size suffixes
    """
    if pd.isna(upc_descr) or not upc_descr:
        return upc_descr

    # Remove trailing pack size indicators (e.g., " 6P", " 12P", " 24P")
    # Pattern: space + number + P at end of string
    normalized = re.sub(r'\s+\d+P$', '', upc_descr)

    # Also remove other common size patterns
    # Pattern: space + number + common size units
    normalized = re.sub(r'\s+\d+(\.\d+)?\s*(OZ|LB|CT|PK|PC|ML|L|GAL|QT|PT)$', '', normalized, flags=re.IGNORECASE)

    return normalized.strip()


def set_year_axis(ax):
    """Format year axis ticks as integers (no decimals)."""
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x)}"))


def compute_proliferation_by_module(years_to_process=None, use_cache=None, data_source='matched'):
    """
    Compute the number of unique UPCs by product module for each year.

    This is the RAW measure - counts every UPC including size variants.

    Parameters:
    -----------
    years_to_process : list, optional
        List of years to process
    use_cache : bool, optional
        Whether to use cached data
    data_source : str
        'raw' for Nielsen-only data, 'matched' for USDA-merged data

    Returns:
    --------
    DataFrame with columns: panel_year, product_module_normalized, n_upcs, n_purchases
    """
    if use_cache is None:
        use_cache = USE_CACHE

    paths = get_paths_for_source(data_source)
    source_label = DATA_SOURCE_LABELS[data_source]

    print("=" * 80)
    print(f"COMPUTING PRODUCT PROLIFERATION BY MODULE (RAW UPC COUNT)")
    print(f"Data source: {source_label}")
    print("=" * 80)

    if use_cache and os.path.exists(paths['proliferation_cache']):
        print(f"Loading from cache: {paths['proliferation_cache']}")
        cached = pd.read_csv(paths['proliferation_cache'])
        if 'upf_category' not in cached.columns and 'product_module_normalized' in cached.columns:
            cached['upf_category'] = cached['product_module_normalized'].apply(classify_upf)
        return cached

    year_dirs = sorted(glob(os.path.join(paths['purchases_path'], 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {paths['purchases_path']}")
        return None

    print(f"Found {len(year_dirs)} year partitions")

    results = []

    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        if years_to_process is not None and year not in years_to_process:
            continue

        print(f"Processing {year}...", end=' ')

        try:
            df = pd.read_parquet(year_dir, columns=['upc', 'product_module_normalized', 'quantity'])
        except Exception as e:
            print(f"Error: {e}")
            continue

        # Count unique UPCs and total purchases per module
        module_stats = df.groupby('product_module_normalized').agg(
            n_upcs=('upc', 'nunique'),
            n_purchases=('upc', 'count'),
            n_units=('quantity', 'sum')
        ).reset_index()

        module_stats['panel_year'] = year

        # Add UPF classification
        module_stats['upf_category'] = module_stats['product_module_normalized'].apply(classify_upf)

        results.append(module_stats)

        total_upcs = df['upc'].nunique()
        print(f"{total_upcs:,} unique UPCs across {len(module_stats)} modules")

        del df

    proliferation_df = pd.concat(results, ignore_index=True)

    # Save to cache
    if use_cache:
        os.makedirs(paths['cache_dir'], exist_ok=True)
        proliferation_df.to_csv(paths['proliferation_cache'], index=False)
        print(f"\nSaved to: {paths['proliferation_cache']}")

    return proliferation_df


def compute_normalized_proliferation_by_module(years_to_process=None, use_cache=None, data_source='matched'):
    """
    Compute the number of unique PRODUCTS (not SKUs) by product module.

    Uses normalized product names that strip size/pack variations.
    A "product" is defined as unique (brand_name, normalized_upc_descr) combination.

    Parameters:
    -----------
    years_to_process : list, optional
        List of years to process
    use_cache : bool, optional
        Whether to use cached data
    data_source : str
        'raw' for Nielsen-only data, 'matched' for USDA-merged data

    Returns:
    --------
    DataFrame with columns: panel_year, product_module_normalized, n_upcs, n_products, n_purchases
    """
    if use_cache is None:
        use_cache = USE_CACHE

    paths = get_paths_for_source(data_source)
    source_label = DATA_SOURCE_LABELS[data_source]

    print("\n" + "=" * 80)
    print("COMPUTING NORMALIZED PRODUCT PROLIFERATION (SIZE-ADJUSTED)")
    print(f"Data source: {source_label}")
    print("=" * 80)

    if use_cache and os.path.exists(paths['proliferation_normalized_cache']):
        print(f"Loading from cache: {paths['proliferation_normalized_cache']}")
        cached = pd.read_csv(paths['proliferation_normalized_cache'])
        if 'upf_category' not in cached.columns and 'product_module_normalized' in cached.columns:
            cached['upf_category'] = cached['product_module_normalized'].apply(classify_upf)
        return cached

    year_dirs = sorted(glob(os.path.join(paths['purchases_path'], 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {paths['purchases_path']}")
        return None

    print(f"Found {len(year_dirs)} year partitions")

    results = []

    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        if years_to_process is not None and year not in years_to_process:
            continue

        print(f"Processing {year}...", end=' ')

        # For raw data, brand_name may not exist, so handle that
        cols_to_load = ['upc', 'upc_descr', 'product_module_normalized', 'quantity']
        if data_source == 'matched':
            cols_to_load.append('brand_name')

        try:
            df = pd.read_parquet(year_dir, columns=cols_to_load)
        except Exception as e:
            # Try without brand_name if it fails
            try:
                df = pd.read_parquet(year_dir, columns=['upc', 'upc_descr', 'product_module_normalized', 'quantity'])
            except Exception as e2:
                print(f"Error: {e2}")
                continue

        # Normalize product descriptions
        df['normalized_descr'] = df['upc_descr'].apply(normalize_product_name)

        # Create product identifier (brand + normalized description)
        # For raw data without brand_name, just use normalized description
        if 'brand_name' in df.columns:
            df['product_id'] = df['brand_name'].fillna('UNKNOWN') + '|' + df['normalized_descr'].fillna('')
        else:
            df['product_id'] = df['normalized_descr'].fillna('')

        # Count unique UPCs, unique products, and purchases per module
        module_stats = df.groupby('product_module_normalized').agg(
            n_upcs=('upc', 'nunique'),
            n_products=('product_id', 'nunique'),
            n_purchases=('upc', 'count'),
            n_units=('quantity', 'sum')
        ).reset_index()

        # Calculate SKUs per product ratio
        module_stats['skus_per_product'] = module_stats['n_upcs'] / module_stats['n_products']

        module_stats['panel_year'] = year

        # Add UPF classification
        module_stats['upf_category'] = module_stats['product_module_normalized'].apply(classify_upf)

        results.append(module_stats)

        total_upcs = df['upc'].nunique()
        total_products = df['product_id'].nunique()
        print(f"{total_upcs:,} UPCs -> {total_products:,} products ({total_upcs/total_products:.2f} SKUs/product)")

        del df

    proliferation_df = pd.concat(results, ignore_index=True)

    # Save to cache
    if use_cache:
        os.makedirs(paths['cache_dir'], exist_ok=True)
        proliferation_df.to_csv(paths['proliferation_normalized_cache'], index=False)
        print(f"\nSaved to: {paths['proliferation_normalized_cache']}")

    return proliferation_df


def compute_cornification_by_module(years_to_process=None):
    """
    Compute cornification rates by product module for correlation analysis.

    NOTE: Only available for 'matched' data source (requires USDA ingredient data).

    Returns:
    --------
    DataFrame with panel_year, product_module_normalized, corn_rate
    """
    print("\n" + "=" * 80)
    print("COMPUTING CORNIFICATION BY MODULE")
    print("(Only available for USDA-matched data)")
    print("=" * 80)

    paths = get_paths_for_source('matched')

    # Check if we already have this cached from plot_corn_trends.py
    module_cache = os.path.join(paths['cache_dir'], 'module_trends_cache.csv')
    if os.path.exists(module_cache):
        print(f"Loading from existing cache: {module_cache}")
        cached = pd.read_csv(module_cache)
        # The cache from plot_corn_trends.py uses 'first_ing_is_corn_usual_or_literal',
        # but this function's callers expect 'corn_rate'
        corn_col = 'first_ing_is_corn_usual_or_literal'
        if corn_col in cached.columns and 'corn_rate' not in cached.columns:
            cached = cached.rename(columns={corn_col: 'corn_rate'})
        return cached

    # Otherwise compute it
    year_dirs = sorted(glob(os.path.join(paths['purchases_path'], 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found")
        return None

    corn_var = 'first_ing_is_corn_usual_or_literal'
    results = []

    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        if years_to_process is not None and year not in years_to_process:
            continue

        print(f"Processing {year}...", end=' ')

        try:
            df = pd.read_parquet(year_dir, columns=['product_module_normalized', corn_var, 'quantity'])
        except Exception as e:
            print(f"Error: {e}")
            continue

        df['quantity'] = df['quantity'].fillna(1)

        # Compute quantity-weighted corn rate by module
        def weighted_corn_rate(group):
            total_qty = group['quantity'].sum()
            corn_qty = (group[corn_var] * group['quantity']).sum()
            return pd.Series({
                'corn_rate': corn_qty / total_qty * 100 if total_qty > 0 else 0,
                'n_units': total_qty
            })

        module_stats = df.groupby('product_module_normalized').apply(weighted_corn_rate).reset_index()
        module_stats['panel_year'] = year
        results.append(module_stats)

        print(f"{len(module_stats)} modules")

        del df

    corn_df = pd.concat(results, ignore_index=True)
    return corn_df


def plot_overall_proliferation(proliferation_df, normalized_df, output_path=None, data_source='matched'):
    """
    Plot total UPC count and product count over time (aggregate across all modules).

    Creates separate figures for:
    1. Total UPCs over time
    2. UPCs vs Products comparison

    Parameters:
    -----------
    proliferation_df : DataFrame
        Raw UPC count data
    normalized_df : DataFrame
        Normalized product count data
    output_path : str, optional
        Base path to save figures (will append _upcs.png and _products.png)
    data_source : str
        'raw' or 'matched' - used for labeling
    """
    source_label = DATA_SOURCE_LABELS[data_source]

    print("\n" + "=" * 80)
    print(f"PLOTTING OVERALL PRODUCT PROLIFERATION")
    print(f"Data source: {source_label}")
    print("=" * 80)

    # Aggregate to year level
    yearly_raw = proliferation_df.groupby('panel_year').agg(
        total_upcs=('n_upcs', 'sum'),
        total_purchases=('n_purchases', 'sum')
    ).reset_index()

    yearly_normalized = normalized_df.groupby('panel_year').agg(
        total_upcs=('n_upcs', 'sum'),
        total_products=('n_products', 'sum'),
        total_purchases=('n_purchases', 'sum')
    ).reset_index()

    # Calculate growth rates for summary
    start_upcs = yearly_raw['total_upcs'].iloc[0]
    end_upcs = yearly_raw['total_upcs'].iloc[-1]
    growth = (end_upcs - start_upcs) / start_upcs * 100

    start_products = yearly_normalized['total_products'].iloc[0]
    end_products = yearly_normalized['total_products'].iloc[-1]
    product_growth = (end_products - start_products) / start_products * 100

    # Figure 1: Total UPCs over time
    fig1 = plt.figure(figsize=(8, 6))
    plt.plot(yearly_raw['panel_year'], yearly_raw['total_upcs'] / 1000,
             marker='o', linewidth=2, color='#1f77b4')
    plt.xlabel('Year')
    plt.ylabel('Number of Unique UPCs (thousands)')
    plt.title(f'Total Unique UPCs\n({source_label})')
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    set_year_axis(plt.gca())
    plt.text(0.05, 0.95, f'Growth: {growth:+.1f}%', transform=plt.gca().transAxes,
             fontsize=10, verticalalignment='top')
    plt.tight_layout()

    if output_path:
        base_path = output_path.replace('.png', '')
        path1 = f"{base_path}_upcs.png"
        plt.savefig(path1, dpi=150, bbox_inches='tight')
        print(f"Saved to: {path1}")

    plt.show()

    # Figure 2: Products only (size-adjusted)
    fig2 = plt.figure(figsize=(8, 6))
    plt.plot(yearly_normalized['panel_year'], yearly_normalized['total_products'] / 1000,
             marker='o', linewidth=2, color='#ff7f0e')
    plt.xlabel('Year')
    plt.ylabel('Count (thousands)')
    plt.title(f'Unique Products (size-adjusted)\n({source_label})')
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    set_year_axis(plt.gca())
    plt.tight_layout()

    if output_path:
        base_path = output_path.replace('.png', '')
        path2 = f"{base_path}_products.png"
        plt.savefig(path2, dpi=150, bbox_inches='tight')
        print(f"Saved to: {path2}")

    plt.show()

    # Print summary
    print(f"\nSUMMARY ({source_label}):")
    print(f"Year range: {yearly_raw['panel_year'].min()} - {yearly_raw['panel_year'].max()}")
    print(f"UPCs: {start_upcs:,} -> {end_upcs:,} ({growth:+.1f}%)")
    print(f"Products: {start_products:,} -> {end_products:,} ({product_growth:+.1f}%)")

    avg_skus = yearly_normalized['total_upcs'].mean() / yearly_normalized['total_products'].mean()
    print(f"Average SKUs per product: {avg_skus:.2f}")

    return fig1, fig2


def plot_proliferation_by_top_modules(proliferation_df, normalized_df, top_n=10, output_path=None, data_source='matched'):
    """
    Plot UPC/product growth for the top N largest product modules.

    Creates separate figures for:
    1. Raw UPC count by module
    2. Normalized product count by module

    Parameters:
    -----------
    proliferation_df : DataFrame
        Raw UPC count data
    normalized_df : DataFrame
        Normalized product count data
    top_n : int
        Number of top modules to plot
    output_path : str, optional
        Base path to save figures (will append _upcs.png and _products.png)
    data_source : str
        'raw' or 'matched' - used for labeling
    """
    source_label = DATA_SOURCE_LABELS[data_source]

    print("\n" + "=" * 80)
    print(f"PLOTTING PROLIFERATION FOR TOP {top_n} MODULES")
    print(f"Data source: {source_label}")
    print("=" * 80)

    # Find top modules by total purchases
    top_modules = proliferation_df.groupby('product_module_normalized')['n_purchases'].sum() \
                                  .nlargest(top_n).index.tolist()

    print(f"Top {top_n} modules by purchase volume:")
    for i, m in enumerate(top_modules, 1):
        print(f"  {i}. {m}")

    # Filter to top modules
    top_raw = proliferation_df[proliferation_df['product_module_normalized'].isin(top_modules)]
    top_norm = normalized_df[normalized_df['product_module_normalized'].isin(top_modules)]

    # Figure 1: Raw UPC count by module
    fig1 = plt.figure(figsize=(10, 7))
    for module in top_modules:
        module_data = top_raw[top_raw['product_module_normalized'] == module].sort_values('panel_year')
        label = module[:30] + '...' if len(module) > 30 else module
        plt.plot(module_data['panel_year'], module_data['n_upcs'],
                 marker='o', linewidth=2, label=label)

    plt.xlabel('Year')
    plt.ylabel('Number of Unique UPCs')
    plt.title(f'UPC Count by Category (Top {top_n} Modules)\n({source_label})')
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize=8)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    set_year_axis(plt.gca())
    plt.tight_layout()

    if output_path:
        base_path = output_path.replace('.png', '')
        path1 = f"{base_path}_upcs.png"
        plt.savefig(path1, dpi=150, bbox_inches='tight')
        print(f"Saved to: {path1}")

    plt.show()

    # Figure 2: Normalized product count by module
    fig2 = plt.figure(figsize=(10, 7))
    for module in top_modules:
        module_data = top_norm[top_norm['product_module_normalized'] == module].sort_values('panel_year')
        label = module[:30] + '...' if len(module) > 30 else module
        plt.plot(module_data['panel_year'], module_data['n_products'],
                 marker='o', linewidth=2, label=label)

    plt.xlabel('Year')
    plt.ylabel('Number of Unique Products (size-adjusted)')
    plt.title(f'Product Count by Category (Top {top_n} Modules)\n({source_label})')
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize=8)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    set_year_axis(plt.gca())
    plt.tight_layout()

    if output_path:
        base_path = output_path.replace('.png', '')
        path2 = f"{base_path}_products.png"
        plt.savefig(path2, dpi=150, bbox_inches='tight')
        print(f"Saved to: {path2}")

    plt.show()

    return fig1, fig2


def plot_upf_vs_non_upf_proliferation(proliferation_df, value_col='n_upcs', output_path=None, data_source='matched'):
    """
    Plot UPF vs Non-UPF proliferation over time on the same figure.

    Parameters:
    -----------
    proliferation_df : DataFrame
        Output from compute_proliferation_by_module()
    value_col : str
        Column to aggregate (e.g., 'n_upcs' or 'n_products')
    output_path : str, optional
        Path to save figure
    data_source : str
        'raw' or 'matched' - used for labeling
    """
    source_label = DATA_SOURCE_LABELS[data_source]

    yearly = proliferation_df[proliferation_df['upf_category'].isin(['UPF', 'Non-UPF'])] \
        .groupby(['panel_year', 'upf_category'])[value_col].sum().reset_index()

    fig = plt.figure(figsize=(8, 6))
    for category, color in [('UPF', '#d62728'), ('Non-UPF', '#1f77b4')]:
        cat_data = yearly[yearly['upf_category'] == category]
        plt.plot(cat_data['panel_year'], cat_data[value_col] / 1000,
                 marker='o', linewidth=2, label=category, color=color)

    plt.xlabel('Year')
    plt.ylabel(f'{value_col.replace("_", " ").title()} (thousands)')
    plt.title(f'UPF vs Non-UPF Proliferation\n({source_label})')
    plt.legend(loc='best')
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    set_year_axis(plt.gca())
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved to: {output_path}")

    plt.show()

    return fig


def plot_upf_vs_non_upf_diff_over_time(proliferation_df, value_col='n_upcs', output_path=None, data_source='matched'):
    """
    Plot the difference in proliferation levels (UPF minus Non-UPF) over time.

    Parameters:
    -----------
    proliferation_df : DataFrame
        Output from compute_proliferation_by_module()
    value_col : str
        Column to aggregate (e.g., 'n_upcs' or 'n_products')
    output_path : str, optional
        Path to save figure
    data_source : str
        'raw' or 'matched' - used for labeling
    """
    source_label = DATA_SOURCE_LABELS[data_source]

    yearly = proliferation_df[proliferation_df['upf_category'].isin(['UPF', 'Non-UPF'])] \
        .groupby(['panel_year', 'upf_category'])[value_col].sum().reset_index()

    pivoted = yearly.pivot(index='panel_year', columns='upf_category', values=value_col).reset_index()
    pivoted['diff_upf_minus_nonupf'] = pivoted['UPF'] - pivoted['Non-UPF']

    fig = plt.figure(figsize=(8, 6))
    plt.plot(pivoted['panel_year'], pivoted['diff_upf_minus_nonupf'] / 1000,
             marker='o', linewidth=2, color='#9467bd')
    plt.xlabel('Year')
    plt.ylabel(f'UPF - Non-UPF {value_col.replace("_", " ").title()} (thousands)')
    plt.title(f'Difference in Proliferation: UPF minus Non-UPF\n({source_label})')
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.axhline(0, color='gray', linewidth=1, alpha=0.6)
    set_year_axis(plt.gca())
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved to: {output_path}")

    plt.show()

    return fig


def correlate_proliferation_with_cornification(normalized_df, corn_df, output_path=None, data_source='matched'):
    """
    Analyze the correlation between product proliferation and cornification.

    Questions:
    1. Do categories with more product growth have higher cornification?
    2. Within categories, does product growth correlate with cornification changes?

    NOTE: Cornification data is only available for 'matched' data source, but this
    function can be called with proliferation data from either source.

    Parameters:
    -----------
    normalized_df : DataFrame
        Normalized product count data (can be from raw or matched source)
    corn_df : DataFrame
        Cornification data (always from matched source)
    output_path : str, optional
        Path to save the figure
    data_source : str
        'raw' or 'matched' - used for labeling the proliferation data source
    """
    source_label = DATA_SOURCE_LABELS[data_source]

    print("\n" + "=" * 80)
    print("CORRELATING PROLIFERATION WITH CORNIFICATION")
    print(f"Proliferation data: {source_label}")
    print("Cornification data: USDA-Matched Data (required for ingredients)")
    print("=" * 80)

    # Merge proliferation and cornification data
    merged = normalized_df.merge(
        corn_df[['panel_year', 'product_module_normalized', 'corn_rate']],
        on=['panel_year', 'product_module_normalized'],
        how='inner'
    )

    print(f"Merged data: {len(merged):,} module-year observations")

    min_total_purchases = 1000 if USE_SAMPLE else 100000

    # Analysis 1: Cross-sectional correlation (average across years)
    module_avg = merged.groupby('product_module_normalized').agg(
        avg_products=('n_products', 'mean'),
        avg_corn_rate=('corn_rate', 'mean'),
        total_purchases=('n_purchases', 'sum')
    ).reset_index()

    # Filter to modules with sufficient data
    module_avg = module_avg[module_avg['total_purchases'] > min_total_purchases]

    if len(module_avg) < 2:
        corr_level, pval_level = np.nan, np.nan
        print("\nCross-sectional correlation (avg products vs avg corn rate):")
        print("  WARNING: Not enough data points to compute correlation (need >= 2).")
    else:
        corr_level, pval_level = stats.pearsonr(module_avg['avg_products'], module_avg['avg_corn_rate'])
        print(f"\nCross-sectional correlation (avg products vs avg corn rate):")
        print(f"  Pearson r = {corr_level:.3f} (p = {pval_level:.4f})")

    # Analysis 2: Growth correlation
    # Calculate growth in products and change in cornification per module
    def calc_changes(group):
        group = group.sort_values('panel_year')
        if len(group) < 2:
            return pd.Series({
                'product_growth': np.nan,
                'corn_change': np.nan,
                'total_purchases': group['n_purchases'].sum()
            })

        start_products = group['n_products'].iloc[0]
        end_products = group['n_products'].iloc[-1]
        product_growth = (end_products - start_products) / start_products * 100 if start_products > 0 else np.nan

        start_corn = group['corn_rate'].iloc[0]
        end_corn = group['corn_rate'].iloc[-1]
        corn_change = end_corn - start_corn

        return pd.Series({
            'product_growth': product_growth,
            'corn_change': corn_change,
            'total_purchases': group['n_purchases'].sum()
        })

    changes = merged.groupby('product_module_normalized').apply(calc_changes).reset_index()
    changes = changes.dropna()
    changes = changes[changes['total_purchases'] > min_total_purchases]  # Filter to substantial modules

    if len(changes) < 2:
        corr_growth, pval_growth = np.nan, np.nan
        print(f"\nGrowth correlation (product growth % vs corn rate change pp):")
        print("  WARNING: Not enough data points to compute correlation (need >= 2).")
    else:
        corr_growth, pval_growth = stats.pearsonr(changes['product_growth'], changes['corn_change'])
        print(f"\nGrowth correlation (product growth % vs corn rate change pp):")
        print(f"  Pearson r = {corr_growth:.3f} (p = {pval_growth:.4f})")

    # Figure 1: Cross-sectional (level)
    fig1 = plt.figure(figsize=(8, 6))
    plt.scatter(module_avg['avg_products'], module_avg['avg_corn_rate'],
                alpha=0.5, s=module_avg['total_purchases'] / 1e6)

    # Add regression line if we have enough points
    if len(module_avg) >= 2:
        z = np.polyfit(module_avg['avg_products'], module_avg['avg_corn_rate'], 1)
        p = np.poly1d(z)
        x_line = np.linspace(module_avg['avg_products'].min(), module_avg['avg_products'].max(), 100)
        plt.plot(x_line, p(x_line), 'r--', alpha=0.7, label=f'r = {corr_level:.3f}')

    plt.xlabel('Average Number of Products (size-adjusted)')
    plt.ylabel('Average Cornification Rate (%)')
    plt.title(f'Product Variety vs Cornification\n(Proliferation: {source_label})')
    if len(module_avg) >= 2:
        plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    if output_path:
        base_path = output_path.replace('.png', '')
        path1 = f"{base_path}_level.png"
        plt.savefig(path1, dpi=150, bbox_inches='tight')
        print(f"Saved to: {path1}")

    plt.show()

    # Figure 2: Growth correlation
    fig2 = plt.figure(figsize=(8, 6))
    plt.scatter(changes['product_growth'], changes['corn_change'],
                alpha=0.5, s=changes['total_purchases'] / 1e6)

    # Add regression line if we have enough points
    if len(changes) >= 2:
        z = np.polyfit(changes['product_growth'], changes['corn_change'], 1)
        p = np.poly1d(z)
        x_line = np.linspace(changes['product_growth'].min(), changes['product_growth'].max(), 100)
        plt.plot(x_line, p(x_line), 'r--', alpha=0.7, label=f'r = {corr_growth:.3f}')

    plt.xlabel('Product Growth (%)')
    plt.ylabel('Cornification Change (pp)')
    plt.title(f'Product Growth vs Cornification Change\n(Proliferation: {source_label})')
    plt.axhline(y=0, color='gray', linestyle='-', alpha=0.3)
    plt.axvline(x=0, color='gray', linestyle='-', alpha=0.3)
    if len(changes) >= 2:
        plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    if output_path:
        base_path = output_path.replace('.png', '')
        path2 = f"{base_path}_growth.png"
        plt.savefig(path2, dpi=150, bbox_inches='tight')
        print(f"Saved to: {path2}")

    plt.show()

    # Print top/bottom modules
    print(f"\nTop 5 modules with highest product growth:")
    top_growth = changes.nlargest(5, 'product_growth')
    for _, row in top_growth.iterrows():
        print(f"  {row['product_module_normalized'][:40]}: {row['product_growth']:+.1f}% growth, {row['corn_change']:+.1f}pp corn change")

    print(f"\nTop 5 modules with highest cornification increase:")
    top_corn = changes.nlargest(5, 'corn_change')
    for _, row in top_corn.iterrows():
        print(f"  {row['product_module_normalized'][:40]}: {row['corn_change']:+.1f}pp corn change, {row['product_growth']:+.1f}% product growth")

    return fig1, fig2, merged, changes


def run_analysis_for_source(data_source, years, corn_df=None):
    """
    Run proliferation analysis for a single data source.

    Parameters:
    -----------
    data_source : str
        'raw' or 'matched'
    years : list
        List of years to process
    corn_df : DataFrame, optional
        Cornification data (only available for matched source)

    Returns:
    --------
    tuple: (proliferation_df, normalized_df)
    """
    source_label = DATA_SOURCE_LABELS[data_source]
    suffix = 'raw' if data_source == 'raw' else 'matched'

    print("\n\n" + "#" * 80)
    print(f"# ANALYZING: {source_label}")
    print("#" * 80)

    # Compute raw proliferation (UPC counts)
    proliferation_df = compute_proliferation_by_module(years_to_process=years, data_source=data_source)
    if proliferation_df is None:
        print(f"ERROR: Could not compute proliferation data for {source_label}")
        return None, None

    # Compute normalized proliferation (product counts, size-adjusted)
    normalized_df = compute_normalized_proliferation_by_module(years_to_process=years, data_source=data_source)
    if normalized_df is None:
        print(f"ERROR: Could not compute normalized proliferation data for {source_label}")
        return None, None

    # Plot overall proliferation trends
    plot_overall_proliferation(
        proliferation_df, normalized_df,
        output_path=os.path.join(OUTPUT_DIR, f'proliferation_overall_{suffix}.png'),
        data_source=data_source
    )

    # Plot proliferation by top modules
    plot_proliferation_by_top_modules(
        proliferation_df, normalized_df, top_n=10,
        output_path=os.path.join(OUTPUT_DIR, f'proliferation_by_module_{suffix}.png'),
        data_source=data_source
    )

    # Plot UPF vs Non-UPF comparison
    plot_upf_vs_non_upf_proliferation(
        proliferation_df,
        value_col='n_upcs',
        output_path=os.path.join(OUTPUT_DIR, f'proliferation_upf_vs_nonupf_{suffix}.png'),
        data_source=data_source
    )

    # Plot UPF vs Non-UPF level difference over time
    plot_upf_vs_non_upf_diff_over_time(
        proliferation_df,
        value_col='n_upcs',
        output_path=os.path.join(OUTPUT_DIR, f'proliferation_upf_vs_nonupf_diff_{suffix}.png'),
        data_source=data_source
    )

    # Correlate proliferation with cornification (only if corn_df is available)
    if corn_df is not None:
        correlate_proliferation_with_cornification(
            normalized_df, corn_df,
            output_path=os.path.join(OUTPUT_DIR, f'proliferation_corn_correlation_{suffix}.png'),
            data_source=data_source
        )

    return proliferation_df, normalized_df


def main():
    """
    Main function to run proliferation analysis.

    Generates TWO sets of graphs:
    1. Raw Nielsen Data - all products in Nielsen, no USDA merge filtering
    2. USDA-Matched Data - only products matched to USDA ingredients database

    This addresses the issue that USDA match rates increase mechanically over time,
    so comparing raw vs. matched data helps identify true trends vs. artifacts.
    """
    global PURCHASES_RAW_PATH, PURCHASES_MATCHED_PATH, CACHE_DIR_RAW, CACHE_DIR_MATCHED, OUTPUT_DIR

    # Update paths based on USE_SAMPLE setting
    paths = get_proliferation_paths()
    PURCHASES_RAW_PATH = paths['purchases_raw_path']
    PURCHASES_MATCHED_PATH = paths['purchases_matched_path']
    CACHE_DIR_RAW = paths['cache_dir_raw']
    CACHE_DIR_MATCHED = paths['cache_dir_matched']
    OUTPUT_DIR = paths['output_dir']

    print("=" * 80)
    print("PRODUCT PROLIFERATION ANALYSIS")
    print("=" * 80)
    print(f"\nUSE_SAMPLE: {USE_SAMPLE}")
    print(f"Raw data path: {PURCHASES_RAW_PATH}")
    print(f"Matched data path: {PURCHASES_MATCHED_PATH}")
    print("\nThis analysis generates TWO sets of graphs:")
    print("  1. Raw Nielsen Data - all products, no USDA filtering")
    print("  2. USDA-Matched Data - only products with USDA ingredient matches")
    print("\nComparing these helps identify whether trends are real or artifacts")
    print("of improving USDA match rates over time.")

    # Years to process
    years = list(range(2004, 2025))  # 2004-2024

    # First, compute cornification by module (only available for matched data)
    # This will be used for correlation analysis with both data sources
    corn_df = compute_cornification_by_module(years_to_process=years)
    if corn_df is None:
        print("WARNING: Could not compute cornification data, skipping correlation analysis")

    # =========================================================================
    # ANALYSIS 1: Raw Nielsen Data (purchases_food)
    # =========================================================================
    raw_prolif, raw_norm = run_analysis_for_source('raw', years, corn_df)

    # =========================================================================
    # ANALYSIS 2: USDA-Matched Data (purchases_with_corn_classification)
    # =========================================================================
    matched_prolif, matched_norm = run_analysis_for_source('matched', years, corn_df)

    # =========================================================================
    # COMPARISON: Side-by-side summary
    # =========================================================================
    if raw_prolif is not None and matched_prolif is not None:
        print("\n\n" + "=" * 80)
        print("COMPARISON: RAW vs USDA-MATCHED DATA")
        print("=" * 80)

        # Aggregate to year level for both
        raw_yearly = raw_prolif.groupby('panel_year').agg(
            total_upcs=('n_upcs', 'sum'),
            total_purchases=('n_purchases', 'sum')
        ).reset_index()

        matched_yearly = matched_prolif.groupby('panel_year').agg(
            total_upcs=('n_upcs', 'sum'),
            total_purchases=('n_purchases', 'sum')
        ).reset_index()

        # Merge for comparison
        comparison = raw_yearly.merge(
            matched_yearly,
            on='panel_year',
            suffixes=('_raw', '_matched')
        )

        comparison['match_rate_upcs'] = comparison['total_upcs_matched'] / comparison['total_upcs_raw'] * 100
        comparison['match_rate_purchases'] = comparison['total_purchases_matched'] / comparison['total_purchases_raw'] * 100

        print("\nYear-by-year UPC match rates:")
        print("-" * 60)
        print(f"{'Year':<6} {'Raw UPCs':>12} {'Matched UPCs':>14} {'Match Rate':>12}")
        print("-" * 60)
        for _, row in comparison.iterrows():
            print(f"{int(row['panel_year']):<6} {int(row['total_upcs_raw']):>12,} "
                  f"{int(row['total_upcs_matched']):>14,} {row['match_rate_upcs']:>11.1f}%")

        print("-" * 60)
        print(f"\nKey insight: If match rate increases over time, proliferation")
        print(f"trends in matched data may be artificially inflated.")

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nOutput files saved to: {OUTPUT_DIR}")
    if corn_df is not None:
        print("  - proliferation_corn_correlation_raw.png (Raw proliferation vs cornification)")
        print("  - proliferation_corn_correlation_matched.png (Matched proliferation vs cornification)")


if __name__ == "__main__":
    main()
