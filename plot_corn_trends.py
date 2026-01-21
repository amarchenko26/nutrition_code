"""
Plot trends in corn-derived food consumption over time using Nielsen Consumer Panel data.

Processes each year's parquet file individually to avoid loading entire dataset into memory.
"""

import pandas as pd
import matplotlib.pyplot as plt
import os
import re
import tarfile
from glob import glob


# ============================================================================
# CACHE PATHS
# ============================================================================
CACHE_DIR = '/Users/anyamarchenko/Documents/GitHub/corn/analysis_output'
YEARLY_TRENDS_CACHE = os.path.join(CACHE_DIR, 'yearly_trends_cache.csv')
HFCS_TRENDS_CACHE = os.path.join(CACHE_DIR, 'hfcs_trends_cache.csv')
DEMOGRAPHIC_TRENDS_CACHE = os.path.join(CACHE_DIR, 'demographic_trends_cache.csv')

# ============================================================================
# DATA PATHS
# ============================================================================
NIELSEN_RAW_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/consumer_panel'
PURCHASES_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_with_corn_classification'


# HFCS variations to exclude
HFCS_PATTERNS = [
    'high fructose corn syrup',
    'high-fructose corn syrup',
    'hfcs',
    'corn syrup high fructose',
]


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


def load_panelists_for_year(year):
    """
    Load panelists (household demographics) data from Nielsen tarball for a given year.

    Parameters:
    -----------
    year : int
        Year to load panelists for

    Returns:
    --------
    DataFrame with household_code and demographic columns, or None if not found
    """
    tarball_path = f'{NIELSEN_RAW_PATH}/Consumer_Panel_Data_{year}.tgz'

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

        # Standardize column names to lowercase
        panelists_df.columns = panelists_df.columns.str.lower()

        return panelists_df


def compute_trends_by_demographic(demographic_var='household_income', years_to_process=None):
    """
    Compute yearly corn trends broken down by a demographic variable.

    This function loads panelists data on-the-fly from Nielsen tarballs and merges
    with the processed purchase data to compute trends by demographic group.

    Parameters:
    -----------
    demographic_var : str
        Column name of the demographic variable to group by (e.g., 'household_income',
        'race', 'hispanic_origin', 'household_size', 'region_code')
    years_to_process : list, optional
        List of years to include. If None, processes all available years.

    Returns:
    --------
    DataFrame with year, demographic group, and corn trend percentages
    """
    print(f"\n{'='*80}")
    print(f"COMPUTING CORN TRENDS BY {demographic_var.upper()}")
    print("="*80)

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
        panelists_df = load_panelists_for_year(year)
        if panelists_df is None:
            print(f"  Skipping year {year} - no panelists data")
            continue

        # Check if demographic variable exists
        if demographic_var not in panelists_df.columns:
            print(f"  WARNING: {demographic_var} not in panelists columns")
            print(f"  Available columns: {panelists_df.columns.tolist()[:20]}...")
            continue

        # Keep only needed columns from panelists
        panelists_subset = panelists_df[['household_code', demographic_var]].copy()

        # Load purchases for this year
        try:
            df_year = pd.read_parquet(year_dir, columns=['household_code'] + corn_vars)
        except Exception as e:
            print(f"  Error loading purchases: {e}")
            continue

        print(f"  Loaded {len(df_year):,} purchases, {len(panelists_subset):,} households")

        # Merge purchases with panelists
        df_merged = df_year.merge(panelists_subset, on='household_code', how='left')

        n_matched = df_merged[demographic_var].notna().sum()
        print(f"  Matched {n_matched:,} purchases ({n_matched/len(df_merged)*100:.1f}%)")

        # Compute means by demographic group
        grouped = df_merged.groupby(demographic_var)[corn_vars].mean() * 100
        grouped['n_purchases'] = df_merged.groupby(demographic_var).size()
        grouped['panel_year'] = year
        grouped = grouped.reset_index()

        results.append(grouped)

        # Free memory
        del df_year, df_merged, panelists_df

    if not results:
        print("ERROR: No data processed")
        return None

    # Combine all years
    trends_df = pd.concat(results, ignore_index=True)

    print(f"\n{'='*80}")
    print(f"SUMMARY: Trends by {demographic_var}")
    print("="*80)
    print(f"Total rows: {len(trends_df):,}")
    print(f"Years: {sorted(trends_df['panel_year'].unique())}")
    print(f"Demographic groups: {sorted(trends_df[demographic_var].unique())}")

    return trends_df


def plot_trends_by_demographic(trends_df, demographic_var, corn_var='any_ing_is_corn_usual_or_literal',
                                title=None, output_path=None):
    """
    Plot corn trends over time, with separate lines for each demographic group.

    Parameters:
    -----------
    trends_df : DataFrame
        Output from compute_trends_by_demographic()
    demographic_var : str
        Name of the demographic variable (for labeling)
    corn_var : str
        Which corn variable to plot
    title : str, optional
        Plot title (auto-generated if None)
    output_path : str, optional
        Path to save the figure
    """
    fig, ax = plt.subplots(figsize=(12, 7))

    # Get unique demographic groups
    groups = sorted(trends_df[demographic_var].dropna().unique())

    for group in groups:
        group_data = trends_df[trends_df[demographic_var] == group].sort_values('panel_year')
        ax.plot(group_data['panel_year'], group_data[corn_var],
                marker='o', linewidth=2, label=f'{group}')

    ax.set_xlabel('Year')
    ax.set_ylabel('% of Purchases')

    if title is None:
        title = f'Corn-Derived Food Purchases by {demographic_var.replace("_", " ").title()}'
    ax.set_title(title)

    ax.legend(loc='best', title=demographic_var.replace('_', ' ').title())
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis='x', rotation=45)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"\nSaved plot to: {output_path}")

    plt.show()

    return fig


def compute_yearly_trends(years_to_process=None):
    """
    Compute yearly averages for corn classification variables.

    Parameters:
    -----------
    years_to_process : list, optional
        List of years to include (e.g., [2004, 2005, 2006]).
        If None, processes all available years.

    Processes each year's partition individually to minimize memory usage.
    Returns a DataFrame with year as index and corn variables as columns.
    """
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
            df_year = pd.read_parquet(year_dir, columns=corn_vars)
        except Exception as e:
            print(f"Error: {e}")
            continue

        # Compute means for this year
        year_means = df_year.mean() * 100  # Convert to percentage
        year_means['panel_year'] = year
        year_means['n_purchases'] = len(df_year)
        results.append(year_means)

        print(f"{len(df_year):,} purchases")

        # Free memory
        del df_year

    # Combine into DataFrame
    yearly_trends = pd.DataFrame(results).set_index('panel_year').sort_index()

    print("\nYearly trends (% of purchases):")
    print(yearly_trends.drop(columns=['n_purchases']).round(2))

    return yearly_trends


def compute_yearly_trends_excluding_hfcs(years_to_process=None):
    """
    Compute yearly averages for first ingredient corn classification,
    excluding high fructose corn syrup.

    This requires loading the ingredients column and checking the first ingredient.
    """
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
            # Load corn vars plus ingredients column
            df_year = pd.read_parquet(year_dir, columns=[
                'first_ing_is_corn_literal',
                'first_ing_is_corn_usual_or_literal',
                'ingredients'
            ])
        except Exception as e:
            print(f"Error: {e}")
            continue

        n_total = len(df_year)

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

        year_results = {
            'panel_year': year,
            'n_purchases': n_total,
            'first_ing_corn_literal': df_year['first_ing_is_corn_literal'].mean() * 100,
            'first_ing_corn_literal_no_hfcs': df_year['first_ing_corn_literal_no_hfcs'].mean() * 100,
            'first_ing_corn_usual': df_year['first_ing_is_corn_usual_or_literal'].mean() * 100,
            'first_ing_corn_usual_no_hfcs': df_year['first_ing_corn_usual_no_hfcs'].mean() * 100,
            'first_ing_hfcs': df_year['first_ing_is_hfcs_and_corn'].mean() * 100,
        }
        results.append(year_results)

        print(f"{n_total:,} purchases, HFCS as first ing: {year_results['first_ing_hfcs']:.2f}%")

        del df_year

    yearly_trends = pd.DataFrame(results).set_index('panel_year').sort_index()

    print("\nFirst ingredient trends (with/without HFCS):")
    print(yearly_trends[['first_ing_corn_literal', 'first_ing_corn_literal_no_hfcs', 'first_ing_hfcs']].round(2))

    return yearly_trends


def save_trends_to_cache(yearly_trends, hfcs_trends):
    """Save computed trends to CSV cache files."""
    os.makedirs(CACHE_DIR, exist_ok=True)

    if yearly_trends is not None:
        yearly_trends.to_csv(YEARLY_TRENDS_CACHE)
        print(f"Saved yearly trends cache to: {YEARLY_TRENDS_CACHE}")

    if hfcs_trends is not None:
        hfcs_trends.to_csv(HFCS_TRENDS_CACHE)
        print(f"Saved HFCS trends cache to: {HFCS_TRENDS_CACHE}")


def load_trends_from_cache():
    """Load trends from CSV cache files."""
    yearly_trends = None
    hfcs_trends = None

    if os.path.exists(YEARLY_TRENDS_CACHE):
        yearly_trends = pd.read_csv(YEARLY_TRENDS_CACHE, index_col='panel_year')
        print(f"Loaded yearly trends from cache: {YEARLY_TRENDS_CACHE}")
    else:
        print(f"WARNING: Yearly trends cache not found: {YEARLY_TRENDS_CACHE}")

    if os.path.exists(HFCS_TRENDS_CACHE):
        hfcs_trends = pd.read_csv(HFCS_TRENDS_CACHE, index_col='panel_year')
        print(f"Loaded HFCS trends from cache: {HFCS_TRENDS_CACHE}")
    else:
        print(f"WARNING: HFCS trends cache not found: {HFCS_TRENDS_CACHE}")

    return yearly_trends, hfcs_trends


def plot_trends(yearly_trends, hfcs_trends=None):
    """Plot corn classification trends over time."""

    # Determine number of subplots
    n_plots = 3 if hfcs_trends is not None else 2
    fig, axes = plt.subplots(1, n_plots, figsize=(7 * n_plots, 6))

    # Define labels for readability
    labels = {
        'first_ing_is_corn_literal': 'First ing. is corn (literal)',
        'first_ing_is_corn_usual_or_literal': 'First ing. is corn (usual/literal)',
        'any_ing_is_corn_literal': 'Any ing. is corn (literal)',
        'any_ing_is_corn_usual_or_literal': 'Any ing. is corn (usual/literal)',
        'any_ing_is_corn_any': 'Any ing. has any corn classification',
    }

    # Plot 1: First ingredient trends
    ax1 = axes[0]
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

    # Plot 2: Any ingredient trends
    ax2 = axes[1]
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

    # Plot 3: First ingredient excluding HFCS
    if hfcs_trends is not None:
        ax3 = axes[2]

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

    plt.tight_layout()

    # Save figure
    output_path = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs/corn_trends_over_time.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"\nSaved plot to: {output_path}")

    plt.show()

    return fig


def main():
    """Main function to compute trends and plot."""
    print("=" * 80)
    print("CORN-DERIVED FOOD TRENDS OVER TIME")
    print("=" * 80)

    # ========================================================================
    # CONFIGURATION
    # ========================================================================
    # Set to True to recalculate from raw data (slow, 5-10 min)
    # Set to False to use cached data (fast, for tweaking plots)
    RECALCULATE_FROM_RAW = True

    # Specify which years to plot (only used if RECALCULATE_FROM_RAW = True)
    # Set to None to process all available years, or specify a list:
    # years_to_process = None  # All years
    years_to_process = [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
    # ========================================================================

    if RECALCULATE_FROM_RAW:
        print("\nRecalculating from raw data...")

        # Compute yearly trends (processes each year individually)
        yearly_trends = compute_yearly_trends(years_to_process)

        if yearly_trends is None:
            print("ERROR: Could not compute trends")
            return

        # Compute HFCS breakdown
        hfcs_trends = compute_yearly_trends_excluding_hfcs(years_to_process)

        # Save to cache for future runs
        save_trends_to_cache(yearly_trends, hfcs_trends)

    else:
        print("\nUsing cached data (set RECALCULATE_FROM_RAW = True to recalculate)")

        # Load from cache
        yearly_trends, hfcs_trends = load_trends_from_cache()

        if yearly_trends is None:
            print("ERROR: No cached data found. Set RECALCULATE_FROM_RAW = True")
            return

    # Plot trends
    plot_trends(yearly_trends, hfcs_trends)

    print("\nDone!")


if __name__ == "__main__":
    main()
