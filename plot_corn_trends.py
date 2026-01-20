"""
Plot trends in corn-derived food consumption over time using Nielsen Consumer Panel data.

Processes each year's parquet file individually to avoid loading entire dataset into memory.
"""

import pandas as pd
import matplotlib.pyplot as plt
import os
from glob import glob


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
    data_path = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_with_corn_classification'

    corn_vars = [
        'first_ing_is_corn_literal',
        'first_ing_is_corn_usual_or_literal',
        'any_ing_is_corn_literal',
        'any_ing_is_corn_usual_or_literal',
        'any_ing_is_corn_any',
    ]

    # Find all year partitions
    year_dirs = sorted(glob(os.path.join(data_path, 'panel_year=*')))

    if not year_dirs:
        print(f"ERROR: No year partitions found in {data_path}")
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


def plot_trends(yearly_trends):
    """Plot corn classification trends over time."""

    # Create figure with two subplots
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

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

    plt.tight_layout()

    # Save figure
    output_path = '/Users/anyamarchenko/Documents/GitHub/corn/analysis_output/corn_trends_over_time.png'
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
    # CONFIGURATION: Specify which years to plot
    # ========================================================================
    # Set to None to process all available years, or specify a list:
    # years_to_process = None  # All years
    years_to_process = [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
    # ========================================================================

    # Compute yearly trends (processes each year individually)
    yearly_trends = compute_yearly_trends(years_to_process)

    if yearly_trends is None:
        print("ERROR: Could not compute trends")
        return

    # Plot trends
    plot_trends(yearly_trends)

    print("\nDone!")


if __name__ == "__main__":
    main()
