#!/usr/bin/env python3
"""
Analyze Ingredient Match Quality
Compares product distribution between all purchases and those matched to USDA ingredients
to identify any systematic bias in the matching process.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def analyze_year(year, purchases_dir, matched_dir):
    """
    Analyze match quality for a single year.

    Returns statistics comparing all purchases vs matched purchases.
    """
    print(f"\n{'='*60}")
    print(f"ANALYZING YEAR {year}")
    print("="*60)

    # Load all purchases (before USDA merge)
    all_path = os.path.join(purchases_dir, f'panel_year={year}')
    if not os.path.exists(all_path):
        print(f"  ERROR: All purchases not found: {all_path}")
        return None

    all_df = pd.read_parquet(all_path)
    print(f"  All purchases: {len(all_df):,}")

    # Load matched purchases (after USDA merge)
    matched_path = os.path.join(matched_dir, f'panel_year={year}', 'data.parquet')
    if not os.path.exists(matched_path):
        print(f"  ERROR: Matched purchases not found: {matched_path}")
        return None

    matched_df = pd.read_parquet(matched_path)
    print(f"  Matched purchases: {len(matched_df):,}")

    # Calculate match rate
    match_rate = len(matched_df) / len(all_df) * 100
    print(f"  Match rate: {match_rate:.2f}%")

    # Get product_module_descr distributions
    all_dist = all_df['product_module_descr'].value_counts(normalize=True)
    matched_dist = matched_df['product_module_descr'].value_counts(normalize=True)

    # Align distributions (some categories might only be in one)
    all_categories = set(all_dist.index) | set(matched_dist.index)

    comparison = pd.DataFrame({
        'all_pct': all_dist,
        'matched_pct': matched_dist
    }).fillna(0)

    comparison['diff'] = comparison['matched_pct'] - comparison['all_pct']
    comparison['abs_diff'] = comparison['diff'].abs()

    # Sort by absolute difference
    comparison = comparison.sort_values('abs_diff', ascending=False)

    print(f"\n  Top 10 categories with largest distribution shift:")
    print(f"  {'Category':<40} {'All %':>8} {'Match %':>8} {'Diff':>8}")
    print(f"  {'-'*64}")
    for cat, row in comparison.head(10).iterrows():
        print(f"  {cat[:40]:<40} {row['all_pct']*100:>7.2f}% {row['matched_pct']*100:>7.2f}% {row['diff']*100:>+7.2f}%")

    # Calculate summary statistics
    stats = {
        'year': year,
        'total_purchases': len(all_df),
        'matched_purchases': len(matched_df),
        'match_rate': match_rate,
        'n_categories_all': len(all_dist),
        'n_categories_matched': len(matched_dist),
        'mean_abs_diff': comparison['abs_diff'].mean() * 100,
        'max_abs_diff': comparison['abs_diff'].max() * 100,
        'top_overrep_category': comparison['diff'].idxmax(),
        'top_overrep_diff': comparison['diff'].max() * 100,
        'top_underrep_category': comparison['diff'].idxmin(),
        'top_underrep_diff': comparison['diff'].min() * 100,
    }

    # Free memory
    del all_df, matched_df

    return stats, comparison


def analyze_category_match_rates(year, purchases_dir, matched_dir):
    """
    Calculate match rate by product category for a single year.
    """
    # Load all purchases
    all_path = os.path.join(purchases_dir, f'panel_year={year}')
    all_df = pd.read_parquet(all_path, columns=['upc', 'product_module_descr'])

    # Load matched purchases
    matched_path = os.path.join(matched_dir, f'panel_year={year}', 'data.parquet')
    matched_df = pd.read_parquet(matched_path, columns=['upc', 'product_module_descr'])

    # Count by category
    all_counts = all_df.groupby('product_module_descr').size()
    matched_counts = matched_df.groupby('product_module_descr').size()

    # Calculate match rate per category
    category_stats = pd.DataFrame({
        'all_count': all_counts,
        'matched_count': matched_counts
    }).fillna(0)

    category_stats['match_rate'] = category_stats['matched_count'] / category_stats['all_count'] * 100
    category_stats['year'] = year

    del all_df, matched_df

    return category_stats


def main():
    """
    Main function to analyze ingredient match quality across all years.
    """
    print("INGREDIENT MATCH QUALITY ANALYSIS")
    print("="*80)

    # Paths
    purchases_dir = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_food'
    matched_dir = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_with_ingredients'
    output_dir = '/Users/anyamarchenko/Documents/GitHub/corn/analysis_output'

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Years to analyze
    years = list(range(2004, 2024))

    # Collect statistics across years
    all_stats = []
    all_category_stats = []

    for year in years:
        result = analyze_year(year, purchases_dir, matched_dir)
        if result:
            stats, comparison = result
            all_stats.append(stats)

            # Also get category-level match rates
            cat_stats = analyze_category_match_rates(year, purchases_dir, matched_dir)
            all_category_stats.append(cat_stats)

    if not all_stats:
        print("\nERROR: No data was successfully analyzed")
        return

    # Create summary dataframe
    summary_df = pd.DataFrame(all_stats)

    # =========================================================================
    # TABLE 1: Year-by-year summary
    # =========================================================================
    print("\n\n" + "="*80)
    print("TABLE 1: MATCH RATE SUMMARY BY YEAR")
    print("="*80)

    print(f"\n{'Year':<6} {'Total':>12} {'Matched':>12} {'Rate':>8} {'Mean Diff':>10} {'Max Diff':>10}")
    print("-"*60)

    for _, row in summary_df.iterrows():
        print(f"{int(row['year']):<6} {int(row['total_purchases']):>12,} {int(row['matched_purchases']):>12,} "
              f"{row['match_rate']:>7.1f}% {row['mean_abs_diff']:>9.2f}% {row['max_abs_diff']:>9.2f}%")

    print("-"*60)
    print(f"{'Avg':<6} {'':<12} {'':<12} {summary_df['match_rate'].mean():>7.1f}% "
          f"{summary_df['mean_abs_diff'].mean():>9.2f}% {summary_df['max_abs_diff'].mean():>9.2f}%")

    # Save table
    summary_df.to_csv(os.path.join(output_dir, 'match_rate_summary_by_year.csv'), index=False)

    # =========================================================================
    # TABLE 2: Most over/under-represented categories
    # =========================================================================
    print("\n\n" + "="*80)
    print("TABLE 2: CATEGORIES WITH LARGEST REPRESENTATION SHIFTS")
    print("="*80)

    print("\nOver-represented in matched data (higher % after matching):")
    print(f"{'Year':<6} {'Category':<45} {'Diff':>8}")
    print("-"*60)
    for _, row in summary_df.iterrows():
        print(f"{int(row['year']):<6} {row['top_overrep_category'][:45]:<45} {row['top_overrep_diff']:>+7.2f}%")

    print("\n\nUnder-represented in matched data (lower % after matching):")
    print(f"{'Year':<6} {'Category':<45} {'Diff':>8}")
    print("-"*60)
    for _, row in summary_df.iterrows():
        print(f"{int(row['year']):<6} {row['top_underrep_category'][:45]:<45} {row['top_underrep_diff']:>+7.2f}%")

    # =========================================================================
    # Combine category stats across years
    # =========================================================================
    combined_cat_stats = pd.concat(all_category_stats, ignore_index=False)
    combined_cat_stats = combined_cat_stats.reset_index().rename(columns={'index': 'product_module_descr'})

    # =========================================================================
    # TABLE 3: Categories with lowest match rates (averaged across years)
    # =========================================================================
    print("\n\n" + "="*80)
    print("TABLE 3: CATEGORIES WITH LOWEST MATCH RATES (averaged across years)")
    print("="*80)

    # Average match rate by category
    avg_match_by_cat = combined_cat_stats.groupby('product_module_descr').agg({
        'all_count': 'sum',
        'matched_count': 'sum',
        'match_rate': 'mean'
    })
    avg_match_by_cat['overall_match_rate'] = avg_match_by_cat['matched_count'] / avg_match_by_cat['all_count'] * 100
    avg_match_by_cat = avg_match_by_cat.sort_values('overall_match_rate')

    # Filter to categories with at least 10,000 total purchases
    significant_cats = avg_match_by_cat[avg_match_by_cat['all_count'] >= 10000]

    print(f"\n{'Category':<45} {'Total':>12} {'Matched':>12} {'Rate':>8}")
    print("-"*80)
    for cat, row in significant_cats.head(20).iterrows():
        print(f"{cat[:45]:<45} {int(row['all_count']):>12,} {int(row['matched_count']):>12,} {row['overall_match_rate']:>7.1f}%")

    print("\n\nCategories with HIGHEST match rates:")
    print(f"\n{'Category':<45} {'Total':>12} {'Matched':>12} {'Rate':>8}")
    print("-"*80)
    for cat, row in significant_cats.tail(20).iloc[::-1].iterrows():
        print(f"{cat[:45]:<45} {int(row['all_count']):>12,} {int(row['matched_count']):>12,} {row['overall_match_rate']:>7.1f}%")

    # Save category stats
    avg_match_by_cat.to_csv(os.path.join(output_dir, 'match_rate_by_category.csv'))

    # =========================================================================
    # FIGURE 1: Match rate over time
    # =========================================================================
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Plot 1: Overall match rate by year
    ax1 = axes[0, 0]
    ax1.plot(summary_df['year'], summary_df['match_rate'], 'b-o', linewidth=2, markersize=6)
    ax1.set_xlabel('Year')
    ax1.set_ylabel('Match Rate (%)')
    ax1.set_title('Overall Match Rate by Year')
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(0, 100)

    # Plot 2: Mean absolute distribution difference by year
    ax2 = axes[0, 1]
    ax2.plot(summary_df['year'], summary_df['mean_abs_diff'], 'r-o', linewidth=2, markersize=6)
    ax2.set_xlabel('Year')
    ax2.set_ylabel('Mean Abs. Difference (%)')
    ax2.set_title('Mean Category Distribution Shift by Year')
    ax2.grid(True, alpha=0.3)

    # Plot 3: Distribution of match rates by category
    ax3 = axes[1, 0]
    ax3.hist(significant_cats['overall_match_rate'], bins=30, edgecolor='black', alpha=0.7)
    ax3.set_xlabel('Match Rate (%)')
    ax3.set_ylabel('Number of Categories')
    ax3.set_title('Distribution of Match Rates Across Categories')
    ax3.axvline(significant_cats['overall_match_rate'].median(), color='r', linestyle='--',
                label=f'Median: {significant_cats["overall_match_rate"].median():.1f}%')
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # Plot 4: Top 10 lowest match rate categories
    ax4 = axes[1, 1]
    bottom_10 = significant_cats.head(10)
    y_pos = range(len(bottom_10))
    ax4.barh(y_pos, bottom_10['overall_match_rate'], color='coral', edgecolor='black')
    ax4.set_yticks(y_pos)
    ax4.set_yticklabels([cat[:35] + '...' if len(cat) > 35 else cat for cat in bottom_10.index])
    ax4.set_xlabel('Match Rate (%)')
    ax4.set_title('Categories with Lowest Match Rates')
    ax4.grid(True, alpha=0.3, axis='x')

    plt.tight_layout()
    fig_path = os.path.join(output_dir, 'ingredient_match_analysis.png')
    plt.savefig(fig_path, dpi=150, bbox_inches='tight')
    print(f"\n\n✓ Figure saved to: {fig_path}")

    # =========================================================================
    # FIGURE 2: Match rate trends for select categories
    # =========================================================================
    fig2, ax = plt.subplots(figsize=(14, 8))

    # Select some interesting categories to track over time
    interesting_cats = [
        'SOFT DRINKS - CARBONATED',
        'CEREAL - READY TO EAT',
        'COOKIES',
        'CANDY-CHOCOLATE',
        'DAIRY-MILK-REFRIGERATED',
        'BAKERY - BREAD - FRESH',
        'SOUP-CANNED',
        'YOGURT-REFRIGERATED'
    ]

    for cat in interesting_cats:
        cat_data = combined_cat_stats[combined_cat_stats['product_module_descr'] == cat]
        if len(cat_data) > 0:
            ax.plot(cat_data['year'], cat_data['match_rate'], '-o', label=cat[:30], linewidth=2, markersize=4)

    ax.set_xlabel('Year')
    ax.set_ylabel('Match Rate (%)')
    ax.set_title('Match Rate Over Time for Selected Product Categories')
    ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left')
    ax.grid(True, alpha=0.3)
    ax.set_ylim(0, 100)

    plt.tight_layout()
    fig2_path = os.path.join(output_dir, 'match_rate_by_category_over_time.png')
    plt.savefig(fig2_path, dpi=150, bbox_inches='tight')
    print(f"✓ Figure saved to: {fig2_path}")

    plt.close('all')

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nOverall match rate: {summary_df['match_rate'].mean():.1f}% (range: {summary_df['match_rate'].min():.1f}% - {summary_df['match_rate'].max():.1f}%)")
    print(f"Mean category distribution shift: {summary_df['mean_abs_diff'].mean():.2f}%")
    print(f"Categories analyzed: {len(significant_cats):,}")
    print(f"\nOutput files saved to: {output_dir}")
    print(f"  - match_rate_summary_by_year.csv")
    print(f"  - match_rate_by_category.csv")
    print(f"  - ingredient_match_analysis.png")
    print(f"  - match_rate_by_category_over_time.png")


if __name__ == "__main__":
    main()
