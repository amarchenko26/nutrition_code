#!/usr/bin/env python3
"""
Analyze Correlation Between Household Ailments and Cornification

This script:
1. Loads ailments data (dietary diseases by household)
2. Loads CPI deflator to compute real prices
3. Computes household-level cornification measures:
   - Share of expenditure on corn products
   - Share of weight/quantity on corn products
4. Merges with ailments data
5. Computes correlations between ailments and cornification

Addresses mechanical issues with % of purchases metric by using:
- Real (deflated) expenditure shares
- Weight-based measures
"""

import os
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt


# ============================================================================
# PATHS
# ============================================================================
PURCHASES_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_with_corn_classification'
AILMENTS_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/ailments/dietary_ailments_by_household.parquet'
CPI_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/price_deflator/CPIEBEV.csv'
OUTPUT_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/ailments_corn_analysis'
FIGS_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/farm bill/figs'


def load_cpi_deflator():
    """
    Load CPI deflator and create monthly lookup.

    Returns:
    --------
    dict : Mapping of (year, month) -> CPI value
    float : Base CPI value (we'll use December 2019 as base)
    """
    print("Loading CPI deflator...")
    cpi_df = pd.read_csv(CPI_PATH)

    # Parse date
    cpi_df['observation_date'] = pd.to_datetime(cpi_df['observation_date'])
    cpi_df['year'] = cpi_df['observation_date'].dt.year
    cpi_df['month'] = cpi_df['observation_date'].dt.month

    # Handle missing values
    cpi_df = cpi_df.dropna(subset=['CPIEBEV'])

    # Create lookup dictionary
    cpi_lookup = {}
    for _, row in cpi_df.iterrows():
        cpi_lookup[(row['year'], row['month'])] = row['CPIEBEV']

    # Use December 2019 as base (CPI = 100 equivalent)
    base_cpi = cpi_lookup.get((2019, 12), 267.558)  # Dec 2019 value

    print(f"  Loaded {len(cpi_lookup)} monthly CPI values")
    print(f"  Base CPI (Dec 2019): {base_cpi:.3f}")
    print(f"  Range: {min(cpi_df['CPIEBEV']):.1f} to {max(cpi_df['CPIEBEV']):.1f}")

    return cpi_lookup, base_cpi


def deflate_price(price, year, month, cpi_lookup, base_cpi):
    """
    Deflate a nominal price to real price (base = Dec 2019).

    Parameters:
    -----------
    price : float
        Nominal price
    year : int
        Year of purchase
    month : int
        Month of purchase
    cpi_lookup : dict
        CPI values by (year, month)
    base_cpi : float
        Base CPI value

    Returns:
    --------
    float : Real price
    """
    cpi = cpi_lookup.get((year, month))
    if cpi is None:
        # Try to find closest available
        for delta in range(1, 12):
            for m in [month - delta, month + delta]:
                if 1 <= m <= 12:
                    cpi = cpi_lookup.get((year, m))
                    if cpi:
                        break
            if cpi:
                break

    if cpi is None or cpi == 0:
        return price  # Return nominal if can't deflate

    return price * (base_cpi / cpi)


def compute_household_cornification(year, cpi_lookup, base_cpi):
    """
    Compute household-level cornification measures for a given year.

    Computes:
    - real_total_price_paid: Deflated price of each item
    - real_total_spent: Deflated total trip spending
    - corn_share_trip: Share of trip spending on corn products
    - Annual aggregates by household

    Parameters:
    -----------
    year : int
        Year to process
    cpi_lookup : dict
        CPI lookup dictionary
    base_cpi : float
        Base CPI value

    Returns:
    --------
    DataFrame with household-year level cornification measures
    """
    year_path = os.path.join(PURCHASES_PATH, f'panel_year={year}', 'data.parquet')

    if not os.path.exists(year_path):
        print(f"  Year {year}: File not found")
        return None

    print(f"  Processing {year}...")

    # Load data
    cols_needed = [
        'household_code', 'trip_code_uc', 'purchase_date',
        'total_price_paid', 'total_spent', 'quantity',
        'size1_amount', 'size1_units',
        'any_ing_is_corn_usual_or_literal', 'any_ing_is_corn_literal'
    ]

    df = pd.read_parquet(year_path, columns=cols_needed)
    print(f"    Loaded {len(df):,} purchases")

    # Parse purchase date
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])
    df['month'] = df['purchase_date'].dt.month

    # Deflate prices
    print(f"    Deflating prices...")
    df['real_price_paid'] = df.apply(
        lambda row: deflate_price(row['total_price_paid'], year, row['month'], cpi_lookup, base_cpi),
        axis=1
    )
    df['real_total_spent'] = df.apply(
        lambda row: deflate_price(row['total_spent'], year, row['month'], cpi_lookup, base_cpi),
        axis=1
    )

    # Compute weight in standardized units (convert to ounces where possible)
    # Common units: OZ, LB, CT, FL OZ, GAL, PT, QT
    def standardize_weight(amount, units):
        """Convert to ounces where possible."""
        if pd.isna(amount) or pd.isna(units):
            return np.nan

        units = str(units).upper().strip()
        amount = float(amount)

        # Weight conversions to ounces
        if units in ['OZ', 'FL OZ']:
            return amount
        elif units == 'LB':
            return amount * 16
        elif units == 'GAL':
            return amount * 128  # fluid ounces
        elif units == 'QT':
            return amount * 32
        elif units == 'PT':
            return amount * 16
        elif units == 'CT':
            return amount  # Count - keep as is
        else:
            return np.nan

    df['weight_oz'] = df.apply(
        lambda row: standardize_weight(row['size1_amount'], row['size1_units']),
        axis=1
    )
    df['total_weight'] = df['weight_oz'] * df['quantity']

    # Corn indicator
    df['is_corn'] = df['any_ing_is_corn_usual_or_literal'].fillna(False).astype(int)
    df['is_corn_literal'] = df['any_ing_is_corn_literal'].fillna(False).astype(int)

    # Compute trip-level corn share
    trip_stats = df.groupby(['household_code', 'trip_code_uc']).agg({
        'real_price_paid': 'sum',
        'real_total_spent': 'first',  # Same for all items in trip
        'total_weight': 'sum',
        'is_corn': 'sum',  # Number of corn items
        'quantity': 'sum',  # Total items
    }).reset_index()

    trip_stats.columns = ['household_code', 'trip_code_uc', 'trip_corn_spending',
                          'trip_total_spent', 'trip_total_weight', 'trip_corn_items', 'trip_total_items']

    # Compute corn spending per trip (corn items only)
    corn_spending = df[df['is_corn'] == 1].groupby(['household_code', 'trip_code_uc'])['real_price_paid'].sum().reset_index()
    corn_spending.columns = ['household_code', 'trip_code_uc', 'trip_corn_spending']

    corn_weight = df[df['is_corn'] == 1].groupby(['household_code', 'trip_code_uc'])['total_weight'].sum().reset_index()
    corn_weight.columns = ['household_code', 'trip_code_uc', 'trip_corn_weight']

    # Merge
    trip_stats = trip_stats.merge(corn_spending, on=['household_code', 'trip_code_uc'], how='left', suffixes=('', '_y'))
    trip_stats['trip_corn_spending'] = trip_stats['trip_corn_spending_y'].fillna(0)
    trip_stats = trip_stats.drop(columns=['trip_corn_spending_y'], errors='ignore')

    trip_stats = trip_stats.merge(corn_weight, on=['household_code', 'trip_code_uc'], how='left')
    trip_stats['trip_corn_weight'] = trip_stats['trip_corn_weight'].fillna(0)

    # Compute shares per trip
    trip_stats['corn_share_spending'] = trip_stats['trip_corn_spending'] / trip_stats['trip_total_spent'].replace(0, np.nan)
    trip_stats['corn_share_weight'] = trip_stats['trip_corn_weight'] / trip_stats['trip_total_weight'].replace(0, np.nan)
    trip_stats['corn_share_items'] = trip_stats['trip_corn_items'] / trip_stats['trip_total_items'].replace(0, np.nan)

    # Aggregate to household-year level
    hh_stats = trip_stats.groupby('household_code').agg({
        'trip_corn_spending': 'sum',
        'trip_total_spent': 'sum',
        'trip_corn_weight': 'sum',
        'trip_total_weight': 'sum',
        'trip_corn_items': 'sum',
        'trip_total_items': 'sum',
        'trip_code_uc': 'nunique',  # Number of trips
    }).reset_index()

    hh_stats.columns = ['household_code', 'annual_corn_spending', 'annual_total_spending',
                        'annual_corn_weight', 'annual_total_weight',
                        'annual_corn_items', 'annual_total_items', 'n_trips']

    # Compute annual shares
    hh_stats['corn_share_spending'] = hh_stats['annual_corn_spending'] / hh_stats['annual_total_spending'].replace(0, np.nan)
    hh_stats['corn_share_weight'] = hh_stats['annual_corn_weight'] / hh_stats['annual_total_weight'].replace(0, np.nan)
    hh_stats['corn_share_items'] = hh_stats['annual_corn_items'] / hh_stats['annual_total_items'].replace(0, np.nan)

    hh_stats['year'] = year

    print(f"    {len(hh_stats):,} households")
    print(f"    Mean corn share (spending): {hh_stats['corn_share_spending'].mean()*100:.1f}%")
    print(f"    Mean corn share (weight): {hh_stats['corn_share_weight'].mean()*100:.1f}%")
    print(f"    Mean corn share (items): {hh_stats['corn_share_items'].mean()*100:.1f}%")

    return hh_stats


def main():
    """Main function to analyze ailments-cornification correlation."""
    print("="*80)
    print("ANALYZING AILMENTS AND CORNIFICATION CORRELATION")
    print("="*80)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load CPI deflator
    cpi_lookup, base_cpi = load_cpi_deflator()

    # Load ailments data
    print("\nLoading ailments data...")
    ailments_df = pd.read_parquet(AILMENTS_PATH)
    print(f"  {len(ailments_df):,} household-year observations")
    print(f"  Years: {sorted(ailments_df['survey_year'].unique())}")

    # Compute household cornification for relevant years (where we have ailments data)
    ailment_years = sorted(ailments_df['survey_year'].unique())

    # Also compute for a few years before/after for comparison
    years_to_process = list(range(2011, 2022))

    print("\nComputing household-level cornification...")
    all_hh_stats = []

    for year in years_to_process:
        result = compute_household_cornification(year, cpi_lookup, base_cpi)
        if result is not None:
            all_hh_stats.append(result)

    hh_corn_df = pd.concat(all_hh_stats, ignore_index=True)
    print(f"\nTotal household-year observations: {len(hh_corn_df):,}")

    # Save household cornification data
    corn_path = os.path.join(OUTPUT_DIR, 'household_cornification.parquet')
    hh_corn_df.to_parquet(corn_path, index=False)
    print(f"Saved to: {corn_path}")

    # Merge with ailments
    print("\nMerging with ailments data...")

    # Ailments are surveyed at specific times, so match to same year or closest year
    merged_df = hh_corn_df.merge(
        ailments_df,
        left_on=['household_code', 'year'],
        right_on=['household_id', 'survey_year'],
        how='inner'
    )
    print(f"  Matched: {len(merged_df):,} household-year observations")
    print(f"  Unique households: {merged_df['household_code'].nunique():,}")

    # Save merged data
    merged_path = os.path.join(OUTPUT_DIR, 'household_ailments_corn_merged.parquet')
    merged_df.to_parquet(merged_path, index=False)
    print(f"Saved to: {merged_path}")

    # Compute correlations
    print("\n" + "="*80)
    print("CORRELATION ANALYSIS")
    print("="*80)

    corn_measures = ['corn_share_spending', 'corn_share_weight', 'corn_share_items']
    ailment_cols = ['obesity', 'diabetes_type2', 'hypertension', 'cholesterol',
                    'heart_disease', 'any_diabetes', 'any_metabolic_disease', 'n_dietary_conditions']

    # Filter to valid observations
    valid_df = merged_df.dropna(subset=corn_measures)

    print("\nCorrelation coefficients (Pearson):")
    print("-" * 80)
    print(f"{'Ailment':<25} {'Spending':>12} {'Weight':>12} {'Items':>12}")
    print("-" * 80)

    correlation_results = []

    for ailment in ailment_cols:
        if ailment not in valid_df.columns:
            continue

        row = {'ailment': ailment}
        for corn_measure in corn_measures:
            # Filter out NaN
            subset = valid_df[[corn_measure, ailment]].dropna()
            if len(subset) < 100:
                continue

            corr, pval = stats.pearsonr(subset[corn_measure], subset[ailment])
            row[corn_measure] = corr
            row[f'{corn_measure}_pval'] = pval

        correlation_results.append(row)

        # Print
        spending_corr = row.get('corn_share_spending', np.nan)
        weight_corr = row.get('corn_share_weight', np.nan)
        items_corr = row.get('corn_share_items', np.nan)
        print(f"{ailment:<25} {spending_corr:>12.4f} {weight_corr:>12.4f} {items_corr:>12.4f}")

    print("-" * 80)

    # Save correlation results
    corr_df = pd.DataFrame(correlation_results)
    corr_path = os.path.join(OUTPUT_DIR, 'ailments_corn_correlations.csv')
    corr_df.to_csv(corr_path, index=False)
    print(f"\nSaved correlations to: {corr_path}")

    # Compute mean cornification by ailment status
    print("\n" + "="*80)
    print("MEAN CORN SHARE BY AILMENT STATUS")
    print("="*80)

    print("\nCorn Share (Spending) by Ailment Status:")
    print("-" * 80)
    print(f"{'Ailment':<25} {'No Ailment':>15} {'Has Ailment':>15} {'Difference':>12}")
    print("-" * 80)

    mean_results = []

    for ailment in ['obesity', 'diabetes_type2', 'hypertension', 'any_metabolic_disease']:
        if ailment not in valid_df.columns:
            continue

        no_ailment = valid_df[valid_df[ailment] == 0]['corn_share_spending'].mean() * 100
        has_ailment = valid_df[valid_df[ailment] == 1]['corn_share_spending'].mean() * 100
        diff = has_ailment - no_ailment

        print(f"{ailment:<25} {no_ailment:>14.2f}% {has_ailment:>14.2f}% {diff:>+11.2f}pp")

        mean_results.append({
            'ailment': ailment,
            'no_ailment_mean': no_ailment,
            'has_ailment_mean': has_ailment,
            'difference_pp': diff
        })

    print("-" * 80)

    # Create visualization
    print("\nCreating visualization...")

    # Plot: Corn share by number of conditions
    fig, ax = plt.subplots(figsize=(10, 6))

    condition_counts = valid_df.groupby('n_dietary_conditions')['corn_share_spending'].agg(['mean', 'std', 'count']).reset_index()
    condition_counts = condition_counts[condition_counts['count'] >= 100]  # Filter small groups

    ax.bar(condition_counts['n_dietary_conditions'],
           condition_counts['mean'] * 100,
           yerr=condition_counts['std'] * 100 / np.sqrt(condition_counts['count']),
           capsize=3, color='steelblue', edgecolor='navy')

    ax.set_xlabel('Number of Dietary Conditions')
    ax.set_ylabel('Mean Corn Share of Spending (%)')
    ax.set_title('Household Corn Consumption by Number of Dietary Conditions\n(Spending-Weighted, Real Prices)')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    fig_path = os.path.join(FIGS_DIR, 'corn_share_by_conditions.png')
    plt.savefig(fig_path, dpi=150, bbox_inches='tight')
    print(f"Saved figure to: {fig_path}")
    plt.show()

    # Plot: Corn share over time by metabolic disease status
    fig, ax = plt.subplots(figsize=(12, 6))

    for status, label, color in [(0, 'No Metabolic Disease', 'green'), (1, 'Has Metabolic Disease', 'red')]:
        subset = valid_df[valid_df['any_metabolic_disease'] == status]
        yearly = subset.groupby('year')['corn_share_spending'].mean() * 100
        ax.plot(yearly.index, yearly.values, marker='o', linewidth=2, label=label, color=color)

    ax.set_xlabel('Year')
    ax.set_ylabel('Mean Corn Share of Spending (%)')
    ax.set_title('Corn Consumption Over Time by Metabolic Disease Status\n(Spending-Weighted, Real Prices)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    fig_path = os.path.join(FIGS_DIR, 'corn_share_by_metabolic_disease.png')
    plt.savefig(fig_path, dpi=150, bbox_inches='tight')
    print(f"Saved figure to: {fig_path}")
    plt.show()

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
