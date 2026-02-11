#!/usr/bin/env python3
"""
Corn Classification Merger
Merges corn classification data with Nielsen-USDA ingredients data
to identify corn-based ingredients in food products.
"""

import os
import re
import shutil
import pandas as pd


# ============================================================================
# CONFIGURATION
# ============================================================================
# Set to True to use sample data (faster iteration during development)
# Set to False to use full data (for production runs)
USE_SAMPLE = True

# Base paths (will be modified based on USE_SAMPLE)
BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim'


def get_paths():
    """Get input/output paths based on USE_SAMPLE setting."""
    suffix = '_sample' if USE_SAMPLE else ''
    return {
        'corn_path': '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/corn/corn_classification.csv',
        'purchases_path': os.path.join(BASE_DATA_DIR, f'purchases_with_ingredients{suffix}'),
        'output_dir': os.path.join(BASE_DATA_DIR, f'purchases_with_corn_classification{suffix}'),
    }


def load_corn_classification(corn_path):
    """
    Load and clean corn classification data

    Parameters:
    -----------
    corn_path : str
        Path to corn classification CSV file

    Returns:
    --------
    corn_df : DataFrame
        Cleaned corn classification data with ingredient and corn_status columns
    """
    print("="*80)
    print("LOADING CORN CLASSIFICATION DATA")
    print("="*80)

    if not os.path.exists(corn_path):
        print(f"ERROR: Corn classification file not found: {corn_path}")
        return None

    print(f"\nReading: {corn_path}")

    # Load the CSV
    corn_df = pd.read_csv(corn_path, low_memory=False)

    print(f"Rows loaded: {len(corn_df):,}")
    print(f"Columns: {corn_df.columns.tolist()}")

    # Keep only the columns we need
    cols_to_keep = ['ingredient', 'corn_status']
    corn_df = corn_df[cols_to_keep].copy()

    # Remove rows with missing ingredient or corn_status
    n_before = len(corn_df)
    corn_df = corn_df.dropna(subset=['ingredient', 'corn_status'])
    n_after = len(corn_df)
    print(f"\nRows after removing missing values: {n_after:,} (dropped {n_before - n_after:,})")

    # Clean ingredient column: lowercase, strip whitespace
    corn_df['ingredient_clean'] = (
        corn_df['ingredient']
        .str.lower()
        .str.strip()
        .str.replace(r'\s+', ' ', regex=True)  # normalize multiple spaces
    )

    # Clean corn_status: strip whitespace
    corn_df['corn_status'] = corn_df['corn_status'].str.strip()

    # Show sample of cleaned data
    print(f"\nSample of cleaned data:")
    print(corn_df[['ingredient', 'ingredient_clean', 'corn_status']].head(10).to_string())

    # Show corn_status distribution
    print(f"\nCorn status distribution:")
    status_counts = corn_df['corn_status'].value_counts()
    for status, count in status_counts.items():
        print(f"  {status}: {count:,}")

    # Check for duplicates in cleaned ingredient
    n_unique = corn_df['ingredient_clean'].nunique()
    n_total = len(corn_df)
    if n_unique < n_total:
        print(f"\nWARNING: {n_total - n_unique} duplicate ingredients after cleaning")
        # Show duplicates
        dups = corn_df[corn_df['ingredient_clean'].duplicated(keep=False)].sort_values('ingredient_clean')
        print(f"Duplicates:\n{dups[['ingredient', 'ingredient_clean', 'corn_status']].to_string()}")

        # Keep first occurrence
        corn_df = corn_df.drop_duplicates(subset=['ingredient_clean'], keep='first')
        print(f"Kept first occurrence, final count: {len(corn_df):,}")

    print(f"\nFinal unique ingredients: {len(corn_df):,}")

    return corn_df


def parse_ingredients(ingredients_str):
    """
    Parse a comma-separated ingredients string into a list of cleaned ingredients.

    Parameters:
    -----------
    ingredients_str : str
        Comma-separated ingredients string from USDA data

    Returns:
    --------
    list : List of cleaned ingredient strings (lowercase, stripped)
    """
    if pd.isna(ingredients_str) or not ingredients_str:
        return []

    # Split by comma
    ingredients = ingredients_str.split(',')

    # Clean each ingredient: lowercase, strip, normalize spaces
    cleaned = []
    for ing in ingredients:
        ing_clean = ing.lower().strip()
        ing_clean = re.sub(r'\s+', ' ', ing_clean)  # normalize multiple spaces
        if ing_clean:  # skip empty strings
            cleaned.append(ing_clean)

    return cleaned


def classify_corn_content(ingredients_list, corn_lookup):
    """
    Classify corn content of a product based on its ingredients list.

    Parameters:
    -----------
    ingredients_list : list
        List of cleaned ingredient strings
    corn_lookup : dict
        Dictionary mapping ingredient_clean -> corn_status

    Returns:
    --------
    dict : Dictionary with corn classification variables
    """
    result = {
        'first_ing_is_corn_literal': False,
        'first_ing_is_corn_usual_or_literal': False,
        'any_ing_is_corn_literal': False,
        'any_ing_is_corn_usual_or_literal': False,
        'any_ing_is_corn_any': False,  # includes "sometimes"
        'corn_ingredients_found': [],
        'n_corn_ingredients': 0,
    }

    if not ingredients_list:
        return result

    literal_statuses = {'Literally is corn'}
    usual_or_literal_statuses = {'Literally is corn', "Usually corn-based (doesn't have to be)"}
    any_corn_statuses = {'Literally is corn', "Usually corn-based (doesn't have to be)", "Sometimes corn-based (often isn't)"}

    corn_found = []

    for i, ingredient in enumerate(ingredients_list):
        # Check if this ingredient matches any corn classification
        status = corn_lookup.get(ingredient)

        if status:
            corn_found.append((ingredient, status))

            # Check first ingredient
            if i == 0:
                if status in literal_statuses:
                    result['first_ing_is_corn_literal'] = True
                    result['first_ing_is_corn_usual_or_literal'] = True
                elif status in usual_or_literal_statuses:
                    result['first_ing_is_corn_usual_or_literal'] = True

            # Check any ingredient
            if status in literal_statuses:
                result['any_ing_is_corn_literal'] = True
                result['any_ing_is_corn_usual_or_literal'] = True
                result['any_ing_is_corn_any'] = True
            elif status in usual_or_literal_statuses:
                result['any_ing_is_corn_usual_or_literal'] = True
                result['any_ing_is_corn_any'] = True
            elif status in any_corn_statuses:
                result['any_ing_is_corn_any'] = True

    result['corn_ingredients_found'] = corn_found
    result['n_corn_ingredients'] = len(corn_found)

    return result


def process_year(purchases_path, year, corn_lookup, output_dir):
    """
    Process a single year of purchases data and add corn classification columns.

    Parameters:
    -----------
    purchases_path : str
        Path to the purchases_with_ingredients directory
    year : int
        Year to process
    corn_lookup : dict
        Dictionary mapping ingredient_clean -> corn_status
    output_dir : str
        Output directory for processed files

    Returns:
    --------
    stats : dict
        Statistics about corn classification for this year
    """
    print(f"\n\n{'='*80}")
    print(f"PROCESSING YEAR {year}")
    print("="*80)

    # Read the year's data
    year_path = os.path.join(purchases_path, f'panel_year={year}', 'data.parquet')

    if not os.path.exists(year_path):
        print(f"ERROR: File not found: {year_path}")
        return None

    print(f"\nReading: {year_path}")
    df = pd.read_parquet(year_path)
    print(f"Rows loaded: {len(df):,}")

    # Parse ingredients and classify corn content
    print(f"\nClassifying corn content...")

    # Vectorized approach: apply classification function to each row
    def classify_row(ingredients_str):
        ingredients_list = parse_ingredients(ingredients_str)
        return classify_corn_content(ingredients_list, corn_lookup)

    # Apply to all rows and expand results into columns
    print(f"  Processing rows...")
    classifications = df['ingredients'].apply(classify_row)

    # Extract each field into separate columns
    df['first_ing_is_corn_literal'] = classifications.apply(lambda x: x['first_ing_is_corn_literal'])
    df['first_ing_is_corn_usual_or_literal'] = classifications.apply(lambda x: x['first_ing_is_corn_usual_or_literal'])
    df['any_ing_is_corn_literal'] = classifications.apply(lambda x: x['any_ing_is_corn_literal'])
    df['any_ing_is_corn_usual_or_literal'] = classifications.apply(lambda x: x['any_ing_is_corn_usual_or_literal'])
    df['any_ing_is_corn_any'] = classifications.apply(lambda x: x['any_ing_is_corn_any'])
    df['n_corn_ingredients'] = classifications.apply(lambda x: x['n_corn_ingredients'])

    # Calculate statistics
    n_total = len(df)
    n_with_ingredients = df['ingredients'].notna().sum()

    stats = {
        'year': year,
        'total_rows': n_total,
        'rows_with_ingredients': n_with_ingredients,
        'first_ing_corn_literal': df['first_ing_is_corn_literal'].sum(),
        'first_ing_corn_usual_or_literal': df['first_ing_is_corn_usual_or_literal'].sum(),
        'any_ing_corn_literal': df['any_ing_is_corn_literal'].sum(),
        'any_ing_corn_usual_or_literal': df['any_ing_is_corn_usual_or_literal'].sum(),
        'any_ing_corn_any': df['any_ing_is_corn_any'].sum(),
    }

    # Print stats
    print(f"\nCorn Classification Results:")
    print(f"  Total rows: {n_total:,}")
    print(f"  Rows with ingredients: {n_with_ingredients:,}")
    print(f"  First ingredient is corn (literal): {stats['first_ing_corn_literal']:,} ({stats['first_ing_corn_literal']/n_total*100:.2f}%)")
    print(f"  First ingredient is corn (usual/literal): {stats['first_ing_corn_usual_or_literal']:,} ({stats['first_ing_corn_usual_or_literal']/n_total*100:.2f}%)")
    print(f"  Any ingredient is corn (literal): {stats['any_ing_corn_literal']:,} ({stats['any_ing_corn_literal']/n_total*100:.2f}%)")
    print(f"  Any ingredient is corn (usual/literal): {stats['any_ing_corn_usual_or_literal']:,} ({stats['any_ing_corn_usual_or_literal']/n_total*100:.2f}%)")
    print(f"  Any ingredient is corn (any): {stats['any_ing_corn_any']:,} ({stats['any_ing_corn_any']/n_total*100:.2f}%)")

    # Save to output directory
    year_output_path = os.path.join(output_dir, f'panel_year={year}', 'data.parquet')
    os.makedirs(os.path.dirname(year_output_path), exist_ok=True)
    df.to_parquet(year_output_path, engine='pyarrow', compression='snappy', index=False)

    file_size_mb = os.path.getsize(year_output_path) / 1024 / 1024
    print(f"\n✓ Year {year} saved: {year_output_path} ({file_size_mb:.1f} MB)")

    # Free memory
    del df

    return stats


def main():
    """
    Main function to classify corn content in Nielsen-USDA merged data
    """
    print("CORN CLASSIFICATION MERGER")
    print("="*80)

    # Get paths based on USE_SAMPLE setting
    paths = get_paths()
    corn_path = paths['corn_path']
    purchases_path = paths['purchases_path']
    output_dir = paths['output_dir']

    print(f"USE_SAMPLE: {USE_SAMPLE}")
    print(f"Input: {purchases_path}")
    print(f"Output: {output_dir}")

    # Clear and recreate output directory
    if os.path.exists(output_dir):
        print(f"\nClearing existing output directory: {output_dir}")
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Load corn classification data
    corn_df = load_corn_classification(corn_path)

    if corn_df is None:
        print("ERROR: Could not load corn classification data. Exiting.")
        return

    # Create lookup dictionary for fast matching
    corn_lookup = dict(zip(corn_df['ingredient_clean'], corn_df['corn_status']))
    print(f"\nCorn lookup dictionary created with {len(corn_lookup):,} ingredients")

    # Years to process
    years = list(range(2004, 2025))  # 2004-2024

    print(f"\n\nProcessing {len(years)} years: {years[0]}-{years[-1]}")

    # Process each year
    all_stats = []

    for year in years:
        stats = process_year(purchases_path, year, corn_lookup, output_dir)
        if stats:
            all_stats.append(stats)

    # Summary report
    print("\n\n" + "="*80)
    print("CORN CLASSIFICATION SUMMARY (2004-2024)")
    print("="*80)

    if all_stats:
        summary_df = pd.DataFrame(all_stats)

        # Calculate cornification rates for each year
        summary_df['first_ing_corn_literal_rate'] = summary_df['first_ing_corn_literal'] / summary_df['total_rows'] * 100
        summary_df['first_ing_corn_usual_or_literal_rate'] = summary_df['first_ing_corn_usual_or_literal'] / summary_df['total_rows'] * 100
        summary_df['any_ing_corn_literal_rate'] = summary_df['any_ing_corn_literal'] / summary_df['total_rows'] * 100
        summary_df['any_ing_corn_usual_or_literal_rate'] = summary_df['any_ing_corn_usual_or_literal'] / summary_df['total_rows'] * 100
        summary_df['any_ing_corn_any_rate'] = summary_df['any_ing_corn_any'] / summary_df['total_rows'] * 100

        print("\nCORN CONTENT BY YEAR:")
        print("-" * 120)
        print(f"{'Year':<6} {'Total':>12} {'1st Lit %':>10} {'1st Usu %':>10} {'Any Lit %':>10} {'Any Usu %':>10} {'Any All %':>10}")
        print("-" * 120)

        for _, row in summary_df.iterrows():
            print(f"{row['year']:<6} {row['total_rows']:>12,} "
                  f"{row['first_ing_corn_literal_rate']:>9.2f}% "
                  f"{row['first_ing_corn_usual_or_literal_rate']:>9.2f}% "
                  f"{row['any_ing_corn_literal_rate']:>9.2f}% "
                  f"{row['any_ing_corn_usual_or_literal_rate']:>9.2f}% "
                  f"{row['any_ing_corn_any_rate']:>9.2f}%")

        print("-" * 120)

        # Calculate averages across all years
        avg_first_lit = summary_df['first_ing_corn_literal_rate'].mean()
        avg_first_usu = summary_df['first_ing_corn_usual_or_literal_rate'].mean()
        avg_any_lit = summary_df['any_ing_corn_literal_rate'].mean()
        avg_any_usu = summary_df['any_ing_corn_usual_or_literal_rate'].mean()
        avg_any_all = summary_df['any_ing_corn_any_rate'].mean()

        total_rows = summary_df['total_rows'].sum()
        print(f"{'AVG':<6} {total_rows:>12,} "
              f"{avg_first_lit:>9.2f}% "
              f"{avg_first_usu:>9.2f}% "
              f"{avg_any_lit:>9.2f}% "
              f"{avg_any_usu:>9.2f}% "
              f"{avg_any_all:>9.2f}%")

        # Create average row for CSV
        avg_row = {
            'year': 'AVG',
            'total_rows': summary_df['total_rows'].sum(),
            'rows_with_ingredients': summary_df['rows_with_ingredients'].sum(),
            'first_ing_corn_literal': summary_df['first_ing_corn_literal'].sum(),
            'first_ing_corn_usual_or_literal': summary_df['first_ing_corn_usual_or_literal'].sum(),
            'any_ing_corn_literal': summary_df['any_ing_corn_literal'].sum(),
            'any_ing_corn_usual_or_literal': summary_df['any_ing_corn_usual_or_literal'].sum(),
            'any_ing_corn_any': summary_df['any_ing_corn_any'].sum(),
            'first_ing_corn_literal_rate': avg_first_lit,
            'first_ing_corn_usual_or_literal_rate': avg_first_usu,
            'any_ing_corn_literal_rate': avg_any_lit,
            'any_ing_corn_usual_or_literal_rate': avg_any_usu,
            'any_ing_corn_any_rate': avg_any_all,
        }

        # Append average row to summary dataframe for CSV export
        summary_with_avg = pd.concat([summary_df, pd.DataFrame([avg_row])], ignore_index=True)

        # Save summary with average row
        summary_path = os.path.join(output_dir, 'corn_classification_summary.csv')
        summary_with_avg.to_csv(summary_path, index=False)
        print(f"\nSaved summary to: {summary_path}")

        print(f"\n✓ All output saved to: {output_dir}")
        print(f"\nTo read the data:")
        print(f"  df = pd.read_parquet('{output_dir}')")
        print(f"\nTo read specific years:")
        print(f"  df = pd.read_parquet('{output_dir}', filters=[('panel_year', 'in', [2020, 2021])])")

        print(f"\nNew columns added:")
        print(f"  - first_ing_is_corn_literal: First ingredient is literally corn")
        print(f"  - first_ing_is_corn_usual_or_literal: First ingredient is usually/literally corn")
        print(f"  - any_ing_is_corn_literal: Any ingredient is literally corn")
        print(f"  - any_ing_is_corn_usual_or_literal: Any ingredient is usually/literally corn")
        print(f"  - any_ing_is_corn_any: Any ingredient has any corn classification")
        print(f"  - n_corn_ingredients: Count of corn-classified ingredients")
    else:
        print("\n\nERROR: No data was successfully processed")


if __name__ == "__main__":
    main()
