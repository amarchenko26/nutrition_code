#!/usr/bin/env python3
"""
USDA FoodData Cleaner with Reformulation Tracking

This script:
1. Loads all USDA FoodData releases and extracts release dates
2. Tracks ingredient reformulations across releases (same UPC, different ingredients)
3. Outputs a time-varying ingredients file for year-based merging with Nielsen
4. Generates summary statistics on reformulation patterns
"""

import os
import re
import zipfile
from pathlib import Path
import pandas as pd
from io import BytesIO


def normalize_ingredients(ingredients_str):
    """
    Normalize ingredient string for comparison.
    Removes differences in spelling, capitalization, spacing, punctuation.

    Parameters:
    -----------
    ingredients_str : str
        Raw ingredients string

    Returns:
    --------
    str : Normalized ingredients string for comparison
    """
    if pd.isna(ingredients_str) or not ingredients_str:
        return ""

    # Convert to lowercase
    normalized = str(ingredients_str).lower()

    # Remove extra whitespace
    normalized = re.sub(r'\s+', ' ', normalized)

    # Remove common punctuation variations
    normalized = normalized.replace(';', ',')
    normalized = normalized.replace(':', ',')

    # Remove parentheses content variations (keep content but normalize)
    # e.g., "(vitamin A)" vs "( vitamin A )" -> "(vitamin a)"
    normalized = re.sub(r'\(\s+', '(', normalized)
    normalized = re.sub(r'\s+\)', ')', normalized)

    # Normalize common ingredient name variations
    normalized = normalized.replace('high fructose corn syrup', 'hfcs')
    normalized = normalized.replace('high-fructose corn syrup', 'hfcs')

    # Strip leading/trailing whitespace
    normalized = normalized.strip()

    return normalized


def extract_release_date(filename):
    """
    Extract release date from USDA filename.

    Parameters:
    -----------
    filename : str
        Filename like 'FoodData_Central_branded_food_csv_2024-04-18.zip'
        or 'BFPD_csv_07132018.zip'

    Returns:
    --------
    tuple : (year, month, day) or None if not parseable
    """
    # Try standard format: YYYY-MM-DD
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
    if match:
        return int(match.group(1)), int(match.group(2)), int(match.group(3))

    # Try BFPD format: MMDDYYYY
    match = re.search(r'(\d{2})(\d{2})(\d{4})', filename)
    if match:
        return int(match.group(3)), int(match.group(1)), int(match.group(2))

    return None


def load_usda_release(zip_path):
    """
    Load a single USDA release from a zip file.

    Parameters:
    -----------
    zip_path : Path
        Path to the zip file

    Returns:
    --------
    tuple : (DataFrame, release_date_tuple) or (None, None) if error
    """
    print(f"\nLoading: {zip_path.name}")

    release_date = extract_release_date(zip_path.name)
    if release_date is None:
        print(f"  WARNING: Could not extract date from filename")
        return None, None

    year, month, day = release_date
    print(f"  Release date: {year}-{month:02d}-{day:02d}")

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            file_list = zf.namelist()

            # Find the branded_food.csv file (or Products.csv for 2018)
            if zip_path.name == "BFPD_csv_07132018.zip":
                target_filename = "Products.csv"
            else:
                target_filename = "branded_food.csv"

            found_file = None
            for filename in file_list:
                base_filename = os.path.basename(filename)
                if base_filename.lower() == target_filename.lower():
                    found_file = filename
                    break

            if not found_file:
                print(f"  ERROR: {target_filename} not found in archive")
                return None, None

            # Read the CSV
            with zf.open(found_file) as csv_file:
                df = pd.read_csv(BytesIO(csv_file.read()), low_memory=False)
                print(f"  Rows loaded: {len(df):,}")

                # Add release date columns
                df['usda_release_year'] = year
                df['usda_release_month'] = month
                df['usda_release_day'] = day

                return df, release_date

    except Exception as e:
        print(f"  ERROR: {str(e)}")
        return None, None


def consolidate_ingredients_column(df):
    """
    Consolidate ingredients and ingredients_english columns into one.

    Parameters:
    -----------
    df : DataFrame
        DataFrame with potential ingredients columns

    Returns:
    --------
    DataFrame with consolidated 'ingredients' column
    """
    has_ingredients = 'ingredients' in df.columns
    has_ingredients_english = 'ingredients_english' in df.columns

    if not has_ingredients and not has_ingredients_english:
        print("  WARNING: No ingredients columns found")
        df['ingredients'] = None
        return df

    # Ensure both columns exist
    if not has_ingredients:
        df['ingredients'] = None
    if not has_ingredients_english:
        df['ingredients_english'] = None

    # Convert to string and handle NaN
    df['ingredients'] = df['ingredients'].astype(str).replace('nan', '').replace('None', '')
    df['ingredients_english'] = df['ingredients_english'].astype(str).replace('nan', '').replace('None', '')

    # Use ingredients_english if ingredients is empty
    empty_ing = (df['ingredients'] == '') | (df['ingredients'].isna())
    nonempty_ing_eng = (df['ingredients_english'] != '') & (df['ingredients_english'].notna())

    df.loc[empty_ing & nonempty_ing_eng, 'ingredients'] = df.loc[empty_ing & nonempty_ing_eng, 'ingredients_english']

    # Lowercase
    df['ingredients'] = df['ingredients'].str.lower()

    # Replace empty strings with None
    df['ingredients'] = df['ingredients'].replace('', None)

    return df


def load_all_usda_releases(base_path):
    """
    Load all USDA releases and combine with release date tracking.

    Parameters:
    -----------
    base_path : str
        Path to directory containing USDA zip files

    Returns:
    --------
    DataFrame with all releases combined, including release date columns
    """
    print("="*80)
    print("LOADING ALL USDA RELEASES")
    print("="*80)

    if not os.path.exists(base_path):
        print(f"ERROR: Directory not found: {base_path}")
        return None

    # Find all zip files
    zip_files = sorted(Path(base_path).glob("*.zip"))

    if not zip_files:
        print(f"No .zip files found in {base_path}")
        return None

    print(f"\nFound {len(zip_files)} zip files")

    all_releases = []
    release_info = []

    for zip_path in zip_files:
        df, release_date = load_usda_release(zip_path)
        if df is not None:
            df = consolidate_ingredients_column(df)
            all_releases.append(df)
            release_info.append({
                'filename': zip_path.name,
                'year': release_date[0],
                'month': release_date[1],
                'day': release_date[2],
                'n_products': len(df)
            })

    if not all_releases:
        print("ERROR: No releases loaded successfully")
        return None

    # Summary of releases
    print("\n" + "="*80)
    print("USDA RELEASE SUMMARY")
    print("="*80)
    release_summary = pd.DataFrame(release_info)
    print(release_summary.to_string(index=False))

    # Combine all releases
    print(f"\nCombining {len(all_releases)} releases...")
    combined_df = pd.concat(all_releases, ignore_index=True)
    print(f"Total rows (all releases): {len(combined_df):,}")

    return combined_df, release_summary


def standardize_upc(df):
    """
    Standardize UPC codes for matching with Nielsen.
    Creates upc_11 column (11 digits, without check digit).

    Parameters:
    -----------
    df : DataFrame
        DataFrame with gtin_upc column

    Returns:
    --------
    DataFrame with standardized UPC columns
    """
    # Convert to string and remove decimals
    df['gtin_upc'] = df['gtin_upc'].astype(str).str.replace('.0', '', regex=False)

    # Pad to 12 digits
    df['upc_12'] = df['gtin_upc'].str.zfill(12)

    # Create 11-digit version (remove check digit)
    df['upc_11'] = df['upc_12'].str[:-1]

    return df


def track_reformulations(combined_df):
    """
    Track ingredient reformulations across USDA releases.

    A reformulation is when the same UPC has different (normalized) ingredients
    in different releases.

    Parameters:
    -----------
    combined_df : DataFrame
        Combined DataFrame from all USDA releases

    Returns:
    --------
    tuple : (time_varying_df, reformulation_summary)
        - time_varying_df: DataFrame with one row per UPC per release (for changed products)
          plus one row per UPC for products that never changed
        - reformulation_summary: Dictionary with summary statistics
    """
    print("\n" + "="*80)
    print("TRACKING INGREDIENT REFORMULATIONS")
    print("="*80)

    # Standardize UPCs
    combined_df = standardize_upc(combined_df)

    # Create normalized ingredients for comparison
    print("\nNormalizing ingredients for comparison...")
    combined_df['ingredients_normalized'] = combined_df['ingredients'].apply(normalize_ingredients)

    # Sort by UPC and release date
    combined_df = combined_df.sort_values(['upc_11', 'usda_release_year', 'usda_release_month'])

    # Group by UPC and find unique normalized ingredients
    print("Identifying reformulations...")

    upc_groups = combined_df.groupby('upc_11')

    reformulation_records = []
    unchanged_records = []

    n_upcs = combined_df['upc_11'].nunique()
    print(f"Total unique UPCs: {n_upcs:,}")

    for upc, group in upc_groups:
        # Get unique normalized ingredients (excluding empty)
        non_empty = group[group['ingredients_normalized'] != '']

        if len(non_empty) == 0:
            continue

        unique_ingredients = non_empty['ingredients_normalized'].unique()

        if len(unique_ingredients) > 1:
            # This UPC has been reformulated
            # Keep each release's version
            for _, row in non_empty.iterrows():
                reformulation_records.append(row)
        else:
            # No reformulation - keep the most recent version
            latest = non_empty.iloc[-1]
            unchanged_records.append(latest)

    print(f"\nReformulation analysis complete:")
    print(f"  UPCs with reformulations: {len(set(r['upc_11'] for r in reformulation_records)):,}")
    print(f"  UPCs without reformulations: {len(unchanged_records):,}")

    # Create time-varying DataFrame
    reformulated_df = pd.DataFrame(reformulation_records) if reformulation_records else pd.DataFrame()
    unchanged_df = pd.DataFrame(unchanged_records) if unchanged_records else pd.DataFrame()

    # For unchanged products, set a flag
    if len(unchanged_df) > 0:
        unchanged_df['was_reformulated'] = False
    if len(reformulated_df) > 0:
        reformulated_df['was_reformulated'] = True

    # Combine
    if len(reformulated_df) > 0 and len(unchanged_df) > 0:
        time_varying_df = pd.concat([reformulated_df, unchanged_df], ignore_index=True)
    elif len(reformulated_df) > 0:
        time_varying_df = reformulated_df
    else:
        time_varying_df = unchanged_df

    # Calculate summary statistics
    n_reformulated_upcs = len(set(r['upc_11'] for r in reformulation_records)) if reformulation_records else 0
    n_unchanged_upcs = len(unchanged_records)
    n_total_upcs = n_reformulated_upcs + n_unchanged_upcs

    summary = {
        'total_upcs': n_total_upcs,
        'reformulated_upcs': n_reformulated_upcs,
        'unchanged_upcs': n_unchanged_upcs,
        'pct_reformulated': n_reformulated_upcs / n_total_upcs * 100 if n_total_upcs > 0 else 0,
    }

    return time_varying_df, summary


def create_year_specific_ingredients(time_varying_df, output_dir):
    """
    Create year-specific ingredient files for merging with Nielsen.

    For each Nielsen year, uses the closest prior USDA release.

    Parameters:
    -----------
    time_varying_df : DataFrame
        Time-varying ingredients DataFrame
    output_dir : str
        Directory to save output files

    Returns:
    --------
    dict : Mapping of Nielsen year to USDA release used
    """
    print("\n" + "="*80)
    print("CREATING YEAR-SPECIFIC INGREDIENT FILES")
    print("="*80)

    # Get available USDA release years
    usda_years = sorted(time_varying_df['usda_release_year'].unique())
    print(f"\nUSDA release years: {usda_years}")

    # Nielsen years (2004-2024)
    nielsen_years = list(range(2004, 2025))

    # For each Nielsen year, find the best USDA release
    # Use the most recent USDA release <= Nielsen year (or earliest if Nielsen year is before all releases)
    year_mapping = {}

    for nielsen_year in nielsen_years:
        # Find USDA releases <= this Nielsen year
        prior_releases = [y for y in usda_years if y <= nielsen_year]

        if prior_releases:
            # Use the most recent prior release
            usda_year = max(prior_releases)
        else:
            # Nielsen year is before earliest USDA release - use earliest
            usda_year = min(usda_years)

        year_mapping[nielsen_year] = usda_year

    print("\nNielsen Year -> USDA Release mapping:")
    for ny, uy in year_mapping.items():
        print(f"  {ny} -> {uy}")

    # Create a lookup table for each Nielsen year
    # For each UPC, get the ingredients from the appropriate USDA release

    # First, for each UPC, get the ingredients for each USDA release year
    # (using the latest release within that year if multiple)

    # Get the latest release within each year for each UPC
    time_varying_df = time_varying_df.sort_values(['upc_11', 'usda_release_year', 'usda_release_month', 'usda_release_day'])

    # For each UPC and year, keep the last entry (most recent within that year)
    upc_year_ingredients = time_varying_df.groupby(['upc_11', 'usda_release_year']).last().reset_index()

    print(f"\nUPC-year combinations: {len(upc_year_ingredients):,}")

    # Save the mapping
    mapping_df = pd.DataFrame([
        {'nielsen_year': k, 'usda_release_year': v}
        for k, v in year_mapping.items()
    ])
    mapping_path = os.path.join(output_dir, 'nielsen_usda_year_mapping.csv')
    mapping_df.to_csv(mapping_path, index=False)
    print(f"\nSaved year mapping to: {mapping_path}")

    # Save the time-varying ingredients file
    # This contains one row per UPC per USDA release year (where ingredients exist)
    cols_to_keep = ['upc_11', 'gtin_upc', 'ingredients', 'brand_name', 'branded_food_category',
                    'usda_release_year', 'usda_release_month', 'usda_release_day',
                    'was_reformulated', 'ingredients_normalized']

    # Only keep columns that exist
    cols_to_keep = [c for c in cols_to_keep if c in upc_year_ingredients.columns]

    output_df = upc_year_ingredients[cols_to_keep].copy()

    # Save as parquet for efficiency
    output_path = os.path.join(output_dir, 'usda_ingredients_by_year.parquet')
    output_df.to_parquet(output_path, index=False)
    print(f"Saved time-varying ingredients to: {output_path}")
    print(f"  Rows: {len(output_df):,}")

    # Also save a "latest only" version for backward compatibility
    latest_df = time_varying_df.groupby('upc_11').last().reset_index()
    latest_cols = ['upc_11', 'gtin_upc', 'ingredients', 'brand_name', 'branded_food_category']
    latest_cols = [c for c in latest_cols if c in latest_df.columns]
    latest_df = latest_df[latest_cols]

    latest_path = os.path.join(output_dir, 'usda_branded_food_deduped.csv')
    latest_df.to_csv(latest_path, index=False)
    print(f"\nSaved latest-only ingredients to: {latest_path}")
    print(f"  Rows: {len(latest_df):,}")

    return year_mapping, output_df


def load_corn_classification(corn_path):
    """
    Load corn classification data for analyzing cornification changes.

    Parameters:
    -----------
    corn_path : str
        Path to corn classification CSV file

    Returns:
    --------
    dict : Mapping of ingredient (lowercase) -> corn_status
    """
    if not os.path.exists(corn_path):
        print(f"  WARNING: Corn classification file not found: {corn_path}")
        return None

    corn_df = pd.read_csv(corn_path, low_memory=False)

    # Clean ingredient column
    corn_df['ingredient_clean'] = (
        corn_df['ingredient']
        .str.lower()
        .str.strip()
        .str.replace(r'\s+', ' ', regex=True)
    )

    corn_df['corn_status'] = corn_df['corn_status'].str.strip()

    # Create lookup dictionary
    corn_lookup = dict(zip(corn_df['ingredient_clean'], corn_df['corn_status']))

    return corn_lookup


def classify_ingredients_for_corn(ingredients_str, corn_lookup):
    """
    Classify whether a product contains corn based on its ingredients.

    Parameters:
    -----------
    ingredients_str : str
        Comma-separated ingredients string
    corn_lookup : dict
        Mapping of ingredient -> corn_status

    Returns:
    --------
    dict with corn classification results
    """
    if pd.isna(ingredients_str) or not ingredients_str or corn_lookup is None:
        return {
            'has_corn_literal': False,
            'has_corn_usual_or_literal': False,
            'has_corn_any': False,
            'n_corn_ingredients': 0,
        }

    # Parse ingredients
    ingredients = ingredients_str.split(',')
    cleaned = []
    for ing in ingredients:
        ing_clean = ing.lower().strip()
        ing_clean = re.sub(r'\s+', ' ', ing_clean)
        if ing_clean:
            cleaned.append(ing_clean)

    literal_statuses = {'Literally is corn'}
    usual_or_literal_statuses = {'Literally is corn', "Usually corn-based (doesn't have to be)"}
    any_corn_statuses = {'Literally is corn', "Usually corn-based (doesn't have to be)", "Sometimes corn-based (often isn't)"}

    has_literal = False
    has_usual_or_literal = False
    has_any = False
    n_corn = 0

    for ingredient in cleaned:
        status = corn_lookup.get(ingredient)
        if status:
            n_corn += 1
            if status in literal_statuses:
                has_literal = True
                has_usual_or_literal = True
                has_any = True
            elif status in usual_or_literal_statuses:
                has_usual_or_literal = True
                has_any = True
            elif status in any_corn_statuses:
                has_any = True

    return {
        'has_corn_literal': has_literal,
        'has_corn_usual_or_literal': has_usual_or_literal,
        'has_corn_any': has_any,
        'n_corn_ingredients': n_corn,
    }


def analyze_cornification_changes(time_varying_df, output_dir):
    """
    Analyze cornification changes in reformulated products.

    For products that were reformulated, compare corn content between
    earliest and latest formulation.

    Parameters:
    -----------
    time_varying_df : DataFrame
        Time-varying ingredients data
    output_dir : str
        Directory to save output

    Returns:
    --------
    dict : Summary statistics on cornification changes
    """
    print("\n" + "="*80)
    print("ANALYZING CORNIFICATION CHANGES IN REFORMULATIONS")
    print("="*80)

    # Load corn classification
    corn_path = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/corn/corn_classification.csv'
    corn_lookup = load_corn_classification(corn_path)

    if corn_lookup is None:
        print("WARNING: Could not load corn classification. Skipping cornification analysis.")
        return None

    print(f"Loaded corn classification with {len(corn_lookup):,} ingredients")

    # Get reformulated products only
    reformulated = time_varying_df[time_varying_df['was_reformulated'] == True].copy()

    if len(reformulated) == 0:
        print("No reformulated products found.")
        return None

    reformulated_upcs = reformulated['upc_11'].unique()
    print(f"\nAnalyzing {len(reformulated_upcs):,} reformulated UPCs")

    # For each reformulated UPC, get earliest and latest formulation
    reformulated = reformulated.sort_values(['upc_11', 'usda_release_year', 'usda_release_month'])

    # Get first and last for each UPC
    earliest = reformulated.groupby('upc_11').first().reset_index()
    latest = reformulated.groupby('upc_11').last().reset_index()

    # Classify corn content for earliest and latest
    print("Classifying corn content for earliest formulations...")
    earliest_corn = earliest['ingredients'].apply(lambda x: classify_ingredients_for_corn(x, corn_lookup))
    earliest['corn_earliest'] = earliest_corn.apply(lambda x: x['has_corn_usual_or_literal'])

    print("Classifying corn content for latest formulations...")
    latest_corn = latest['ingredients'].apply(lambda x: classify_ingredients_for_corn(x, corn_lookup))
    latest['corn_latest'] = latest_corn.apply(lambda x: x['has_corn_usual_or_literal'])

    # Merge to compare
    comparison = earliest[['upc_11', 'corn_earliest', 'usda_release_year']].merge(
        latest[['upc_11', 'corn_latest', 'usda_release_year']],
        on='upc_11',
        suffixes=('_earliest', '_latest')
    )

    # Calculate changes
    comparison['added_corn'] = (~comparison['corn_earliest']) & (comparison['corn_latest'])
    comparison['removed_corn'] = (comparison['corn_earliest']) & (~comparison['corn_latest'])
    comparison['unchanged_corn'] = comparison['corn_earliest'] == comparison['corn_latest']

    # Summary statistics
    n_total = len(comparison)
    n_added = comparison['added_corn'].sum()
    n_removed = comparison['removed_corn'].sum()
    n_unchanged = comparison['unchanged_corn'].sum()
    n_had_corn_early = comparison['corn_earliest'].sum()
    n_has_corn_late = comparison['corn_latest'].sum()

    print(f"\n{'='*60}")
    print("CORNIFICATION CHANGES IN REFORMULATED PRODUCTS")
    print("="*60)
    print(f"\nTotal reformulated UPCs analyzed: {n_total:,}")
    print(f"\nCorn content (usual or literal):")
    print(f"  Early formulation:  {n_had_corn_early:,} ({n_had_corn_early/n_total*100:.1f}%) had corn")
    print(f"  Latest formulation: {n_has_corn_late:,} ({n_has_corn_late/n_total*100:.1f}%) have corn")
    print(f"\nChanges:")
    print(f"  Added corn:     {n_added:,} ({n_added/n_total*100:.1f}%)")
    print(f"  Removed corn:   {n_removed:,} ({n_removed/n_total*100:.1f}%)")
    print(f"  No change:      {n_unchanged:,} ({n_unchanged/n_total*100:.1f}%)")
    print(f"\nNet change: {n_added - n_removed:+,} (positive = more products added corn)")

    # Create summary
    summary = {
        'total_reformulated_upcs': n_total,
        'had_corn_early': n_had_corn_early,
        'has_corn_late': n_has_corn_late,
        'added_corn': n_added,
        'removed_corn': n_removed,
        'unchanged_corn': n_unchanged,
        'net_change': n_added - n_removed,
        'pct_added_corn': n_added / n_total * 100 if n_total > 0 else 0,
        'pct_removed_corn': n_removed / n_total * 100 if n_total > 0 else 0,
    }

    # Save detailed comparison
    comparison_path = os.path.join(output_dir, 'reformulation_cornification_details.csv')
    comparison.to_csv(comparison_path, index=False)
    print(f"\nSaved detailed comparison to: {comparison_path}")

    # Save summary
    summary_path = os.path.join(output_dir, 'reformulation_cornification_summary.csv')
    pd.DataFrame([summary]).to_csv(summary_path, index=False)
    print(f"Saved cornification summary to: {summary_path}")

    return summary


def main():
    """Main function to process USDA data with reformulation tracking."""
    print("USDA FOODDATA CLEANER WITH REFORMULATION TRACKING")
    print("="*80)

    base_path = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/usda'
    output_dir = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/usda'

    os.makedirs(output_dir, exist_ok=True)

    # Load all releases
    result = load_all_usda_releases(base_path)
    if result is None:
        print("ERROR: Failed to load USDA releases")
        return

    combined_df, release_summary = result

    # Track reformulations
    time_varying_df, reformulation_summary = track_reformulations(combined_df)

    # Create year-specific files
    year_mapping, ingredients_by_year = create_year_specific_ingredients(time_varying_df, output_dir)

    # Save reformulation summary
    print("\n" + "="*80)
    print("REFORMULATION SUMMARY")
    print("="*80)
    print(f"\nTotal unique UPCs: {reformulation_summary['total_upcs']:,}")
    print(f"UPCs with reformulations: {reformulation_summary['reformulated_upcs']:,} ({reformulation_summary['pct_reformulated']:.1f}%)")
    print(f"UPCs without reformulations: {reformulation_summary['unchanged_upcs']:,}")

    summary_path = os.path.join(output_dir, 'reformulation_summary.csv')
    pd.DataFrame([reformulation_summary]).to_csv(summary_path, index=False)
    print(f"\nSaved reformulation summary to: {summary_path}")

    # Analyze cornification changes in reformulated products
    corn_summary = analyze_cornification_changes(time_varying_df, output_dir)

    # Create combined summary
    combined_summary = {
        **reformulation_summary,
        **(corn_summary if corn_summary else {})
    }
    combined_summary_path = os.path.join(output_dir, 'reformulation_complete_summary.csv')
    pd.DataFrame([combined_summary]).to_csv(combined_summary_path, index=False)
    print(f"\nSaved complete summary to: {combined_summary_path}")

    print("\n" + "="*80)
    print("USDA CLEANING COMPLETE")
    print("="*80)
    print(f"\nOutput files in: {output_dir}")
    print("  - usda_ingredients_by_year.parquet (time-varying, for year-based merge)")
    print("  - usda_branded_food_deduped.csv (latest only, for backward compatibility)")
    print("  - nielsen_usda_year_mapping.csv (Nielsen year -> USDA release mapping)")
    print("  - reformulation_summary.csv (basic reformulation statistics)")
    print("  - reformulation_cornification_summary.csv (corn changes in reformulations)")
    print("  - reformulation_cornification_details.csv (UPC-level corn change details)")
    print("  - reformulation_complete_summary.csv (all statistics combined)")


if __name__ == "__main__":
    main()
