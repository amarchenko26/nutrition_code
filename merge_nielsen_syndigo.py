"""
Merge Nielsen Consumer Panel data with Syndigo product/nutrition data.

UPC Harmonization:
- Nielsen UPCs are 12 digits (string). Prepend '0' to get 13 digits.
- Syndigo UPCs are 14 digits (GTIN-14). Drop last digit (check digit) to get 13 digits.
- Merge on the shared 13-digit UPC key.
"""

import os
import pandas as pd
import numpy as np
from glob import glob

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
SYNDIGO_DIR = os.path.join(BASE_DATA_DIR, 'raw', 'syndigo')
NIELSEN_PURCHASES_DIR = os.path.join(BASE_DATA_DIR, 'interim', 'purchases_food')
OUTPUT_DIR = os.path.join(BASE_DATA_DIR, 'interim', 'purchases_with_nutrition')

SYNDIGO_YEARS = list(range(2005, 2025))

# Core nutrients to pivot to wide format: NutrientMasterID -> column name
CORE_NUTRIENTS = {
    1: 'calories',
    2: 'calories_from_fat',
    4: 'total_fat_g',
    5: 'saturated_fat_g',
    8: 'cholesterol_mg',
    9: 'sodium_mg',
    11: 'total_carbohydrate_g',
    12: 'dietary_fiber_g',
    15: 'sugars_g',
    18: 'protein_g',
}


# ============================================================================
# SYNDIGO DATA LOADING
# ============================================================================

def get_product_filename(year):
    """Return correct Product CSV filename for a given Syndigo year."""
    # 2005-2015 use ProductYEAR.csv, 2016+ use Product.csv
    year_path = os.path.join(SYNDIGO_DIR, str(year))
    if os.path.exists(os.path.join(year_path, f'Product{year}.csv')):
        return f'Product{year}.csv'
    return 'Product.csv'


def harmonize_syndigo_upc(upc_series):
    """Syndigo 14-digit GTIN-14 -> drop last digit (check digit) -> 13 digits."""
    return upc_series.astype(str).str.strip().str.zfill(14).str[:-1]


def harmonize_nielsen_upc(upc_series):
    """Nielsen 12-digit UPC -> prepend '0' -> 13 digits."""
    return '0' + upc_series.astype(str).str.zfill(12)


def load_syndigo_year(year):
    """
    Load and combine the 4 Syndigo files for a single year into a
    wide-format DataFrame with one row per UPC.
    """
    year_dir = os.path.join(SYNDIGO_DIR, str(year))
    if not os.path.isdir(year_dir):
        print(f"  Skipping {year}: directory not found")
        return None

    # --- Product ---
    product_file = get_product_filename(year)
    product_df = pd.read_csv(os.path.join(year_dir, product_file),
                             dtype={'UPC': str}, encoding='latin-1')
    # Drop ProductID if present (appears in some years)
    if 'ProductID' in product_df.columns:
        product_df = product_df.drop(columns=['ProductID'])

    # Keep key product columns
    keep_cols = ['UPC', 'Brand', 'Manufacturer', 'Category', 'Ingredients',
                 'ItemSize', 'ItemMeasure']
    # ItemName doesn't exist in 2005; use Description as fallback
    if 'ItemName' in product_df.columns:
        keep_cols.append('ItemName')
    elif 'Description' in product_df.columns:
        product_df = product_df.rename(columns={'Description': 'ItemName'})
        keep_cols.append('ItemName')

    product_df = product_df[[c for c in keep_cols if c in product_df.columns]]

    # --- NutrientMaster (lookup table) ---
    nutrient_master = pd.read_csv(os.path.join(year_dir, 'NutrientMaster.csv'),
                                  encoding='latin-1')

    # --- Nutrient ---
    nutrient_df = pd.read_csv(os.path.join(year_dir, 'Nutrient.csv'),
                              dtype={'UPC': str}, encoding='latin-1')
    if 'ProductID' in nutrient_df.columns:
        nutrient_df = nutrient_df.drop(columns=['ProductID'])

    # Filter to base preparation (ValuePreparedType == 0) and core nutrients
    nutrient_df = nutrient_df[
        (nutrient_df['ValuePreparedType'] == 0) &
        (nutrient_df['NutrientMasterID'].isin(CORE_NUTRIENTS.keys()))
    ].copy()

    # Map nutrient IDs to column names and pivot wide
    nutrient_df['nutrient_col'] = nutrient_df['NutrientMasterID'].map(CORE_NUTRIENTS)
    nutrient_wide = nutrient_df.pivot_table(
        index='UPC',
        columns='nutrient_col',
        values='Quantity',
        aggfunc='first'
    ).reset_index()

    # --- ValuePrepared (serving size info) ---
    vp_df = pd.read_csv(os.path.join(year_dir, 'ValuePrepared.csv'),
                         dtype={'UPC': str}, encoding='latin-1')
    if 'ProductID' in vp_df.columns:
        vp_df = vp_df.drop(columns=['ProductID'])

    # Filter to base preparation
    vp_df = vp_df[vp_df['ValuePreparedType'] == 0].copy()
    vp_keep = ['UPC', 'ServingSizeText', 'ServingSizeUOM', 'ServingsPerContainer']
    vp_df = vp_df[[c for c in vp_keep if c in vp_df.columns]]
    vp_df = vp_df.drop_duplicates(subset='UPC', keep='first')

    # --- Merge all Syndigo files on UPC ---
    merged = product_df.merge(nutrient_wide, on='UPC', how='left')
    merged = merged.merge(vp_df, on='UPC', how='left')
    merged['syndigo_year'] = year

    # Harmonize UPC: drop check digit -> 13 digits
    merged['upc_13'] = harmonize_syndigo_upc(merged['UPC'])

    return merged


def build_syndigo_master():
    """
    Build cumulative Syndigo master across all years.
    For each UPC, keeps the most recent year's record.
    """
    print("Building Syndigo master from raw CSV files...")
    print("-" * 80)

    all_years = []
    for year in SYNDIGO_YEARS:
        year_df = load_syndigo_year(year)
        if year_df is not None:
            all_years.append(year_df)
            print(f"  {year}: {len(year_df):,} products")

    master = pd.concat(all_years, ignore_index=True)

    # For each UPC, keep the most recent syndigo_year
    master = master.sort_values('syndigo_year', ascending=False)
    master = master.drop_duplicates(subset='upc_13', keep='first')

    print(f"\nSyndigo master: {len(master):,} unique products (13-digit UPCs)")
    return master


# ============================================================================
# MERGE WITH NIELSEN
# ============================================================================

def merge_nielsen_with_syndigo(syndigo_master):
    """
    Merge Nielsen purchases (year by year) with Syndigo nutrition data.
    """
    print("\n" + "=" * 80)
    print("MERGING NIELSEN WITH SYNDIGO")
    print("=" * 80)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Detect available Nielsen year partitions
    year_dirs = sorted(glob(os.path.join(NIELSEN_PURCHASES_DIR, 'panel_year=*')))
    years = [int(os.path.basename(d).replace('panel_year=', '')) for d in year_dirs]
    print(f"Nielsen years available: {years}")

    # Prepare Syndigo merge columns (drop raw Syndigo UPC, keep upc_13 + data cols)
    syndigo_merge = syndigo_master.drop(columns=['UPC'])

    # Prefix Syndigo columns to avoid conflicts with Nielsen columns
    rename_map = {}
    for col in syndigo_merge.columns:
        if col not in ['upc_13', 'syndigo_year'] and col not in CORE_NUTRIENTS.values():
            rename_map[col] = f'syndigo_{col.lower()}'
    syndigo_merge = syndigo_merge.rename(columns=rename_map)

    all_stats = []

    for year in years:
        print(f"\nProcessing {year}...")
        partition_path = os.path.join(NIELSEN_PURCHASES_DIR, f'panel_year={year}')

        purchases = pd.read_parquet(partition_path)
        purchases['upc_13'] = harmonize_nielsen_upc(purchases['upc'])

        n_purchases = len(purchases)
        n_upcs = purchases['upc'].nunique()

        # Left join: keep all Nielsen purchases
        merged = purchases.merge(syndigo_merge, on='upc_13', how='left')

        # Match stats
        has_nutrition = merged['calories'].notna()
        n_matched = has_nutrition.sum()
        matched_upcs = merged.loc[has_nutrition, 'upc'].nunique()

        print(f"  {n_purchases:,} purchases, "
              f"{n_matched:,} matched ({n_matched/n_purchases*100:.1f}%), "
              f"{matched_upcs:,}/{n_upcs:,} UPCs ({matched_upcs/n_upcs*100:.1f}%)")

        all_stats.append({
            'year': year,
            'total_purchases': n_purchases,
            'matched_purchases': int(n_matched),
            'purchase_match_pct': round(n_matched / n_purchases * 100, 1),
            'total_upcs': n_upcs,
            'matched_upcs': int(matched_upcs),
            'upc_match_pct': round(matched_upcs / n_upcs * 100, 1),
        })

        # Drop merge key, add panel_year, write
        merged = merged.drop(columns=['upc_13'])
        merged['panel_year'] = year
        merged.to_parquet(
            OUTPUT_DIR,
            partition_cols=['panel_year'],
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        del merged, purchases

    # Save match summary
    if all_stats:
        stats_df = pd.DataFrame(all_stats)
        stats_path = os.path.join(OUTPUT_DIR, 'match_rate_summary.csv')
        stats_df.to_csv(stats_path, index=False)
        print(f"\n{'=' * 80}")
        print("MATCH RATE SUMMARY")
        print("=" * 80)
        print(stats_df.to_string(index=False))
        print(f"\nSaved to: {stats_path}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("NIELSEN-SYNDIGO MERGER")
    print("=" * 80)

    # Phase 1: Build Syndigo master
    syndigo_master_path = os.path.join(BASE_DATA_DIR, 'interim', 'syndigo_master.parquet')

    if os.path.exists(syndigo_master_path):
        print(f"Loading cached Syndigo master from: {syndigo_master_path}")
        syndigo_master = pd.read_parquet(syndigo_master_path)
        print(f"  {len(syndigo_master):,} unique products")
    else:
        syndigo_master = build_syndigo_master()
        os.makedirs(os.path.dirname(syndigo_master_path), exist_ok=True)
        syndigo_master.to_parquet(syndigo_master_path, engine='pyarrow',
                                  compression='snappy', index=False)
        print(f"Saved Syndigo master: {syndigo_master_path}")

    # Phase 2: Merge with Nielsen
    merge_nielsen_with_syndigo(syndigo_master)

    print("\nDone.")


if __name__ == "__main__":
    main()
