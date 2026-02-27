"""
Build the Health Index (HI) panel: pivot Syndigo nutrients wide, merge onto
Nielsen purchase transactions, and report merge rates by year.

Input:
  - Syndigo-Nielsen merged nutrition (long): syndigo_final.parquet
  - Nielsen purchases (hive-partitioned):    purchases_food_sample/panel_year=YYYY/

Output:
  - Purchase-level panel with nutrition:     hi_panel/purchases_with_nutrition.parquet
"""

import os
import pandas as pd
import pyarrow.dataset as ds
import pyarrow as pa

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DATA_DIR  = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
SYNDIGO_PATH   = os.path.join(BASE_DATA_DIR, 'interim', 'syndigo_nielsen_merged', 'syndigo_final.parquet')
PURCHASES_DIR  = os.path.join(BASE_DATA_DIR, 'interim', 'purchases_food_sample')
OUTPUT_DIR     = os.path.join(BASE_DATA_DIR, 'interim', 'hi_panel')

# Nutrient name (from Syndigo) -> short column name for wide format
NUTRIENT_COL_MAP = {
    'Calories':            'cal_per_100g',
    'Total Fat':           'totfat_per_100g',
    'Saturated Fat':       'satfat_per_100g',
    'Polyunsaturated Fat': 'pofat_per_100g',
    'Monounsaturated Fat': 'mofat_per_100g',
    'Cholesterol':         'chol_per_100g',
    'Sodium':              'sodium_per_100g',
    'Dietary Fiber':       'fiber_per_100g',
    'Sugars':              'sugar_per_100g',
}

# The 5 nutrients used in the HI formula
HI_NUTRIENTS = ['fiber_per_100g', 'sugar_per_100g', 'satfat_per_100g',
                'sodium_per_100g', 'chol_per_100g']


# ============================================================================
# HELPERS
# ============================================================================

def harmonize_nielsen_upc(upc_series):
    """Nielsen 12-digit UPC -> prepend '0' -> 13 digits."""
    return '0' + upc_series.astype(str).str.zfill(12)


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("BUILD HI PANEL")
    print("=" * 80)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ------------------------------------------------------------------
    # STEP 1: Load Syndigo (long) and pivot wide
    # ------------------------------------------------------------------
    print("\nLoading Syndigo nutrition (long format)...")
    syndigo = pd.read_parquet(SYNDIGO_PATH)
    print(f"  {len(syndigo):,} rows, {syndigo['upc'].nunique():,} unique UPCs")

    # Pivot nut_per_100g: one row per UPC, one column per nutrient
    print("\nPivoting nutrients wide...")
    wide = syndigo.pivot_table(index='upc', columns='nutrient',
                               values='nut_per_100g', aggfunc='first')
    wide.columns = [NUTRIENT_COL_MAP[c] for c in wide.columns]
    wide = wide.reset_index()

    # Grab per-UPC fields that are constant across nutrient rows
    upc_fields = (syndigo.groupby('upc')[['g_total', 'g_serving_size']]
                         .first()
                         .reset_index())
    wide = wide.merge(upc_fields, on='upc', how='left')

    # Stats
    n_upcs = len(wide)
    has_all_hi = wide[HI_NUTRIENTS].notna().all(axis=1).sum()
    has_cal = wide['cal_per_100g'].notna().sum()
    print(f"  {n_upcs:,} UPCs after pivot")
    print(f"  UPCs with all 5 HI nutrients: {has_all_hi:,} ({has_all_hi/n_upcs:.1%})")
    print(f"  UPCs with calories: {has_cal:,} ({has_cal/n_upcs:.1%})")

    # ------------------------------------------------------------------
    # STEP 2: Load Nielsen purchases and merge
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("MERGING WITH NIELSEN PURCHASES")

    # Load all purchases at once via pyarrow dataset
    print("\nLoading Nielsen purchases...")
    dataset = ds.dataset(PURCHASES_DIR, format='parquet', partitioning='hive',
                         exclude_invalid_files=True)
    # Exclude size1_amount (double in 2004-2020, string in 2021-2024) and
    # size1_units — not needed here (only used in merge_nielsen_syn.py fallback)
    drop_cols = {'size1_amount', 'size1_units'}
    keep_cols = [f.name for f in dataset.schema if f.name not in drop_cols]
    purchases = dataset.to_table(columns=keep_cols).to_pandas()
    print(f"  {len(purchases):,} purchase rows, {purchases['upc'].nunique():,} unique UPCs")

    # Harmonize Nielsen UPCs
    purchases['upc'] = harmonize_nielsen_upc(purchases['upc'])

    # Left-merge purchases onto nutrition
    print("\nMerging...")
    merged = purchases.merge(wide, on='upc', how='left', indicator=True)

    # ------------------------------------------------------------------
    # Merge rate reporting
    # ------------------------------------------------------------------
    print("\nMERGE RATES BY YEAR")
    print("-" * 80)
    print(f"  {'Year':<6} {'Purch Rows':>12} {'Matched':>12} {'% Rows':>8}"
          f"  {'UPCs':>8} {'Matched':>8} {'% UPCs':>8}")
    print("  " + "-" * 72)

    overall_rows = 0
    overall_matched_rows = 0
    overall_upcs = 0
    overall_matched_upcs = 0

    for year in sorted(merged['panel_year'].unique()):
        yr = merged[merged['panel_year'] == year]
        n_rows = len(yr)
        n_matched_rows = (yr['_merge'] == 'both').sum()
        n_upcs = yr['upc'].nunique()
        n_matched_upcs = yr.loc[yr['_merge'] == 'both', 'upc'].nunique()

        overall_rows += n_rows
        overall_matched_rows += n_matched_rows
        overall_upcs += n_upcs
        overall_matched_upcs += n_matched_upcs

        print(f"  {year:<6} {n_rows:>12,} {n_matched_rows:>12,} {n_matched_rows/n_rows:>7.1%}"
              f"  {n_upcs:>8,} {n_matched_upcs:>8,} {n_matched_upcs/n_upcs:>7.1%}")

    print("  " + "-" * 72)
    print(f"  {'Total':<6} {overall_rows:>12,} {overall_matched_rows:>12,} "
          f"{overall_matched_rows/overall_rows:>7.1%}"
          f"  {overall_upcs:>8,} {overall_matched_upcs:>8,} "
          f"{overall_matched_upcs/overall_upcs:>7.1%}")

    merged = merged.drop(columns=['_merge'])

    # ------------------------------------------------------------------
    # STEP 3a: Manually add missing nutrition for some UPCs
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("Manuallay add MISSING NUTRITION")

    # Add manual nutrition for "EGGS FRESH" product_module_normalized
    eggs_mask = merged['product_module_normalized'] == 'EGGS FRESH'
    if eggs_mask.any():
        print(f"\n  Adding nutrition for {eggs_mask.sum():,} purchases of EGGS FRESH")
        merged.loc[eggs_mask, 'cal_per_100g'] = 155
        merged.loc[eggs_mask, 'satfat_per_100g'] = 3.1
        merged.loc[eggs_mask, 'sodium_per_100g'] = 124
        merged.loc[eggs_mask, 'chol_per_100g'] = 373
        merged.loc[eggs_mask, 'fiber_per_100g'] = 0
        merged.loc[eggs_mask, 'sugar_per_100g'] = 1.1

    # ------------------------------------------------------------------
    # STEP 3b: Impute missing nutrition using product-module averages
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("IMPUTING MISSING NUTRITION (MODULE AVERAGES)")

    nutrient_cols = list(NUTRIENT_COL_MAP.values())

    # Identify matched vs unmatched at purchase level
    has_nutrition = merged[nutrient_cols].notna().any(axis=1)

    # Compute module means at UPC level (not purchase-weighted)
    matched_upc_nutrition = (merged.loc[has_nutrition]
                             .drop_duplicates(subset='upc')
                             .groupby('product_module')[nutrient_cols]
                             .mean())

    n_modules_with_data = len(matched_upc_nutrition)
    n_modules_total = merged['product_module'].nunique()
    print(f"  Modules with nutrition data: {n_modules_with_data:,} / {n_modules_total:,}")

    # Fill unmatched purchases with module averages
    merged['imputed'] = 0
    unmatched = ~has_nutrition

    for col in nutrient_cols:
        merged.loc[unmatched, col] = (
            merged.loc[unmatched, 'product_module'].map(matched_upc_nutrition[col]))

    # Mark as imputed where we actually filled values
    newly_filled = unmatched & merged[nutrient_cols].notna().any(axis=1)
    merged.loc[newly_filled, 'imputed'] = 1

    # Stats
    n_imputed_rows = newly_filled.sum()
    n_imputed_upcs = merged.loc[newly_filled, 'upc'].nunique()
    still_missing = unmatched & ~newly_filled
    n_still_missing_rows = still_missing.sum()
    n_still_missing_upcs = merged.loc[still_missing, 'upc'].nunique()

    print(f"  Imputed: {n_imputed_rows:,} purchases ({n_imputed_rows/len(merged):.1%}), "
          f"{n_imputed_upcs:,} UPCs")
    print(f"  Still missing (no module data): {n_still_missing_rows:,} purchases, "
          f"{n_still_missing_upcs:,} UPCs")

    # print some examples of modules with no nutrition data
    no_data_modules = merged.loc[still_missing, 'product_module'].unique()
    print(f"\n  Examples of modules with no nutrition data (total {len(no_data_modules):,}):")
    print(f"  {no_data_modules[:50]}")

    # Coverage after imputation
    has_all_hi_final = merged[HI_NUTRIENTS].notna().all(axis=1).sum()
    print(f"  Purchases with all 5 HI nutrients after imputation: "
          f"{has_all_hi_final:,} ({has_all_hi_final/len(merged):.1%})")

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------
    output_path = os.path.join(OUTPUT_DIR, 'purchases_with_nutrition.parquet')
    merged.to_parquet(output_path, index=False)
    print(f"\n  Saved to {output_path}")
    print(f"  Shape: {merged.shape}")
    print("\nDone.")


if __name__ == "__main__":
    main()
