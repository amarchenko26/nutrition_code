"""
Merge Nielsen purchases with Syndigo nutrition data.

1. Load the pooled Syndigo master (from clean_syndigo.py)
2. Load Nielsen purchases, extract unique UPCs with size1_amount/size1_units
3. Merge on harmonized 13-digit UPC
4. For UPCs where Syndigo's g_total is missing, fall back to Nielsen's size1
5. Recalculate g_serving_size and nut_per_100g for filled-in rows
"""

import os
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow as pa
from clean_syndigo import convert_itemsize_to_grams

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
SYNDIGO_PATH  = os.path.join(BASE_DATA_DIR, 'interim', 'syndigo', 'syndigo_nutrients_master.parquet')
PURCHASES_DIR = os.path.join(BASE_DATA_DIR, 'interim', 'purchases_food')
OUTPUT_DIR    = os.path.join(BASE_DATA_DIR, 'interim', 'syndigo_nielsen_merged')

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


# ============================================================================
# MAIN
# ============================================================================

def harmonize_nielsen_upc(upc_series):
    """Nielsen 12-digit UPC -> prepend '0' -> 13 digits."""
    return '0' + upc_series.astype(str).str.zfill(12)


def main():
    print("NIELSEN-SYNDIGO MERGE")
    print("=" * 80)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ---- Load Syndigo master ----
    print("\nLoading Syndigo master...")
    syndigo = pd.read_parquet(SYNDIGO_PATH)
    print(f"  {syndigo['upc'].nunique():,} UPCs, {len(syndigo):,} rows")

    # ---- Load Nielsen: extract unique UPCs with size info ----
    # Scan in batches to avoid loading 900M+ rows into memory
    print("\nLoading Nielsen purchases (extracting unique UPCs in batches)...")
    schema = pa.schema([
        ('upc', pa.string()),
        ('size1_amount', pa.string()),
        ('size1_units', pa.string()),
    ])
    dataset = ds.dataset(PURCHASES_DIR, format='parquet', partitioning='hive',
                         schema=schema, exclude_invalid_files=True)

    upc_size_dict = {}  # upc -> (size1_amount, size1_units)
    total_rows = 0
    for batch in dataset.to_batches(columns=['upc', 'size1_amount', 'size1_units'],
                                    batch_size=5_000_000):
        chunk = batch.to_pandas()
        total_rows += len(chunk)
        chunk['size1_amount'] = pd.to_numeric(chunk['size1_amount'], errors='coerce')
        chunk['upc'] = harmonize_nielsen_upc(chunk['upc'])

        # Keep first non-null size1 per UPC we haven't seen yet
        has_size = chunk['size1_amount'].notna()
        for _, row in chunk[has_size].drop_duplicates('upc').iterrows():
            if row['upc'] not in upc_size_dict:
                upc_size_dict[row['upc']] = (row['size1_amount'], row['size1_units'])

        print(f"    processed {total_rows:>13,} rows, {len(upc_size_dict):,} UPCs so far",
              end='\r')

    print(f"\n  {total_rows:,} purchase rows, {len(upc_size_dict):,} unique UPCs with size1 info")

    nielsen_sizes = pd.DataFrame([
        {'upc': upc, 'size1_amount': amt, 'size1_units': units}
        for upc, (amt, units) in upc_size_dict.items()
    ])

    # ---- Merge ----
    print("\nMerging...")
    merged = syndigo.merge(nielsen_sizes, on='upc', how='left', indicator=True)

    match_stats = merged.groupby('upc')['_merge'].first().value_counts()
    print(f"  Syndigo UPCs matched to Nielsen: {match_stats.get('both', 0):,}")
    print(f"  Syndigo UPCs not in Nielsen: {match_stats.get('left_only', 0):,}")
    merged = merged.drop(columns=['_merge'])

    # ---- Fill missing g_total from Nielsen's size1 ----
    missing_g = merged['g_total'].isna()
    has_size1 = merged['size1_amount'].notna()
    can_fill = missing_g & has_size1

    if can_fill.any():
        # Convert Nielsen's size1 to grams using the same function
        nielsen_grams = convert_itemsize_to_grams(
            merged.loc[can_fill, 'size1_amount'],
            merged.loc[can_fill, 'size1_units'])

        filled = nielsen_grams.notna()
        merged.loc[can_fill & filled.reindex(merged.index, fill_value=False), 'g_total'] = nielsen_grams[filled]

        n_filled_upcs = merged.loc[can_fill, 'upc'][filled.reindex(merged.loc[can_fill].index, fill_value=False)].nunique()
        print(f"\n  Filled g_total from Nielsen size1 for {n_filled_upcs:,} UPCs")

    # Fill serving size from package grams / servings where direct serving size is missing.
    missing_ss = (
        merged['g_serving_size'].isna()
        & merged['g_total'].notna()
        & merged['servingspercontainer'].notna()
    )
    merged.loc[missing_ss, 'g_serving_size'] = (
        merged.loc[missing_ss, 'g_total'] / merged.loc[missing_ss, 'servingspercontainer']
    )

    # Recalculate nut_per_100g using one formula.
    recalc = (
        merged['nut_per_100g'].isna()
        & merged['g_nut_per_serving'].notna()
        & merged['g_serving_size'].notna()
    )
    merged.loc[recalc, 'nut_per_100g'] = (
        merged.loc[recalc, 'g_nut_per_serving'] / merged.loc[recalc, 'g_serving_size'] * 100
    )

    # ---- Final stats ----
    n_final = merged['upc'].nunique()
    g_total_avail = merged.groupby('upc')['g_total'].first().notna().sum()
    nut_counts = merged.groupby('upc')['nut_per_100g'].apply(lambda x: x.notna().sum())

    print(f"\n  Final: {n_final:,} unique UPCs")
    print(f"  UPCs with g_total: {g_total_avail:,} ({g_total_avail/n_final:.1%})")
    print(f"  UPCs with all 9 nutrients: {(nut_counts == 9).sum():,}")
    print(f"  UPCs with 0 usable nutrients: {(nut_counts == 0).sum():,}")
    print(f"  Mean nutrients per UPC: {nut_counts.mean():.1f}")

    # ---- Pivot wide ----
    print("\nPivoting nutrients wide...")
    wide = merged[merged['nutrient'].isin(NUTRIENT_COL_MAP)].pivot_table(
        index='upc', columns='nutrient', values='nut_per_100g', aggfunc='first')
    wide.columns = [NUTRIENT_COL_MAP[c] for c in wide.columns]
    wide = wide.reset_index()

    upc_fields = merged.groupby('upc')[['g_total', 'g_serving_size']].first().reset_index()
    wide = wide.merge(upc_fields, on='upc', how='left')

    # Cap outliers (nutrients are in g/100g; these indicate unit misclassification)
    wide.loc[wide['sodium_per_100g'] > 5, 'sodium_per_100g'] = np.nan
    wide.loc[wide['chol_per_100g'] > 2, 'chol_per_100g'] = np.nan

    print(f"  {len(wide):,} UPCs, columns: {list(wide.columns)}")

    # ---- Save ----
    output_path = os.path.join(OUTPUT_DIR, 'syndigo_wide.parquet')
    wide.to_parquet(output_path, index=False)
    print(f"\n  Saved to {output_path}")
    print("\nDone.")


if __name__ == "__main__":
    main()
