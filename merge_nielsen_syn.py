"""
Merge Nielsen purchases with Syndigo nutrition data.

1. Load the pooled Syndigo master (from clean_syndigo.py)
2. Load Nielsen purchases, extract unique UPCs with size1_amount/size1_units
3. Merge on harmonized 13-digit UPC
4. For UPCs where Syndigo's g_total is missing, fall back to Nielsen's size1
5. Recalculate nut_per_100g for filled-in rows
"""

import os
import pandas as pd
import pyarrow.dataset as ds
import pyarrow as pa
from clean_syndigo import convert_itemsize_to_grams

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
SYNDIGO_PATH  = os.path.join(BASE_DATA_DIR, 'interim', 'syndigo', 'syndigo_nutrients_master.parquet')
PURCHASES_DIR = os.path.join(BASE_DATA_DIR, 'interim', 'purchases_food_sample')
OUTPUT_DIR    = os.path.join(BASE_DATA_DIR, 'interim', 'syndigo_nielsen_merged')


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
    print("\nLoading Nielsen purchases (extracting unique UPCs)...")
    # size1_amount is double in 2004-2020 but string in 2021-2024;
    # force all 3 columns to string to avoid Arrow type conflict
    schema = pa.schema([
        ('upc', pa.string()),
        ('size1_amount', pa.string()),
        ('size1_units', pa.string()),
    ])
    dataset = ds.dataset(PURCHASES_DIR, format='parquet', partitioning='hive',
                         schema=schema, exclude_invalid_files=True)
    purchases = dataset.to_table(columns=['upc', 'size1_amount', 'size1_units']).to_pandas()
    purchases['size1_amount'] = pd.to_numeric(purchases['size1_amount'], errors='coerce')
    print(f"  {len(purchases):,} purchase rows, {purchases['upc'].nunique():,} unique UPCs")

    # Harmonize Nielsen UPCs: 12-digit -> prepend '0' -> 13 digits
    purchases['upc'] = harmonize_nielsen_upc(purchases['upc'])

    # Get one size1 entry per UPC (most common non-null values)
    nielsen_sizes = (purchases[purchases['size1_amount'].notna()]
                     .groupby('upc')[['size1_amount', 'size1_units']]
                     .first()
                     .reset_index())
    print(f"  {len(nielsen_sizes):,} UPCs with size1 info")

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

    # Recalculate nut_per_100g wherever g_total was just filled
    recalc = merged['nut_per_100g'].isna() & merged['g_total'].notna() & merged['g_nut_total'].notna()
    merged.loc[recalc, 'nut_per_100g'] = (
        merged.loc[recalc, 'g_nut_total'] / merged.loc[recalc, 'g_total'] * 100)

    # ---- Final stats ----
    n_final = merged['upc'].nunique()
    g_total_avail = merged.groupby('upc')['g_total'].first().notna().sum()
    nut_counts = merged.groupby('upc')['nut_per_100g'].apply(lambda x: x.notna().sum())

    print(f"\n  Final: {n_final:,} unique UPCs")
    print(f"  UPCs with g_total: {g_total_avail:,} ({g_total_avail/n_final:.1%})")
    print(f"  UPCs with all 9 nutrients: {(nut_counts == 9).sum():,}")
    print(f"  UPCs with 0 usable nutrients: {(nut_counts == 0).sum():,}")
    print(f"  Mean nutrients per UPC: {nut_counts.mean():.1f}")

    # ---- Save ----
    output_path = os.path.join(OUTPUT_DIR, 'syndigo_final.parquet')
    merged.to_parquet(output_path, index=False)
    print(f"\n  Saved to {output_path}")
    print("\nDone.")


if __name__ == "__main__":
    main()
