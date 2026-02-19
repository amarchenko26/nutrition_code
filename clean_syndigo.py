"""
Merge Nielsen Consumer Panel data with Syndigo product/nutrition data.

UPC Harmonization:
- Nielsen UPCs are 12 digits (string). Prepend '0' to get 13 digits.
- Syndigo UPCs are 14 digits (GTIN-14). Drop last digit (check digit) to get 13 digits.
- Merge on the shared 13-digit UPC key.
"""

import os
import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DATA_DIR           = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
SYNDIGO_DIR             = os.path.join(BASE_DATA_DIR, 'raw', 'syndigo')
OUTPUT_DIR              = os.path.join(BASE_DATA_DIR, 'interim', 'syndigo')

SYNDIGO_YEARS = list(range(2005, 2025))

# Core nutrients to pivot to wide format: NutrientMasterID -> column name
# CORE_NUTRIENTS = {
#     1: 'calories',
#     2: 'calories_from_fat',
#     3: '',
#     4: 'total_fat_g',
#     5: 'saturated_fat_g',
#     6: '',
#     7: '',
#     8: 'cholesterol_mg',
#     9: 'sodium_mg',
#     10: ''.
#     11: 'total_carbohydrate_g',
#     12: 'dietary_fiber_g',
#     13: 'total_sugars_g',
#     14: 'added_sugars_g',
#     15: 'sugar_alcohol_g',
#     16: 'protein_g',
#     17: '',
#     18: 'protein_g',
# }


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
    Load and combine the 2 Syndigo files for a single year into a
    long-format DataFrame with one row per UPC.
    """

    year_dir = os.path.join(SYNDIGO_DIR, str(year))
    if not os.path.isdir(year_dir):
        print(f"  Skipping {year}: directory not found")
        return None

    # --- Product csv (has package sizes) ---
    product_df = pd.read_csv(os.path.join(year_dir, get_product_filename(year)),
                                  encoding='latin-1', dtype={'UPC': str})
    product_df.columns = product_df.columns.str.lower()

    # --- ValuePrepared csv (has serving sizes kill me) ---
    value_prepared = pd.read_csv(os.path.join(year_dir, 'ValuePrepared.csv'),
                                 encoding='latin-1', dtype={'UPC': str})

    # Lowercase all column names for merging
    value_prepared.columns = value_prepared.columns.str.lower()

    # Keep only "as packaged" (type 0), drop "as prepared" rows
    value_prepared = value_prepared[value_prepared['valuepreparedtype'].astype(str) == '0']
    value_prepared = value_prepared.drop(columns=['valuepreparedtype'])

    # --- NutrientMaster (lookup table) ---
    nutrient_master = pd.read_csv(os.path.join(year_dir, 'NutrientMaster.csv'),
                                  encoding='latin-1', dtype={'UPC': str})

    # Lowercase all column names for merging 
    nutrient_master.columns = nutrient_master.columns.str.lower()

    # --- Nutrient ---
    nutrient_df = pd.read_csv(os.path.join(year_dir, 'Nutrient.csv'),
                              dtype={'UPC': str}, encoding='latin-1',
                              low_memory=False)

    nutrient_df.columns = nutrient_df.columns.str.lower()

    # Filter to nutrientmasterid between 1 and 30 inclusive
    nutrient_df = nutrient_df[
        (nutrient_df['nutrientmasterid'] >= 1) & (nutrient_df['nutrientmasterid'] <= 30)
    ].copy()

    # Drop nutrientmasterid 25 (Flavor)
    nutrient_df = nutrient_df[nutrient_df['nutrientmasterid'] != 25]

    # --- Merge Syndigo nutrient with their nutrient name ---
    merged = nutrient_df.merge(nutrient_master, on='nutrientmasterid', how='left')

    # Clean columns 
    merged = merged.rename(columns={
                                    'nutrientmasterid': 'nutrient_id',
                                    'name': 'nutrient'})

    # Keep only "as packaged" nutrients (type 0), then drop the column
    merged = merged[merged['valuepreparedtype'].astype(str) == '0']
    merged = merged.drop(columns=['valuepreparedtype', 'type'])

    # Merge item size and itemmeasure from Product.csv
    merged = merged.merge(product_df[['upc', 'itemsize', 'itemmeasure']],
                          on='upc', how='left')
    
    # Merge serving size info from ValuePrepared.csv
    merged = merged.merge(value_prepared[['upc', 'servingsizetext', 'servingsizeuom', 'servingspercontainer']],
                          on='upc', how='left')

    # Harmonize UPC: drop check digit -> 13 digits
    merged['upc'] = harmonize_syndigo_upc(merged['upc'])

    return merged


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("NIELSEN-SYNDIGO MERGER")
    print("=" * 80)

    # Create interim/syndigo/{year} directories if they don't exist
    for year in SYNDIGO_YEARS:
        year_dir = os.path.join(OUTPUT_DIR, str(year))
        os.makedirs(year_dir, exist_ok=True)

    # Use load_syndigo_year to load and harmonize each year's Syndigo data, then save as Parquet
    for year in SYNDIGO_YEARS:
        print(f"\nProcessing Syndigo year {year}...")
        syndigo_df = load_syndigo_year(year)
        if syndigo_df is not None:
            output_path = os.path.join(OUTPUT_DIR, str(year), 'syndigo_nutrients.parquet')
            syndigo_df.to_parquet(output_path, index=False)
            print(f"  Saved harmonized Syndigo data to {output_path}")
        else:
            print(f"  No data for year {year}, skipping.")

    print("\nDone.")


if __name__ == "__main__":
    main()
