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

# Nutrients to keep: NutrientMasterID -> (name, standard unit on label)
NUTRIENTS_TO_KEEP = {
    1:  ('Calories',            'kcal'),
    4:  ('Total Fat',           'g'),
    5:  ('Saturated Fat',       'g'),
    6:  ('Polyunsaturated Fat', 'g'),
    7:  ('Monounsaturated Fat', 'g'),
    8:  ('Cholesterol',         'mg'),
    9:  ('Sodium',              'mg'),
    12: ('Dietary Fiber',       'g'),
    15: ('Sugars',              'g'),
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


def convert_itemsize_to_grams(itemsize, itemmeasure):
    """
    Convert itemsize + itemmeasure to grams.

    Returns a Series of grams (NaN where unit is unrecognized or count-based).
    Logs a summary of unrecognized units so you can check coverage.
    """
    # Conversion factors to grams
    UNIT_TO_GRAMS = {
        # Weight units
        'oz':    28.3495,
        'lb':    453.592,
        'g':     1.0,
        'kg':    1000.0,
        'mg':    0.001,
        # Volume units (approximated as water density)
        'fl oz': 29.5735,
        'ml':    1.0,
        'cl':    10.0,
        'cc':    1.0,
        'l':     1000.0,
        'cup':   236.588,
        'tbsp':  14.787,
        'tsp':   4.929,
        'pt':    473.176,
        'qt':    946.353,
        'gal':   3785.41,
    }

    # Normalize itemmeasure: lowercase, strip, fix typos/variants
    # Built from all 248 unique itemmeasure values across Syndigo 2005-2024
    normalized = itemmeasure.astype(str).str.strip().str.lower().str.replace(r'\s+', ' ', regex=True)
    normalized = normalized.replace({
        # --- Ounces (oz = 28.3495g) ---
        '0z': 'oz', 'ozoz': 'oz', 'ozz': 'oz', 'ox': 'oz', 'iz': 'oz',
        'o z': 'oz', 'oz,': 'oz', 'oz.': 'oz', 'eoz': 'oz', 'ioz': 'oz',
        'zo': 'oz', 'z': 'oz', 'oe': 'oz', 'os': 'oz', 'ounces': 'oz', 'ounce': 'oz',
        # --- Fluid ounces (fl oz = 29.5735g) ---
        'fl': 'fl oz', 'fl ': 'fl oz', 'f. oz': 'fl oz', 'fl.oz': 'fl oz',
        'floz': 'fl oz', 'foz': 'fl oz', '12 fl': 'fl oz',
        'fluid ounce': 'fl oz', 'fluid ounces': 'fl oz',
        # --- Pounds (lb = 453.592g) ---
        'lbs': 'lb', '1b': 'lb', 'ib': 'lb',
        # --- Grams ---
        'gr': 'g', 'gm': 'g', 'gram': 'g', 'grams': 'g',
        # --- Liters (l = 1000g) ---
        'lt': 'l', 'ltr': 'l', 'liter': 'l',
        # --- Gallons (gal = 3785.41g) ---
        'gl': 'gal', 'ga': 'gal',
        # --- Pints (pt = 473.176g) ---
        'pz': 'pt', 'pint': 'pt', 'pt.': 'pt',
        # --- Quarts (qt = 946.353g) ---
        'quart': 'qt', 'qts': 'qt', 'ql': 'qt',
        # --- Cups (cup = 236.588g) ---
        'cups': 'cup', 'cu': 'cup',
        'cup dry': 'cup', 'cup dry mix': 'cup', 'cup mix': 'cup', 'cup condensed soup': 'cup',
        # --- Tablespoons (tbsp = 14.787g) ---
        'tablespoon': 'tbsp', 'tablespoons': 'tbsp', 'tbs': 'tbsp',
        'tbsp unpopped': 'tbsp',
        # --- Teaspoons (tsp = 4.929g) ---
        'teaspoon': 'tsp', 'teaspoons': 'tsp',
        # --- Count-based units (can't convert to grams) ---
        'ea': 'ea', 'each': 'ea', 'ea.': 'ea', 'ea`': 'ea',
        'eaa': 'ea', 'eae': 'ea', 'eaea': 'ea', 'eas': 'ea', 'eaw': 'ea',
        'eea': 'ea', 'fea': 'ea', 'cea': 'ea', 'oea': 'ea', 'ae': 'ea',
        'a': 'ea', 'e': 'ea', 'es': 'ea', 'unit': 'ea',
        'ct': 'ea', 'pc': 'ea', 'pcs': 'ea', 'pics': 'ea',
        'pk': 'ea', 'pkg': 'ea', 'pack': 'ea', 'packs': 'ea', 'pak': 'ea',
        'bar': 'ea', 'bars': 'ea', 'bag': 'ea', 'bags': 'ea',
        'box': 'ea', 'case': 'ea', 'cakes': 'ea', 'pans': 'ea', 'slice': 'ea',
        'slices': 'ea', 'eggs': 'ea', 'egg': 'ea', 'roll': 'ea', 'rolls': 'ea',
        'set': 'ea', '1 set': 'ea',
        'tabs': 'ea', 'tab': 'ea', 'tablet': 'ea', 'tablets': 'ea',
        'pills': 'ea', 'caps': 'ea', 'cpsl': 'ea',
        'gels': 'ea', 'softgel': 'ea', 'softgels': 'ea', 'gummies': 'ea',
        'pads': 'ea', 'wipes': 'ea', 'pr': 'ea', 'tea bag': 'ea',
        # Serving-size count units (from servingsizetext/servingsizeuom)
        'pieces': 'ea', 'piece': 'ea', 'package': 'ea', 'pckg': 'ea',
        'can': 'ea', 'cookies': 'ea', 'cookie': 'ea',
        'scoop': 'ea', 'scoops': 'ea', 'bottle': 'ea',
        'packet': 'ea', 'container': 'ea', 'pouch': 'ea',
        'crackers': 'ea', 'chips': 'ea', 'wafers': 'ea',
        'waffles': 'ea', 'biscuits': 'ea', 'envelope': 'ea',
        # Food items (count-based)
        'bagel': 'ea', 'bowl': 'ea', 'brownie': 'ea', 'bun': 'ea',
        'cake': 'ea', 'cone': 'ea', 'donut': 'ea', 'link': 'ea',
        'meal': 'ea', 'meal with sauce': 'ea', 'muffin': 'ea',
        'olives': 'ea', 'pastries': 'ea', 'pastry': 'ea', 'patty': 'ea',
        'pieces tofu': 'ea', 'pizza': 'ea', 'pop': 'ea',
        'sandwich': 'ea', 'snack': 'ea', 'stick': 'ea', 'tortilla': 'ea',
        'actijube': 'ea',
        # Containers
        'carton': 'ea', 'jar': 'ea', 'tub': 'ea', 'tube': 'ea',
        # Compound units (itemsize is a count, unit has per-item size baked in)
        '6 oz': 'ea', '12 oz': 'ea', '16 oz': 'ea',
        # Non-food measurement units
        'ft': 'ea', 'sq ft': 'ea', 'in': 'ea', 'cu in': 'ea',
        'cm': 'ea', 'yd': 'ea', 'yds': 'ea', 'm': 'ea',
        # Junk values
        'none': 'unknown', 'nan': 'unknown',
        's': 'unknown', 'v': 'unknown', 'oa': 'unknown', 'ra': 'unknown',
        'lz': 'unknown', 'ln': 'unknown', 'kb': 'unknown',
    })

    # Map normalized units to conversion factors
    conversion = normalized.map(UNIT_TO_GRAMS)
    size_numeric = pd.to_numeric(itemsize, errors='coerce')
    grams = size_numeric * conversion

    # Log unrecognized units
    unrecognized = normalized[conversion.isna() & (normalized != 'ea') & (normalized != 'unknown')]
    if len(unrecognized) > 0:
        counts = unrecognized.value_counts()
        print(f"    Unrecognized itemmeasure units ({len(unrecognized)} rows): "
              f"{dict(counts.head(15))}")

    return grams




def standardize_nutrient_to_grams(quantity, uom, nutrient_id):
    """
    Convert nutrient quantities to grams based on their UOM field.

    For calories (nutrient_id=1), returns the value as-is (kcal, not grams).
    For all other nutrients, converts to grams.
    Assumes standard unit when UOM is junk/unrecognized.

    Built from all 637 unique UOM values across Syndigo 2005-2024.
    """
    quantity = pd.to_numeric(quantity, errors='coerce')
    normalized = uom.astype(str).str.strip().str.lower().str.replace(r'\s+', ' ', regex=True)

    # --- Classify UOM into: grams, milligrams, micrograms, or unknown ---
    is_g = normalized.isin([
        'g', 'gm', 'gr', 'grams', 'gram', 'grm', 'gms', 'grm',
        'g*', 'gf', 'gb', 'gt',
    ]) | normalized.str.match(r'^g\d')  # catches g0, g3, g7, g8, g00, g10, g13, g15, g32

    is_mg = normalized.isin([
        'mg', 'mmg', 'mng', 'mh', 'mt', 'mf', 'cmg', '3mg', '0mg',
        'mgs', 'mgr', 'mgm', 'mgg', 'vmg', '-mg', ',mg',
    ]) | normalized.str.match(r'^mg\d')  # catches mg0, mg1, mg2, mg5, mg8, mg16, etc.

    is_mcg = normalized.isin([
        'mcg', 'µg', 'ug', 'micro', 'mcxg', 'mctg', 'mcjg', 'mccg',
        'mcmg', 'mch', 'omcg', 'mmcg', 'ncg',
    ])

    is_kcal = normalized.isin(['kcal', 'cal', 'calor', 'cals', 'kacl', 'kccal', 'kal'])

    # --- Compute conversion factor to grams ---
    # Default: assume standard unit for this nutrient
    _, std = NUTRIENTS_TO_KEEP.get(nutrient_id, (None, 'g'))
    if std == 'kcal':
        # Calories: return as-is, no grams conversion
        return quantity
    elif std == 'mg':
        default_factor = 0.001  # assume mg, convert to grams
    else:
        default_factor = 1.0    # assume grams

    factor = pd.Series(default_factor, index=quantity.index)
    factor[is_g] = 1.0
    factor[is_mg] = 0.001
    factor[is_mcg] = 0.000001
    factor[is_kcal] = float('nan')  # kcal for a non-calorie nutrient is nonsensical

    return quantity * factor


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
                                  encoding='latin-1', dtype={'UPC': str},
                                  low_memory=False)
    product_df.columns = product_df.columns.str.lower()



    # --- ValuePrepared csv (has serving sizes kill me) ---
    value_prepared = pd.read_csv(os.path.join(year_dir, 'ValuePrepared.csv'),
                                 encoding='latin-1', dtype={'UPC': str},
                                 on_bad_lines='warn', low_memory=False)

    # Lowercase all column names for merging
    value_prepared.columns = value_prepared.columns.str.lower()

    # Keep only "as packaged" (min type: 0 in most years, 1 in 2008)
    vp_type = pd.to_numeric(value_prepared['valuepreparedtype'], errors='coerce')
    value_prepared = value_prepared[vp_type == vp_type.min()]
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
        (nutrient_df['nutrientmasterid'].isin(NUTRIENTS_TO_KEEP))].copy()



    # --- Merge Syndigo nutrient with their nutrient name ---
    merged = nutrient_df.merge(nutrient_master, on='nutrientmasterid', how='left')

    # Clean columns 
    merged = merged.rename(columns={
                                    'nutrientmasterid': 'nutrient_id',
                                    'name': 'nutrient'})

    # Keep only "as packaged" nutrients (min type = 0 in most years, 1 in 2008)
    vpt = pd.to_numeric(merged['valuepreparedtype'], errors='coerce')
    merged = merged[vpt == vpt.min()]

    # Merge item size and itemmeasure from Product.csv (dedupe to avoid row multiplication)
    product_cols = product_df[['upc', 'itemsize', 'itemmeasure']].drop_duplicates(subset='upc', keep='first')
    merged = merged.merge(product_cols, on='upc', how='left')

    # Merge serving size info from ValuePrepared.csv (dedupe to avoid row multiplication)
    vp_cols = value_prepared[['upc', 'servingsizetext', 'servingsizeuom', 'servingspercontainer']].drop_duplicates(subset='upc', keep='first')
    merged = merged.merge(vp_cols, on='upc', how='left')

    # Harmonize UPC: drop check digit -> 13 digits
    merged['upc'] = harmonize_syndigo_upc(merged['upc'])



    #------ Calculate nutrients per 100g
    # g_total = ItemSize converted to grams (using ItemMeasure, which is the unit for ItemSize)
    # g_serving_size = serving size in grams (direct from serving text/uom, or g_total / servingspercontainer)
    # nutrients_per_100g = nutrients_per_serving / g_serving_size × 100

    # Quantity, servingspercontainer are stored as negative if it's less than, just like in Nielsen. Take absolute value to get actual quantity.
    merged['quantity'] = pd.to_numeric(merged['quantity'], errors='coerce').abs()
    merged['servingspercontainer'] = pd.to_numeric(merged['servingspercontainer'], errors='coerce').abs()

    # Package weight in grams 
    merged['g_total'] = convert_itemsize_to_grams(merged['itemsize'], merged['itemmeasure'])

    # Nutrient quantity per serving in grams (must process per nutrient_id
    # because standardize_nutrient_to_grams needs the nutrient's default unit)
    parts = []
    for nut_id, group in merged.groupby('nutrient_id'):
        group = group.copy()
        group['g_nut_per_serving'] = standardize_nutrient_to_grams(
            group['quantity'], group['uom'], nut_id)
        parts.append(group)
    merged = pd.concat(parts)

    # Serving size in grams: prefer g_total / servingspercontainer (pure arithmetic),
    # fall back to servingsizetext/servingsizeuom (requires unit parsing)
    has_both = merged['g_total'].notna() & merged['servingspercontainer'].notna()
    merged['g_serving_size'] = pd.Series(float('nan'), index=merged.index)
    merged.loc[has_both, 'g_serving_size'] = (
        merged.loc[has_both, 'g_total'] / merged.loc[has_both, 'servingspercontainer']
    )

    missing_ss = merged['g_serving_size'].isna()
    from_text = convert_itemsize_to_grams(
        merged.loc[missing_ss, 'servingsizetext'], merged.loc[missing_ss, 'servingsizeuom'])
    merged.loc[missing_ss, 'g_serving_size'] = from_text

    # One formula: nutrient per 100g = per-serving nutrient / serving size × 100
    merged['nut_per_100g'] = (
        merged['g_nut_per_serving'] / merged['g_serving_size'] * 100
    )

    # Keep only the columns we need
    keep_cols = ['upc', 'nutrient_id', 'nutrient',
                 'quantity', 'uom', 'g_nut_per_serving',
                 'itemsize', 'itemmeasure', 'g_total',
                 'servingspercontainer', 'servingsizetext', 'servingsizeuom', 
                 'g_serving_size', 'nut_per_100g']
    merged = merged[[c for c in keep_cols if c in merged.columns]]

    return merged


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("SYNDIGO CLEANING")
    print("=" * 80)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ---- Load all years ----
    all_years = []
    for year in SYNDIGO_YEARS:
        print(f"\nProcessing {year}...")
        df = load_syndigo_year(year)
        if df is not None:
            df['syndigo_year'] = year
            print(f"  {df['upc'].nunique():,} UPCs, {len(df):,} nutrient rows")
            all_years.append(df)
        else:
            print(f"  Skipped (no data)")

    # ---- Pool all years ----
    print(f"\n{'=' * 80}")
    print("POOLING AND DEDUPLICATING")
    pooled = pd.concat(all_years, ignore_index=True)

    n_upc_years = pooled.groupby(['upc', 'syndigo_year']).ngroups
    n_upcs_total = pooled['upc'].nunique()
    years_per_upc = pooled.groupby('upc')['syndigo_year'].nunique()
    n_multi = (years_per_upc > 1).sum()

    print(f"  UPC-year combinations: {n_upc_years:,}")
    print(f"  Unique UPCs (before dedup): {n_upcs_total:,}")
    print(f"  UPCs in multiple years: {n_multi:,} ({n_multi/n_upcs_total:.1%})")

    # ---- Deduplicate: keep the most complete year per UPC ----
    # Score each (upc, year) by number of non-null nut_per_100g values
    completeness = (pooled.groupby(['upc', 'syndigo_year'])['nut_per_100g']
                         .apply(lambda x: x.notna().sum())
                         .reset_index(name='n_complete'))

    # For multi-year UPCs, check if nutrient values agree across years
    if n_multi > 0:
        multi_upcs = years_per_upc[years_per_upc > 1].index
        dupes = pooled[pooled['upc'].isin(multi_upcs)]

        # Per (upc, nutrient): std across years — 0 means identical values
        spread = dupes.groupby(['upc', 'nutrient_id'])['g_nut_per_serving'].std()
        # Average std across nutrients for each UPC
        avg_spread = spread.groupby('upc').mean()
        n_consistent = (avg_spread < 0.01).sum()
        n_changed = (avg_spread >= 0.01).sum()
        n_no_data = avg_spread.isna().sum()
        print(f"  Multi-year UPCs with consistent values: {n_consistent:,}")
        print(f"  Multi-year UPCs with changed values: {n_changed:,}")
        print(f"  Multi-year UPCs with no comparable data: {n_no_data:,}")

    # Pick best year per UPC: most complete, then most recent
    completeness = completeness.sort_values(
        ['upc', 'n_complete', 'syndigo_year'], ascending=[True, False, False])
    best = completeness.drop_duplicates(subset='upc', keep='first')[['upc', 'syndigo_year']]

    pooled = pooled.merge(best, on=['upc', 'syndigo_year'], how='inner')

    # ---- Final stats ----
    n_final = pooled['upc'].nunique()
    nut_counts = pooled.groupby('upc')['nut_per_100g'].apply(lambda x: x.notna().sum())
    g_total_avail = pooled.groupby('upc')['g_total'].first().notna().sum()

    print(f"\n  After dedup: {n_final:,} unique UPCs, {len(pooled):,} total rows")
    print(f"  UPCs with package weight (g_total): {g_total_avail:,} ({g_total_avail/n_final:.1%})")
    print(f"  UPCs with all {len(NUTRIENTS_TO_KEEP)} nutrients: {(nut_counts == len(NUTRIENTS_TO_KEEP)).sum():,}")
    print(f"  UPCs with 0 usable nutrients: {(nut_counts == 0).sum():,}")
    print(f"  Mean nutrients per UPC: {nut_counts.mean():.1f}")

    print(f"\n  Kept records by Syndigo year:")
    year_dist = pooled.groupby('syndigo_year')['upc'].nunique().sort_index()
    for yr, count in year_dist.items():
        print(f"    {yr}: {count:,} UPCs")

    # ---- Save ----
    output_path = os.path.join(OUTPUT_DIR, 'syndigo_nutrients_master.parquet')
    pooled.to_parquet(output_path, index=False)
    print(f"\n  Saved to {output_path}")
    print("\nDone.")


if __name__ == "__main__":
    main()
