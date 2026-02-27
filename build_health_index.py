"""
build_health_index.py

Construct the household-level Health Index (HI) following Allcott et al.
food_deserts_replication procedures exactly.

Steps (with replication file references):
  1. Load purchases_with_nutrition.parquet
  2. Drop reference card products
     (CollapseTransactions.do:26-27: drop product_module_code 445-468)
  3. Flag Fruit/Veg using product_module_code and product_group_code
     (UPCDataPrep.do:26-33)
  4. Compute HI per 100g
     (GetHealthIndex.do:8-13)
     - Fruit/Veg: Fruit*100/320 + Veg*100/390
     - Other: fiber/29.5 - sugar/32.8 - satfat/17.2 - sodium/2.3 - chol/0.3
  5. Compute HI per 1000 cal
     (GetHealthIndex.do:33: if cals_per1 > 1)
  6. Compute calories per purchase row
     (CollapseTransactions.do:13: cals_perRow = cals_per1 * quantity)
  7. Calorie-weighted collapse to household-year
     (CollapseTransactions.do:68-69: collapse (mean) ... [pw=cals_perRow])
  8. Merge panelist data (projection_factor, household_income_midpoint)
     from panelists_all_years.parquet (clean_panelist.py)
  9. Normalize HI to mean=0, sd=1 (weighted by projection_factor)
     (InsertHealthMeasures.do:125-136)
     - Mean: weighted, pooled across all years
     - SD: weighted SD of year-demeaned residuals

Input:
  - purchases_with_nutrition.parquet (from build_hi_panel.py)
    Expected columns: upc, product_module_code, product_group_code,
    product_module_normalized, cal_per_100g, fiber_per_100g, sugar_per_100g,
    satfat_per_100g, sodium_per_100g, chol_per_100g, g_total, quantity,
    household_code, panel_year, total_price_paid, imputed
  - panelists_all_years.parquet (from clean_panelist.py)
    Used columns: household_code, panel_year, projection_factor,
    household_income_midpoint

Output:
  - purchases_with_hi.parquet   (purchase-level with HI)
  - household_year_hi.parquet   (calorie-weighted household-year aggregates)
"""

import os
import pandas as pd
import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
PANEL_PATH    = os.path.join(BASE_DATA_DIR, 'interim', 'hi_panel',
                             'purchases_with_nutrition.parquet')
PANELIST_PATH = os.path.join(BASE_DATA_DIR, 'interim', 'panelists',
                             'panelists_all_years.parquet')
OUTPUT_DIR    = os.path.join(BASE_DATA_DIR, 'interim', 'hi_panel')

# HI formula components (per 100g)
HI_NUTRIENTS = ['fiber_per_100g', 'sugar_per_100g', 'satfat_per_100g',
                'sodium_per_100g', 'chol_per_100g']
HI_DIVISORS  = [29.5, 32.8, 17.2, 2.3, 0.3]
HI_SIGNS     = [1, -1, -1, -1, -1]

# Fixed HI per 100g for produce (GetHealthIndex.do:8)
FRUIT_HI_PER_100G = 100 / 320
VEG_HI_PER_100G   = 100 / 390

# ---------------------------------------------------------------------------
# Fruit/Veg classification by product_module_code and product_group_code
# Source: UPCDataPrep.do:26-33
# ---------------------------------------------------------------------------

# FreshFruit: UPCDataPrep.do:26
FRESH_FRUIT_MODULES = {453, 3560, 3563, 4010, 4085, 4180,
                       4225, 4230, 4355, 4470, 6049, 6050}

# Fruit (includes canned/dried/frozen): UPCDataPrep.do:27-28
FRUIT_EXTRA_MODULES = {6, 42, 2664}
FRUIT_GROUPS        = {504, 1010}

# FreshVeg: UPCDataPrep.do:30
FRESH_VEG_MODULES = {460, 3544, 4015, 4020, 4023, 4050, 4055, 4060,
                     4140, 4275, 4280, 4350, 4400, 4415, 4460, 4475,
                     6064, 6070}

# Veg (includes canned/frozen): UPCDataPrep.do:31-33
VEG_EXTRA_MODULES = {24, 96, 1316, 3565}
VEG_GROUPS        = {514, 2010}
# Excluded from Veg (cream corn, breaded/sauced frozen veg):
VEG_EXCLUDE_MODULES = {1071, 2618, 2635, 2637, 2638, 2639}

# Reference card products to drop (CollapseTransactions.do:26-27)
REF_CARD_MODULE_RANGE = range(445, 469)  # 445-468 inclusive


# ============================================================================
# HELPERS
# ============================================================================

def flag_fruit(df):
    """Flag Fruit=1 following UPCDataPrep.do:26-28."""
    mod = df['product_module_code']
    fresh = mod.isin(FRESH_FRUIT_MODULES)
    extra_mod = mod.isin(FRUIT_EXTRA_MODULES)

    grp = pd.Series(False, index=df.index)
    if 'product_group_code' in df.columns:
        grp = df['product_group_code'].isin(FRUIT_GROUPS)

    return (fresh | extra_mod | grp).astype(int)


def flag_veg(df):
    """Flag Veg=1 following UPCDataPrep.do:30-33."""
    mod = df['product_module_code']
    fresh = mod.isin(FRESH_VEG_MODULES)
    extra_mod = mod.isin(VEG_EXTRA_MODULES)
    excluded = mod.isin(VEG_EXCLUDE_MODULES)

    grp = pd.Series(False, index=df.index)
    if 'product_group_code' in df.columns:
        grp = df['product_group_code'].isin(VEG_GROUPS)

    return ((fresh | extra_mod | grp) & ~excluded).astype(int)


def compute_hi_standard(df):
    """Standard HI per 100g from the 5 nutrient columns.
    GetHealthIndex.do:11-13"""
    hi = pd.Series(0.0, index=df.index, dtype='float64')
    for col, div, sign in zip(HI_NUTRIENTS, HI_DIVISORS, HI_SIGNS):
        hi += sign * df[col].fillna(0) / div
    return hi



# ============================================================================
# MAIN
# ============================================================================

def main():
    print("BUILD HEALTH INDEX")
    print("=" * 80)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ------------------------------------------------------------------
    # STEP 1: Load purchases panel
    # ------------------------------------------------------------------
    print("\nLoading purchases panel...")
    df = pd.read_parquet(PANEL_PATH)
    print(f"  {len(df):,} purchase rows, {df['upc'].nunique():,} unique UPCs")
    print(f"  Columns: {list(df.columns)}")

    # drop panel_year > 2020 
    df = df[df['panel_year'] <= 2020].copy()

    # drop imputed products (not in original purchases data, added during nutrition merge)
    # if 'imputed' in df.columns:
    #     n_imputed = df['imputed'].sum()
    #     print(f"  Dropping {n_imputed:,} imputed rows (added during nutrition merge)")
    #     df = df[~df['imputed']].copy()

    # Ensure product_module_code exists
    if 'product_module_code' not in df.columns:
        print("\n  ERROR: product_module_code column not found.")
        print("  Add product_module_code to the purchases data first.")
        return

    # Cast to numeric if needed
    df['product_module_code'] = pd.to_numeric(df['product_module_code'],
                                               errors='coerce')
    if 'product_group_code' in df.columns:
        df['product_group_code'] = pd.to_numeric(df['product_group_code'],
                                                  errors='coerce')
    else:
        print("  WARNING: product_group_code not found — fruit/veg flags will"
              " use product_module_code only (misses some canned/frozen)")

    # ------------------------------------------------------------------
    # STEP 2: Drop reference card products
    # CollapseTransactions.do:26-27
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("DROPPING REFERENCE CARD PRODUCTS")

    n_before = len(df)

    # Drop product_module_code 445-468
    ref_card = df['product_module_code'].isin(REF_CARD_MODULE_RANGE)
    print(f"  Reference card (module 445-468): {ref_card.sum():,} rows")

    # Also drop by string match for any that lack numeric codes
    ref_card_str = df['product_module_normalized'].str.contains(
        'REFERENCE CARD', case=False, na=False)
    ref_card_combined = ref_card | ref_card_str
    print(f"  Reference card (string match):   {ref_card_str.sum():,} rows")
    print(f"  Combined:                        {ref_card_combined.sum():,} rows")

    # Also drop missing product_group_code if available
    # (CollapseTransactions.do:27: "| product_group_code==.")
    missing_grp = pd.Series(False, index=df.index)
    if 'product_group_code' in df.columns:
        missing_grp = df['product_group_code'].isna()
        print(f"  Missing product_group_code:      {missing_grp.sum():,} rows")

    drop_mask = ref_card_combined | missing_grp
    df = df[~drop_mask].copy()
    print(f"\n  Dropped: {n_before - len(df):,} rows")
    print(f"  Remaining: {len(df):,} rows, {df['upc'].nunique():,} UPCs")

    # ------------------------------------------------------------------
    # STEP 3: Flag Fruit and Veg
    # UPCDataPrep.do:26-33
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("FLAGGING FRUIT AND VEGETABLE PRODUCTS")

    df['Fruit'] = flag_fruit(df)
    df['Veg']   = flag_veg(df)

    n_fruit_rows = (df['Fruit'] == 1).sum()
    n_veg_rows   = (df['Veg'] == 1).sum()
    n_fruit_upcs = df.loc[df['Fruit'] == 1, 'upc'].nunique()
    n_veg_upcs   = df.loc[df['Veg'] == 1, 'upc'].nunique()

    print(f"  Fruit: {n_fruit_rows:,} purchases, {n_fruit_upcs:,} UPCs")
    print(f"  Veg:   {n_veg_rows:,} purchases, {n_veg_upcs:,} UPCs")

    # Show top fruit/veg modules
    for label, flag in [('Fruit', 'Fruit'), ('Veg', 'Veg')]:
        flagged = df[df[flag] == 1]
        if len(flagged) > 0:
            top_mods = (flagged.groupby('product_module_normalized')
                        .agg(n=('upc', 'size'),
                             spending=('total_price_paid', 'sum'))
                        .sort_values('spending', ascending=False)
                        .head(10))
            print(f"\n  Top 10 {label} modules by spending:")
            for mod_name, row in top_mods.iterrows():
                print(f"    ${row['spending']:>11,.0f}  {row['n']:>8,} purch  {mod_name}")

    # ------------------------------------------------------------------
    # STEP 4: Compute Health Index per 100g
    # GetHealthIndex.do:8-13
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("COMPUTING HEALTH INDEX")

    # First: fixed HI for fruit/veg (line 8)
    # rHealthIndex_per100g = Fruit*100/320 + Veg*100/390
    df['hi_per_100g'] = (df['Fruit'] * FRUIT_HI_PER_100G
                         + df['Veg'] * VEG_HI_PER_100G)

    # Then: standard formula for non-produce (lines 11-13)
    # "if Fruit==0 & Veg==0"
    non_produce = (df['Fruit'] == 0) & (df['Veg'] == 0)
    df.loc[non_produce, 'hi_per_100g'] = compute_hi_standard(df.loc[non_produce])

    # ------------------------------------------------------------------
    # STEP 5: Compute HI per 1000 calories
    # GetHealthIndex.do:33
    # "if cals_per1 > 1" — set missing for very low calorie items
    # ------------------------------------------------------------------

    # cals_per1 = cals_per100g * g_total / 100  (UPCDataPrep.do:17)
    df['cals_per_upc'] = df['cal_per_100g'] * df['g_total'] / 100

    df['hi_per_1000cal'] = np.where(
        df['cals_per_upc'] > 1,
        df['hi_per_100g'] / df['cal_per_100g'] * 1000,
        np.nan
    )

    has_hi = df['hi_per_1000cal'].notna()
    print(f"  Purchases with HI per 1000cal: {has_hi.sum():,} ({has_hi.mean():.1%})")
    print(f"  Fruit purchases: {n_fruit_rows:,}")
    print(f"  Veg purchases:   {n_veg_rows:,}")
    print(f"  Missing HI (no cals or cals<=1): {(~has_hi).sum():,}")

    # ------------------------------------------------------------------
    # STEP 6: Compute calories per purchase row
    # CollapseTransactions.do:13
    # cals_perRow = cals_per1 * quantity
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("COMPUTING CALORIES PER PURCHASE")

    df['cals_per_row'] = df['cals_per_upc'] * df['quantity']

    has_cals = df['cals_per_row'].notna() & (df['cals_per_row'] > 0)
    print(f"  Purchases with calories: {has_cals.sum():,} ({has_cals.mean():.1%})")
    print(f"  Purchases without (missing g_total or cal): "
          f"{(~has_cals).sum():,}")

    # ------------------------------------------------------------------
    # STEP 7: Calorie-weighted collapse to household-year
    # CollapseTransactions.do:68-69
    # collapse (rawsum) Calories=cals_perRow
    #          (mean) $Attributes_cals [pw=cals_perRow]
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("AGGREGATING TO HOUSEHOLD-YEAR LEVEL")

    # Keep only purchases with positive calories
    # (zero-cal items like salt/baking soda are automatically excluded
    #  by the calorie weighting — CollapseTransactions.do:69 comment)
    valid = df[has_cals & has_hi].copy()
    pct = len(valid) / len(df)
    print(f"  Valid purchases for aggregation: {len(valid):,} ({pct:.1%})")

    # Calorie-weighted mean of HI: equivalent to
    # collapse (mean) rHealthIndex_per1000Cal [pw=cals_perRow]
    valid['hi_x_cal'] = valid['hi_per_1000cal'] * valid['cals_per_row']

    hh_year = (valid.groupby(['household_code', 'panel_year'])
               .agg(
                   hi_x_cal_sum=('hi_x_cal', 'sum'),
                   total_calories=('cals_per_row', 'sum'),
                   total_spending=('total_price_paid', 'sum'),
                   n_purchases=('upc', 'size'),
                   fruit_share=('Fruit', 'mean'),
                   veg_share=('Veg', 'mean'),
               )
               .reset_index())

    # HI = calorie-weighted average
    hh_year['hi_household'] = hh_year['hi_x_cal_sum'] / hh_year['total_calories']
    hh_year = hh_year.drop(columns=['hi_x_cal_sum'])

    # Summary
    print(f"\n  Household-years: {len(hh_year):,}")
    print(f"  Unique households: {hh_year['household_code'].nunique():,}")
    print(f"  Years: {sorted(hh_year['panel_year'].unique())}")
    print(f"\n  HI distribution (raw, calorie-weighted, per 1000 cal):")
    hi_raw = hh_year['hi_household']
    print(f"    Mean:   {hi_raw.mean():.4f}")
    print(f"    Median: {hi_raw.median():.4f}")
    print(f"    Std:    {hi_raw.std():.4f}")
    print(f"    p10:    {hi_raw.quantile(0.10):.4f}")
    print(f"    p90:    {hi_raw.quantile(0.90):.4f}")

    # ------------------------------------------------------------------
    # STEP 8: Merge panelist data (projection_factor, household_income_midpoint)
    # Source: panelists_all_years.parquet (from clean_panelist.py)
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("MERGING PANELIST DATA")

    panelists = pd.read_parquet(
        PANELIST_PATH,
        columns=['household_code', 'panel_year', 'projection_factor',
                 'household_income_midpoint'])
    print(f"  Loaded {len(panelists):,} panelist-years")

    n_before_pf = len(hh_year)
    hh_year = hh_year.merge(panelists, on=['household_code', 'panel_year'], how='left')
    has_pf = hh_year['projection_factor'].notna()
    print(f"  Matched: {has_pf.sum():,} / {n_before_pf:,}")
    if (~has_pf).any():
        print(f"  WARNING: {(~has_pf).sum():,} household-years missing projection_factor")


    # ------------------------------------------------------------------
    # STEP 9: Normalize HI to mean=0, sd=1
    # InsertHealthMeasures.do:125-136
    #
    # Mean: weighted by projection_factor, pooled across all years
    # SD:   weighted SD of year-demeaned residuals
    #
    # Replication also restricts to InSample==1 (observed calories
    # > 5% of calorie need). We skip that restriction for now.
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("NORMALIZING HEALTH INDEX")

    w = hh_year['projection_factor']  # DO NOT fallback if missing, missing HHs are magnet

    # Weighted pooled mean across all household-years
    hi_mean = np.average(hh_year['hi_household'], weights=w)

    # Weighted year-demeaned SD:
    # 1. Compute weighted year means
    hh_year['_w_hi'] = hh_year['hi_household'] * w
    year_totals = hh_year.groupby('panel_year').agg(
        w_hi_sum=('_w_hi', 'sum'), w_sum=('projection_factor', lambda x: x.fillna(1).sum()))
    year_totals['w_mean'] = year_totals['w_hi_sum'] / year_totals['w_sum']
    hh_year = hh_year.merge(year_totals[['w_mean']], left_on='panel_year',
                            right_index=True, how='left')
    # 2. Residuals = HI - weighted year mean
    residuals = hh_year['hi_household'] - hh_year['w_mean']
    # 3. Weighted SD of residuals
    hi_sd = np.sqrt(np.average(residuals**2, weights=w))

    hh_year = hh_year.drop(columns=['_w_hi', 'w_mean'])

    print(f"  Weighted pooled mean: {hi_mean:.4f}")
    print(f"  Weighted year-demeaned SD: {hi_sd:.4f}")

    # Normalize: (raw - mean) / sd
    hh_year['hi_hh_normalized'] = (hh_year['hi_household'] - hi_mean) / hi_sd

    hi_norm = hh_year['hi_hh_normalized']
    print(f"\n  Normalized HI distribution:")
    print(f"    Mean:   {hi_norm.mean():.4f}")
    print(f"    Std:    {hi_norm.std():.4f}")
    print(f"    p10:    {hi_norm.quantile(0.10):.4f}")
    print(f"    p90:    {hi_norm.quantile(0.90):.4f}")

    # ------------------------------------------------------------------
    # Save outputs
    # ------------------------------------------------------------------
    purchase_path = os.path.join(OUTPUT_DIR, 'purchases_with_hi.parquet')
    df.to_parquet(purchase_path, index=False)
    print(f"\n  Saved purchase-level: {purchase_path}")
    print(f"  Shape: {df.shape}")

    hh_path = os.path.join(OUTPUT_DIR, 'household_year_hi.parquet')
    hh_year.to_parquet(hh_path, index=False)
    print(f"  Saved household-year: {hh_path}")
    print(f"  Shape: {hh_year.shape}")

    print("\nDone.")


if __name__ == '__main__':
    main()
