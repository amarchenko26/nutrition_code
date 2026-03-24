"""
build_module_healthiness.py

Computes a supply-side healthiness score for each Nielsen product module,
using the same HI formula as build_hi_panel.py but applied to Syndigo
nutrient data (one UPC = one observation, equal-weighted within module).

Output: module_healthiness.parquet
  product_module_code, product_module_descr, product_group_code,
  product_group_descr, n_upcs_total, n_upcs_with_nutrients, pct_coverage,
  hi_per_100g (mean UPC-level HI), plus mean nutrients per 100g
"""

import pandas as pd
import numpy as np
from pathlib import Path
from food_filters import DROP_DEPARTMENTS_PRE_2021, DROP_PRODUCT_GROUPS, DROP_PRODUCT_MODULES
from merge_nielsen_syn import harmonize_nielsen_upc

# ============================================================
# PATHS
# ============================================================
base        = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
PRODUCTS_TSV = base / 'raw' / 'products.tsv'
SYNDIGO_DIR  = base / 'interim' / 'syndigo_nielsen_merged'
OUT_DIR      = base / 'interim' / 'rms_variety'

OUT_DIR.mkdir(parents=True, exist_ok=True)

# Fruit/veg flags (mirrors build_hi_panel.py)
FRUIT_GROUPS        = {'FRUIT - CANNED', 'FRUIT - DRIED', 'FRUIT'}
FRUIT_MODULES_FRESH = {4010, 4085, 4180, 4225, 4355, 4470}
VEG_GROUPS          = {'VEGETABLES - CANNED', 'VEGETABLES-FROZEN', 'VEGETABLES AND GRAINS - DRIED'}
VEG_MODULES_FRESH   = {4015, 4020, 4023, 4050, 4055, 4060, 4140, 4230, 4275, 4280,
                        4350, 4400, 4415, 4460, 4475}

NUTRIENT_COLS = ['fiber_per_100g', 'sugar_per_100g', 'satfat_per_100g',
                 'sodium_per_100g', 'chol_per_100g', 'cal_per_100g']


def log(msg):
    print(msg, flush=True)


# ============================================================
# STEP 1: Load products master → food UPCs + module mapping
# ============================================================
def load_products_master():
    log(f"Loading products master from {PRODUCTS_TSV}...")
    keep_cols = ['upc', 'product_module_code', 'product_module_descr',
                 'product_group_code', 'product_group_descr',
                 'department_code', 'department_descr']
    df = pd.read_csv(PRODUCTS_TSV, sep='\t', low_memory=False, encoding='latin-1',
                     usecols=keep_cols, dtype={'upc': str})
    log(f"  Loaded {len(df):,} rows")
    return df


def get_food_products(products):
    mask = (
        ~products['department_descr'].isin(set(DROP_DEPARTMENTS_PRE_2021)) &
        ~products['product_group_descr'].isin(set(DROP_PRODUCT_GROUPS)) &
        ~products['product_module_descr'].isin(set(DROP_PRODUCT_MODULES))
    )
    food = products[mask].copy()
    log(f"  Food UPCs: {len(food):,} / {len(products):,}")
    return food


# ============================================================
# STEP 2: Load Syndigo wide (one row per UPC, nutrients per 100g)
# ============================================================
def load_syndigo():
    path = SYNDIGO_DIR / 'syndigo_wide.parquet'
    log(f"Loading Syndigo from {path}...")
    df = pd.read_parquet(path, columns=['upc'] + NUTRIENT_COLS)
    log(f"  {len(df):,} UPCs with nutrient data")
    return df


# ============================================================
# STEP 3: Compute UPC-level HI score
# ============================================================
def compute_upc_hi(df):
    """
    Apply the HI formula per UPC (per 100g basis).
    Fruit/veg get fixed scores; all others use the nutrient formula.
    """
    is_fruit = (
        df['product_group_descr'].isin(FRUIT_GROUPS) |
        df['product_module_code'].isin(FRUIT_MODULES_FRESH) |
        df['product_module_descr'].str.contains('FROZEN FRUITS|FRUIT JUICE|FRUIT DRINK',
                                                  case=False, na=False)
    )
    is_veg = (
        df['product_group_descr'].isin(VEG_GROUPS) |
        df['product_module_code'].isin(VEG_MODULES_FRESH) |
        df['product_module_descr'].str.contains(
            'VEGETABLE.*FROZEN|TOMATO PASTE|TOMATO SAUCE|TOMATO PUREE|TOMATOES.*CANNED|TOMATOES.*STEWED|MUSHROOM',
            case=False, na=False)
    )
    is_fv = is_fruit | is_veg

    hi = np.where(
        is_fv,
        is_fruit.astype(float) * 100/320 + is_veg.astype(float) * 100/390,
        df['fiber_per_100g'].fillna(0)  /  29.5
        - df['sugar_per_100g'].fillna(0)  /  32.8
        - df['satfat_per_100g'].fillna(0) /  17.2
        - df['sodium_per_100g'].fillna(0) /   2.3
        - df['chol_per_100g'].fillna(0)   /   0.3
    )
    df = df.copy()
    df['hi_per_100g'] = hi
    df['is_fruit'] = is_fruit
    df['is_veg']   = is_veg
    return df


# ============================================================
# STEP 4: Collapse to module level
# ============================================================
def wavg(group, col, weight_col):
    w = group[weight_col]
    v = group[col]
    mask = v.notna() & w.notna()
    if mask.sum() == 0:
        return float('nan')
    return (v[mask] * w[mask]).sum() / w[mask].sum()


def collapse_to_module(df, food_prods, upc_spending=None):
    """
    Merge UPC nutrient scores onto food products master, then average
    within module. If upc_spending is provided (upc, total_spending),
    uses expenditure-weighted averages; otherwise equal-weighted.
    """
    # Count total UPCs per module (all food UPCs)
    module_total = (food_prods.groupby(
        ['product_module_code', 'product_module_descr',
         'product_group_code', 'product_group_descr'],
        as_index=False)
        .agg(n_upcs_total=('upc', 'nunique'))
    )

    # Merge nutrient scores onto food products
    merged = food_prods.merge(df[['upc', 'hi_per_100g'] + NUTRIENT_COLS],
                               on='upc', how='left')
    # A UPC has nutrients if it was actually matched to Syndigo (any nutrient non-null)
    has_nutrients = merged[NUTRIENT_COLS].notna().any(axis=1)
    scored = merged[has_nutrients].copy()

    if upc_spending is not None:
        scored = scored.merge(upc_spending[['upc', 'total_spending']], on='upc', how='left')
        scored['total_spending'] = scored['total_spending'].fillna(0)
        score_cols = ['hi_per_100g'] + NUTRIENT_COLS
        rows = []
        for mod, grp in scored.groupby('product_module_code'):
            row = {'product_module_code': mod,
                   'n_upcs_with_nutrients': grp['upc'].nunique()}
            for col in score_cols:
                row[col] = wavg(grp, col, 'total_spending')
            rows.append(row)
        module_nutrients = pd.DataFrame(rows)
    else:
        module_nutrients = (scored
                            .groupby('product_module_code', as_index=False)
                            .agg(n_upcs_with_nutrients=('upc', 'nunique'),
                                 hi_per_100g=('hi_per_100g', 'mean'),
                                 **{col: (col, 'mean') for col in NUTRIENT_COLS}))

    out = module_total.merge(module_nutrients, on='product_module_code', how='left')
    out['pct_coverage'] = out['n_upcs_with_nutrients'] / out['n_upcs_total']

    # Component scores from actual nutrient data (all modules, including fruit/veg).
    # These use real Syndigo values — fruit/veg are NOT given a fixed score here.
    # Sign convention: positive = healthier contribution (matches HI formula).
    out['fiber_score']  =  out['fiber_per_100g']  / 29.5
    out['sugar_score']  = -out['sugar_per_100g']  / 32.8
    out['satfat_score'] = -out['satfat_per_100g'] / 17.2
    out['sodium_score'] = -out['sodium_per_100g'] /  2.3
    out['chol_score']   = -out['chol_per_100g']   /  0.3
    return out


# ============================================================
# MAIN
# ============================================================
def main():
    products   = load_products_master()
    food_prods = get_food_products(products)
    del products

    # Harmonize UPCs to 13 digits before any merge (Nielsen 12-digit → prepend '0')
    food_prods['upc'] = harmonize_nielsen_upc(food_prods['upc'])

    syndigo = load_syndigo()  # already 13-digit from merge_nielsen_syn.py

    log("Computing UPC-level HI scores...")
    merged = (food_prods[['upc', 'product_module_code', 'product_module_descr',
                           'product_group_code', 'product_group_descr']]
              .drop_duplicates('upc')
              .merge(syndigo, on='upc', how='left'))
    merged = compute_upc_hi(merged)

    log("Collapsing to module level...")
    spending_path = OUT_DIR / 'upc_total_spending.parquet'
    if spending_path.exists():
        upc_spending = pd.read_parquet(spending_path, columns=['upc', 'total_spending'])
        log(f"  Expenditure-weighting using {spending_path.name}")
    else:
        upc_spending = None
        log("  Equal-weighting (upc_total_spending.parquet not found)")
    out = collapse_to_module(merged, food_prods, upc_spending)

    out_path = OUT_DIR / 'module_healthiness.parquet'
    out.to_parquet(out_path, index=False)
    log(f"\nSaved: {out_path}")
    log(f"Modules: {len(out):,}")
    log(f"Median coverage: {out['pct_coverage'].median():.1%}")
    log(f"Modules with >50% coverage: {(out['pct_coverage'] > 0.5).sum()}")
    log(f"Mean HI across modules: {out['hi_per_100g'].mean():.4f}")


if __name__ == '__main__':
    main()
