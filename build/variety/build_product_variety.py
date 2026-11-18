"""
build_product_variety.py  (run on OSCAR)

Computes Jaravel-style product variety (new/exit spending shares) by module x county x year from Nielsen RMS scanner data,
food categories only, 2006-2020.

Reads  /users/amarche4/data/rms_variety/rms_upc_fips_spending_<year>.parquet, the barcode x county files of EVERY RMS store
       (build_upc_fips_spending_rms.py, from the store x barcode files of analyze/what_moves/build_store_upc_year_rms.py and each
       store-year's county). Until 2026-11-18 this script streamed the scanner archives itself and mapped stores to counties from
       the 2006 stores file only, which left out later stores (67-69% of RMS food spending kept in 2018-2020); that step is gone.
       Master_Files_2006-2020.tgz (products.tsv, for the module, group and department names)
Writes rms_variety_module_fips_year.parquet

Key definitions (following Jaravel 2018):
  new  = UPC not seen nationally in ANY year <= t-1
  exit = UPC not seen nationally in ANY year >= t+1
  ssnp = spending on new UPCs  / total spending (within module x county x year)
  ssep = spending on exit UPCs / total spending (within module x county x year)
"""

import tarfile
import gc
import io
import pandas as pd
import numpy as np
from pathlib import Path
import os, sys
# repo reorganized into stage folders: reach shared utils/ for food_filters
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'utils'))
from food_filters import DROP_DEPARTMENTS_PRE_2021, DROP_PRODUCT_GROUPS, DROP_PRODUCT_MODULES

# ============================================================
# SETTINGS
# ============================================================
import os
if os.path.exists('/users/amarche4'):   # OSCAR
    RMS_DIR = Path('/users/amarche4/data/rms/2006-2020_Scanner_Data')
    OUT_DIR = Path('/users/amarche4/data/rms_variety')
else:                                   # local
    RMS_DIR = Path('/Users/anyamarchenko/oscar_data/rms/2006-2020_Scanner_Data')
    OUT_DIR = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/rms_variety')

MASTER_TGZ = RMS_DIR / 'Master_Files_2006-2020.tgz'
OUT_DIR.mkdir(parents=True, exist_ok=True)

YEARS = list(range(2006, 2021))

# ============================================================
# HELPERS
# ============================================================
def log(msg):
    print(msg, flush=True)


def stream_master_file(targets, keep_cols):
    """Stream master tgz, return first matching target as DataFrame."""
    target_set = set(targets)
    with tarfile.open(MASTER_TGZ, 'r|gz') as tf:
        for member in tf:
            if member.name in target_set:
                f = io.BytesIO(tf.extractfile(member).read())
                df = pd.read_csv(f, sep='\t', low_memory=False, usecols=keep_cols)
                log(f"  Loaded {member.name}: {len(df):,} rows")
                return df
            tf.members = []
    return None


# ============================================================
# STEP 1: Products master
# ============================================================
def load_products_master():
    log("Loading products master...")
    targets = ['Master_Files_2006-2020/Latest/products.tsv'] + [
        f'Master_Files_2006-2020/Archive/{yr}/products.tsv'
        for yr in [2019, 2018, 2017, 2016, 2015, 2014, 2013]
    ]
    keep_cols = [
        'upc', 'upc_ver_uc', 'product_module_code', 'product_module_descr',
        'product_group_code', 'product_group_descr', 'department_code', 'department_descr',
    ]
    df = stream_master_file(targets, keep_cols)
    if df is None:
        raise FileNotFoundError("Could not find products.tsv in master tgz")
    return df


def get_food_products(products):
    """Filter products DataFrame to food — mirrors clean_nielsen.py drop lists."""
    mask = (
        ~products['department_descr'].isin(set(DROP_DEPARTMENTS_PRE_2021)) &
        ~products['product_group_descr'].isin(set(DROP_PRODUCT_GROUPS)) &
        ~products['product_module_descr'].isin(set(DROP_PRODUCT_MODULES))
    )
    food = products[mask].copy()
    log(f"  Food UPCs: {len(food):,} / {len(products):,} (dropped {len(products)-len(food):,})")
    log(f"  Food departments: {sorted(food['department_descr'].dropna().unique())}")
    return food


# ============================================================
# STEP 4: Build cumulative UPC availability sets (national)
# ============================================================
def build_upc_sets(year_dfs):
    """
    upc_upto[t]  = set of UPCs seen nationally in any year <= t  (for new flag)
    upc_post[t]  = set of UPCs seen nationally in any year >  t  (for exit flag)
    """
    years = sorted(year_dfs.keys())
    by_year = year_dfs  # already dict[year → set]

    upc_upto = {}
    cumulative = set()
    for yr in years:
        cumulative |= by_year[yr]
        upc_upto[yr] = cumulative.copy()

    upc_post = {}
    future = set()
    for yr in reversed(years):
        upc_post[yr] = future.copy()
        future |= by_year[yr]

    log(f"  UPC sets built for years {years[0]}-{years[-1]}")
    return upc_upto, upc_post


# ============================================================
# STEP 5: Compute variety measures for one year
# ============================================================
def compute_variety(year, df, upc_upto, upc_post):
    """
    Flag new/exit UPCs (nationally) and collapse to (module, zip) × year.
    """
    seen_before = upc_upto.get(year - 1, set())
    seen_after  = upc_post.get(year, set())

    df = df.copy()
    df['new']  = ~df['upc'].isin(seen_before)
    df['exit'] = ~df['upc'].isin(seen_after)
    df['spending_new']  = df['total_spending'] * df['new'].astype(float)
    df['spending_exit'] = df['total_spending'] * df['exit'].astype(float)

    grp = (df.groupby(['product_module_code', 'product_group_code', 'fips'], as_index=False)
           .agg(
               total_spending  = ('total_spending',  'sum'),
               spending_new    = ('spending_new',    'sum'),
               spending_exit   = ('spending_exit',   'sum'),
               n_upcs          = ('upc',             'nunique'),
               n_upcs_new      = ('new',             'sum'),
               n_upcs_exit     = ('exit',            'sum'),
           ))
    grp['year'] = year
    grp['ssnp'] = grp['spending_new']  / grp['total_spending'].replace(0, np.nan)
    grp['ssep'] = grp['spending_exit'] / grp['total_spending'].replace(0, np.nan)
    return grp


# ============================================================
# MAIN
# ============================================================
def main():
    # Step 1: products master (names of the food modules)
    products   = load_products_master()
    food_prods = get_food_products(products)
    del products
    desc = (food_prods[['product_module_code', 'product_module_descr',
                         'product_group_code', 'product_group_descr',
                         'department_code', 'department_descr']]
            .drop_duplicates('product_module_code').copy())
    del food_prods
    gc.collect()

    available_years = [yr for yr in YEARS
                       if (OUT_DIR / f'rms_upc_fips_spending_{yr}.parquet').exists()]
    if not available_years:
        log("No barcode x county files found: run build_upc_fips_spending_rms.py first.")
        return

    # Step 4: UPC availability sets — load only UPC column from cached parquets
    log("\nBuilding UPC availability sets...")
    by_year = {}
    for year in available_years:
        upcs = pd.read_parquet(OUT_DIR / f'rms_upc_fips_spending_{year}.parquet',
                               columns=['upc'])['upc'].unique()
        by_year[year] = set(upcs)
    upc_upto, upc_post = build_upc_sets(by_year)

    # Step 5: variety measures — one year at a time
    log("\nComputing variety measures...")
    results = []
    for year in sorted(available_years):
        df = pd.read_parquet(OUT_DIR / f'rms_upc_fips_spending_{year}.parquet')
        grp = compute_variety(year, df, upc_upto, upc_post)
        results.append(grp)
        log(f"  {year}: {len(grp):,} module-fips-year rows  "
            f"ssnp={grp['ssnp'].mean():.3f}  ssep={grp['ssep'].mean():.3f}")
        del df
        gc.collect()

    out = pd.concat(results, ignore_index=True)

    out = out.merge(desc, on=['product_module_code', 'product_group_code'], how='left')

    out_path = OUT_DIR / 'rms_variety_module_fips_year.parquet'
    out.to_parquet(out_path, index=False)
    log(f"\nSaved: {out_path}")
    log(f"Columns: {list(out.columns)}")
    log(f"Years: {sorted(out['year'].unique())}")
    log(f"Modules: {out['product_module_code'].nunique()}")
    log(f"FIPS counties: {out['fips'].nunique()}")
    log(f"Mean ssnp: {out['ssnp'].mean():.4f}")
    log(f"Mean ssep: {out['ssep'].mean():.4f}")
    log("Done.")


if __name__ == '__main__':
    main()
