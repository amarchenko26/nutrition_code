"""
build_product_variety.py

Computes Jaravel-style product variety (new/exit spending shares) from
Nielsen RMS scanner data, food categories only, 2006-2020.

Pipeline:
  1. Load products master → food UPC metadata + food group/module codes
  2. Load stores master → store_code_uc → zip_code mapping
  3. For each year: stream RMS tgz (food groups only) → UPC×zip spending
     (cached to interim/ so each tgz is streamed only once)
  4. Build cumulative UPC availability sets across years (national)
  5. For each year: flag new/exit UPCs, collapse to module×zip×year
  6. Save rms_variety_module_zip_year.parquet

Key definitions (following Jaravel 2018):
  new  = UPC not seen nationally in ANY year <= t-1
  exit = UPC not seen nationally in ANY year >= t+1
  ssnp = spending on new UPCs  / total spending (within module×zip×year)
  ssep = spending on exit UPCs / total spending (within module×zip×year)

Movement file columns: store_code_uc, upc, week_end, units, prmult, price, feature, display
  spending per row = units * price / prmult
"""

import tarfile
import re
import gc
import io
import shutil
import tempfile
import pandas as pd
import numpy as np
from pathlib import Path
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

# SAMPLE_MODE = True processes only SAMPLE_N_GROUPS food groups — use for testing
SAMPLE_MODE     = False
SAMPLE_N_GROUPS = 2

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


def is_food_movement_file(member_name, food_group_codes, food_module_codes):
    """
    Return (is_food, group_code, module_code) for a tgz member path.
    Path pattern: .../Movement_Files/{group}_{year}/{module}_{year}.tsv
    """
    m = re.search(r'Movement_Files/(\d+)_\d+/(\d+)_\d+\.tsv$', member_name)
    if not m:
        return False, None, None
    group_code  = int(m.group(1))
    module_code = int(m.group(2))
    is_food = group_code in food_group_codes and module_code in food_module_codes
    return is_food, group_code, module_code


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
# STEP 2: Stores → store_code_uc → FIPS dict
# ============================================================
def load_stores():
    """
    Load store→FIPS mapping from the first available annual scanner tgz.
    Store locations don't change, so one year is sufficient.
    Returns dict: {store_code_uc (int): fips (str, 5-digit)}
    """
    log("Loading stores...")
    cache_path = OUT_DIR / 'store_fips_cache.parquet'
    if cache_path.exists():
        df = pd.read_parquet(cache_path)
        store_fips = dict(zip(df['store_code_uc'], df['fips']))
        log(f"  Loaded {len(store_fips):,} stores from cache")
        return store_fips

    keep_cols = ['store_code_uc', 'fips_state_code', 'fips_county_code']
    for year in YEARS:
        tgz_path = RMS_DIR / f'SCANNER_DATA_{year}.tgz'
        if not tgz_path.exists():
            continue
        # Try known path first (avoids streaming past all movement files)
        candidates = [
            f'nielsen_extracts/RMS/{year}/Annual_Files/stores_{year}.tsv',
            f'nielsen_extracts/RMS/{year}/stores_{year}.tsv',
        ]
        log(f"  Searching {tgz_path.name} for stores file...")
        with tarfile.open(tgz_path, 'r|gz') as tf:
            for member in tf:
                if member.name in candidates or re.search(r'stores.*\.tsv$', member.name, re.IGNORECASE):
                    f = io.BytesIO(tf.extractfile(member).read())
                    df = pd.read_csv(f, sep='\t', low_memory=False, usecols=keep_cols)
                    df = df.dropna(subset=keep_cols)
                    df['fips'] = (df['fips_state_code'].astype(int).astype(str).str.zfill(2) +
                                  df['fips_county_code'].astype(int).astype(str).str.zfill(3))
                    store_fips = dict(zip(df['store_code_uc'].astype(int), df['fips']))
                    log(f"  Mapped {len(store_fips):,} stores to FIPS from {member.name}")
                    df[['store_code_uc', 'fips']].to_parquet(cache_path, index=False)
                    log(f"  Cached to {cache_path.name}")
                    return store_fips
                tf.members = []
    raise FileNotFoundError("Could not find stores .tsv in any annual scanner tgz")


# ============================================================
# STEP 3: Stream year tgz → UPC×zip spending
# ============================================================
def process_year(year, food_group_codes, food_module_codes, store_fips, sample_groups=None):
    """
    Stream one year's RMS tgz, aggregate spending by (upc, group, module, fips).
    Caches result to parquet so each tgz is only streamed once.
    Returns DataFrame: upc, product_group_code, product_module_code, fips, total_spending, total_units
    """
    out_path = OUT_DIR / f'rms_upc_fips_spending_{year}.parquet'
    if out_path.exists():
        log(f"  {year}: loading cached {out_path.name}")
        return pd.read_parquet(out_path)

    tgz_path = RMS_DIR / f'SCANNER_DATA_{year}.tgz'
    if not tgz_path.exists():
        log(f"  {year}: {tgz_path.name} not found, skipping")
        return None

    log(f"  {year}: streaming {tgz_path.name}...")
    chunks = []
    files_read = 0
    GRP_COLS = ['upc', 'product_group_code', 'product_module_code', 'fips']

    with tarfile.open(tgz_path, 'r|gz') as tf:
        for member in tf:
            is_food, group_code, module_code = is_food_movement_file(
                member.name, food_group_codes, food_module_codes)

            if not is_food or member.isdir():
                tf.members = []
                continue

            if sample_groups is not None and group_code not in sample_groups:
                tf.members = []
                continue

            raw = tf.extractfile(member)
            if raw is None:
                tf.members = []
                continue

            tmp_fd, tmp_path = tempfile.mkstemp(suffix='.tsv', dir=OUT_DIR)
            try:
                with os.fdopen(tmp_fd, 'wb') as fp:
                    shutil.copyfileobj(raw, fp)  # streams in 16KB chunks, minimal RAM
                reader = pd.read_csv(
                    tmp_path, sep='\t', low_memory=False, chunksize=500_000,
                    usecols=['store_code_uc', 'upc', 'units', 'prmult', 'price'],
                    dtype={'store_code_uc': 'Int64', 'upc': str,
                           'units': float, 'prmult': float, 'price': float},
                )
                for sub in reader:
                    sub['fips'] = sub['store_code_uc'].map(store_fips)
                    sub = sub.dropna(subset=['fips'])
                    if sub.empty:
                        continue
                    sub['spending'] = sub['units'] * sub['price'] / sub['prmult'].clip(lower=1)
                    sub['product_group_code']  = group_code
                    sub['product_module_code'] = module_code
                    chunks.append(sub.groupby(GRP_COLS, as_index=False)
                                  .agg(total_spending=('spending', 'sum'),
                                       total_units=('units', 'sum')))
                files_read += 1
            except Exception as e:
                log(f"    Warning: {member.name}: {e}")
            finally:
                os.unlink(tmp_path)

            tf.members = []

            if len(chunks) >= 50:
                combined = pd.concat(chunks, ignore_index=True)
                chunks = [combined.groupby(GRP_COLS, as_index=False).sum()]
                gc.collect()
                log(f"    ...{files_read} files, {len(chunks[0]):,} UPC-FIPS pairs")

    if not chunks:
        log(f"  {year}: no food data found")
        return None

    df = pd.concat(chunks, ignore_index=True)
    df = df.groupby(GRP_COLS, as_index=False).sum()

    df.to_parquet(out_path, index=False)
    log(f"  {year}: saved {len(df):,} UPC-FIPS pairs ({files_read} files)")
    return df


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
def main(single_year=None):
    # Step 1: products master
    products   = load_products_master()
    food_prods = get_food_products(products)
    del products
    food_group_codes  = set(food_prods['product_group_code'].dropna().astype(int))
    food_module_codes = set(food_prods['product_module_code'].dropna().astype(int))
    desc = (food_prods[['product_module_code', 'product_module_descr',
                         'product_group_code', 'product_group_descr',
                         'department_code', 'department_descr']]
            .drop_duplicates('product_module_code').copy())
    del food_prods
    gc.collect()

    # Step 2: stores master
    store_fips = load_stores()

    if SAMPLE_MODE:
        sample_groups = set(sorted(food_group_codes)[:SAMPLE_N_GROUPS])
        log(f"\nSAMPLE MODE: processing {SAMPLE_N_GROUPS} groups: {sample_groups}")
    else:
        sample_groups = None
        log(f"\nFull run: {len(food_group_codes)} food groups, {len(food_module_codes)} food modules")

    # Step 3: stream tgz → cache per-year parquets
    if single_year is not None:
        # Job-array mode: process one year only
        years_to_stream = [single_year]
    else:
        years_to_stream = YEARS

    log("\nStreaming RMS tgz files...")
    for year in years_to_stream:
        df = process_year(year, food_group_codes, food_module_codes, store_fips, sample_groups)
        del df
        gc.collect()

    if single_year is not None:
        log("Year streaming complete. Exiting (combine step runs separately).")
        return

    # Steps 4-5: run only when all years are cached (combine mode)
    available_years = [yr for yr in YEARS
                       if (OUT_DIR / f'rms_upc_fips_spending_{yr}.parquet').exists()]
    if not available_years:
        log("No cached parquets found — run per-year jobs first.")
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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, default=None,
                        help='If given, only stream this year and cache to parquet (Step 3 only).')
    args = parser.parse_args()
    main(args.year)
