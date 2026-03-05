"""
Build HH-year food expenditure panel.

Aggregates total_price_paid from purchases_food parquet to HH-year level,
broken down by health category and top product modules.

Output columns:
  household_code, panel_year
  spend_total          -- total real food spending (2013$)
  spend_produce        -- fresh/canned/frozen fruit + veg
  spend_whole_bread    -- whole wheat bread products
  spend_high_sugar     -- items with >100g sugar per 1000 cal (sweets/soda)
  spend_share_produce  -- produce / total
  spend_share_whole    -- whole bread / total bread spending
  spend_share_high_sugar
  spend_<module>       -- spending on top N modules (by national spending)

Saved to:
  interim/panel_dataset/expenditure_hh_year.parquet
  interim/panel_dataset/expenditure_hh_year.dta
"""

import pandas as pd
import numpy as np
from pathlib import Path
import gc
import warnings
warnings.filterwarnings('ignore')

BASE      = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
PURCHASES = BASE / 'interim' / 'purchases_food'
SYNDIGO   = BASE / 'interim' / 'syndigo_nielsen_merged' / 'syndigo_wide.parquet'
OUT_DIR   = BASE / 'interim' / 'panel_dataset'
OUT_DIR.mkdir(parents=True, exist_ok=True)

YEARS = range(2004, 2021)
CPI_BASE_YEAR = 2013
CPI = {
    2004: 188.9, 2005: 195.3, 2006: 201.6, 2007: 207.3, 2008: 215.3,
    2009: 214.5, 2010: 218.1, 2011: 224.9, 2012: 229.6, 2013: 233.0,
    2014: 236.7, 2015: 237.0, 2016: 240.0, 2017: 245.1, 2018: 251.1,
    2019: 255.7, 2020: 258.8,
}
CPI_BASE = CPI[CPI_BASE_YEAR]

# Module sets (same as build_hi_panel.py)
FRUIT_GROUPS        = {'FRUIT - CANNED', 'FRUIT - DRIED', 'FRUIT'}
FRUIT_MODULES_FRESH = {4010, 4085, 4180, 4225, 4355, 4470}
VEG_GROUPS          = {'VEGETABLES - CANNED', 'VEGETABLES-FROZEN', 'VEGETABLES AND GRAINS - DRIED'}
VEG_MODULES_FRESH   = {4015, 4020, 4023, 4050, 4055, 4060, 4140, 4230, 4275, 4280, 4350, 4400, 4415, 4460, 4475}
BREAD_MODULES       = {4000, 4001, 4002}

# High-sugar threshold: >100g sugar per 1000 cal
# Captures soda (~280g/1kcal), candy (~200g/1kcal), cookies/cakes (~100-150g/1kcal)
HIGH_SUGAR_THRESH = 100.0

# Top N product modules to track spending for
TOP_N_MODULES = 20

# ============================================================
# Load Syndigo — only sugar and calorie columns
# ============================================================
print("Loading Syndigo (sugar/cal)...")
syn = pd.read_parquet(SYNDIGO, columns=['upc', 'sugar_per_100g', 'cal_per_100g'])
syn = syn.dropna(subset=['sugar_per_100g', 'cal_per_100g'])
syn = syn[syn['cal_per_100g'] > 0]
syn['sugar_per_1000cal'] = syn['sugar_per_100g'] / syn['cal_per_100g'] * 1000
syn['high_sugar'] = (syn['sugar_per_1000cal'] > HIGH_SUGAR_THRESH).astype(int)
syn = syn[['upc', 'high_sugar']].drop_duplicates('upc')
print(f"  {len(syn):,} UPCs with sugar data, {syn['high_sugar'].mean()*100:.1f}% high-sugar")

# ============================================================
# First pass: find top modules by aggregate spending
# ============================================================
print("Finding top modules by total spending...")
module_spend = {}
for year in YEARS:
    part = PURCHASES / f'panel_year={year}'
    if not part.exists():
        continue
    p = pd.read_parquet(part, columns=['total_price_paid', 'product_module', 'product_module_code'])
    p = p.dropna(subset=['total_price_paid'])
    cpi_factor = CPI_BASE / CPI[year]
    p['spend_real'] = p['total_price_paid'] * cpi_factor
    for mod, grp in p.groupby('product_module_code'):
        mod = int(mod) if pd.notna(mod) else -1
        module_spend[mod] = module_spend.get(mod, 0) + grp['spend_real'].sum()
    del p
    gc.collect()

top_modules = sorted(module_spend, key=module_spend.get, reverse=True)[:TOP_N_MODULES]

# Get module names
mod_names = {}
for year in YEARS:
    part = PURCHASES / f'panel_year={year}'
    if not part.exists():
        continue
    p = pd.read_parquet(part, columns=['product_module', 'product_module_code'])
    p = p.dropna(subset=['product_module_code'])
    p['product_module_code'] = pd.to_numeric(p['product_module_code'], errors='coerce')
    for _, row in p[p['product_module_code'].isin(top_modules)].drop_duplicates('product_module_code').iterrows():
        code = int(row['product_module_code'])
        if code not in mod_names:
            mod_names[code] = row['product_module']
    if len(mod_names) == len(top_modules):
        break
    del p

print(f"Top {TOP_N_MODULES} modules:")
for m in top_modules:
    print(f"  {m}: {mod_names.get(m, 'unknown')} (${module_spend[m]/1e6:.0f}M total)")

# ============================================================
# Second pass: aggregate to HH-year
# ============================================================
print("\nAggregating by HH-year...")
frames = []

for year in YEARS:
    part = PURCHASES / f'panel_year={year}'
    if not part.exists():
        print(f"  {year}: no data")
        continue

    load_cols = ['household_code', 'total_price_paid', 'upc',
                 'product_module', 'product_module_code', 'product_group',
                 'upc_descr']
    p = pd.read_parquet(part, columns=load_cols)
    p = p.dropna(subset=['total_price_paid', 'household_code'])
    p['product_module_code'] = pd.to_numeric(p['product_module_code'], errors='coerce')

    cpi_factor = CPI_BASE / CPI[year]
    p['spend'] = p['total_price_paid'] * cpi_factor

    # Produce flag
    p['is_produce'] = (
        p['product_group'].isin(FRUIT_GROUPS | VEG_GROUPS) |
        p['product_module_code'].isin(FRUIT_MODULES_FRESH | VEG_MODULES_FRESH)
    ).astype(int)

    # Bread flag and whole wheat flag
    p['is_bread'] = p['product_module_code'].isin(BREAD_MODULES).astype(int)
    p['is_whole_bread'] = (
        p['is_bread'].astype(bool) &
        p['upc_descr'].str.contains('WHOLE', case=False, na=False)
    ).astype(int)

    # High-sugar flag from Syndigo
    p['upc_13'] = '0' + p['upc'].astype(str).str.zfill(12)
    p = p.merge(syn.rename(columns={'upc': 'upc_13'}), on='upc_13', how='left')
    p['high_sugar'] = p['high_sugar'].fillna(0).astype(int)

    # Module spending columns
    for mod in top_modules:
        col = f'mod_{mod}'
        p[col] = p['spend'] * (p['product_module_code'] == mod).astype(int)

    mod_cols = [f'mod_{m}' for m in top_modules]

    # Pre-multiply spend by flags for vectorized groupby
    p['spend_produce']     = p['spend'] * p['is_produce']
    p['spend_bread']       = p['spend'] * p['is_bread']
    p['spend_whole_bread'] = p['spend'] * p['is_whole_bread']
    p['spend_high_sugar']  = p['spend'] * p['high_sugar']

    sum_cols = ['spend', 'spend_produce', 'spend_bread', 'spend_whole_bread',
                'spend_high_sugar'] + [f'mod_{m}' for m in top_modules]
    agg = p.groupby('household_code')[sum_cols].sum().reset_index()
    agg = agg.rename(columns={'spend': 'spend_total'})

    agg['panel_year'] = year

    # Shares
    agg['spend_share_produce']    = agg['spend_produce'] / agg['spend_total'].replace(0, np.nan)
    agg['spend_share_whole_bread'] = agg['spend_whole_bread'] / agg['spend_bread'].replace(0, np.nan)
    agg['spend_share_high_sugar'] = agg['spend_high_sugar'] / agg['spend_total'].replace(0, np.nan)

    frames.append(agg)
    print(f"  {year}: {len(agg):,} HHs, avg spend ${agg['spend_total'].mean():.0f}/yr")
    del p, agg
    gc.collect()

# ============================================================
# Save
# ============================================================
combined = pd.concat(frames, ignore_index=True)

# Rename module columns to use readable names where possible
rename_map = {}
for m in top_modules:
    name = mod_names.get(m, str(m))
    safe = name.lower().replace(' ', '_').replace('-', '_').replace('/', '_')[:30]
    rename_map[f'mod_{m}'] = f'spend_{safe}'
combined = combined.rename(columns=rename_map)

print(f"\nCombined: {len(combined):,} HH-year obs")
print(f"Columns: {list(combined.columns)}")

out_pq = OUT_DIR / 'expenditure_hh_year.parquet'
out_dta = OUT_DIR / 'expenditure_hh_year.dta'
combined.to_parquet(out_pq, index=False)
combined.to_stata(str(out_dta), write_index=False)
print(f"Saved: {out_pq}")
print(f"Saved: {out_dta}")
