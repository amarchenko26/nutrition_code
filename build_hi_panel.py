"""Build shared HH-year panel dataset for nutrition analysis.

Run this once (or when data changes). Outputs:
  BASE/interim/panel_dataset/panel_hh_year.parquet

Columns in output:
  household_code, panel_year
  total_cals
  rHI_per_1000cal      -- raw (un-normalized) Health Index per 1000 cal
  hi                   -- normalized HI (pooled mean/sd, no year-residualization)
  hi_allcott           -- normalized HI (Allcott-style: pooled mean, year-demeaned sd)
  sugar_per_1000cal, produce, whole   -- for Figure 1 panels
  projection_factor, real_income, hh_real_income_avg, avg_age_hh_head, household_size
  zip_code             -- if present in panelists file
  [EXTRA_PANELIST_VARS]

Then figure_hi_over_time.py and replicate_figure1.py just load this file.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import gc
import warnings
warnings.filterwarnings('ignore')

def log(msg):
    print(msg, flush=True)

# ============================================================
# SETTINGS
# ============================================================
BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
PURCHASES = BASE / 'interim' / 'purchases_food'
SYNDIGO   = BASE / 'interim' / 'syndigo_nielsen_merged' / 'syndigo_wide.parquet'
PANELISTS = BASE / 'interim' / 'panelists' / 'panelists_all_years.parquet'
OUT_DIR   = BASE / 'interim' / 'panel_dataset'
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH  = OUT_DIR / 'panel_hh_year.parquet'

YEARS           = range(2004, 2021)  # years to process (skips missing years automatically)
SAMPLE_FRAC     = 0.3                # fraction of households to sample (1.0 = all)
MIN_ANNUAL_CALS = 50_000             # drop HH-years below this calorie threshold
RANDOM_SEED     = 42
FORCE_REBUILD   = False              # set True to ignore existing output and rebuild

# Add extra panelist columns to include in the output.
# Check available columns by loading a sample: pd.read_parquet(PANELISTS).columns
# Common options: 'fips_state_code', 'fips_county_code', 'panelist_zip_code'
EXTRA_PANELIST_VARS = ['cholesterol', 'prediabetes', 'diabetes_type1', 'diabetes_type2', 'heart_disease', 'hypertension', 'obesity', 'any_diabetes', 'any_metabolic_disease', 'n_dietary_conditions']

# ============================================================
# CONSTANTS
# ============================================================
CPI = {
    2004: 188.9, 2005: 195.3, 2006: 201.6, 2007: 207.3, 2008: 215.3,
    2009: 214.5, 2010: 218.1, 2011: 224.9, 2012: 229.6, 2013: 233.0,
    2014: 236.7, 2015: 237.0, 2016: 240.0, 2017: 245.1, 2018: 251.1,
    2019: 255.7, 2020: 258.8, 2021: 271.0, 2022: 292.7, 2023: 304.7,
    2024: 314.0,
}

UNIT_TO_GRAMS = {
    'OZ': 28.3495, 'LB': 453.592, 'FL OZ': 29.5735, 'QT': 946.353,
    'GAL': 3785.41, 'ML': 1.0, 'LT': 1000.0, 'KG': 1000.0, 'GM': 1.0, 'GR': 1.0,
}

FRUIT_GROUPS       = {'FRUIT - CANNED', 'FRUIT - DRIED', 'FRUIT'}
FRUIT_MODULES_FRESH = {4010, 4085, 4180, 4225, 4355, 4470}
VEG_GROUPS         = {'VEGETABLES - CANNED', 'VEGETABLES-FROZEN', 'VEGETABLES AND GRAINS - DRIED'}
VEG_MODULES_FRESH  = {4015, 4020, 4023, 4050, 4055, 4060, 4140, 4230, 4275, 4280, 4350, 4400, 4415, 4460, 4475}
BREAD_MODULES      = {4000, 4001, 4002}  # bread, rolls/buns, english muffins/bagels

if OUT_PATH.exists() and not FORCE_REBUILD:
    log(f"Output already exists: {OUT_PATH}")
    log("Set FORCE_REBUILD = True to rebuild.")
    exit()

# ============================================================
# SYNDIGO
# ============================================================
log("Loading Syndigo...")
syn_wide = pd.read_parquet(SYNDIGO)
syn_wide.rename(columns={'upc': 'upc_13'}, inplace=True)
log(f"  {len(syn_wide):,} UPCs")

# ============================================================
# PANELISTS
# ============================================================
log("Loading panelists...")
pan = pd.read_parquet(PANELISTS)
pan = pan[pan['panel_year'].isin(YEARS)].copy()

cpi_base = CPI[2010]
pan['real_income'] = pan['household_income_midpoint'] * (cpi_base / pan['panel_year'].map(CPI)) / 1000
hh_av = pan.groupby('household_code')['real_income'].mean().rename('hh_real_income_avg')
pan = pan.merge(hh_av, on='household_code', how='left')

age_map = {1: 27, 2: 32, 3: 37, 4: 42, 5: 47, 6: 52, 7: 57, 8: 62, 9: 67, 0: np.nan}
pan['male_age']  = pan['male_head_age'].map(age_map)
pan['female_age'] = pan['female_head_age'].map(age_map)
pan['avg_age_hh_head'] = pan[['male_age', 'female_age']].mean(axis=1).clip(23, 90).round().fillna(45).astype(int)
pan['projection_factor'] = pan['projection_factor'].astype(float)

# Zip code: try common Nielsen column names
zip_col = None
for candidate in ['panelist_zip_code', 'zip_code', 'panel_zip_code']:
    if candidate in pan.columns:
        zip_col = candidate
        pan = pan.rename(columns={candidate: 'zip_code'})
        break
if zip_col is None:
    log("  Note: no zip code column found. Add it to EXTRA_PANELIST_VARS if you know the column name.")

log(f"  {pan['household_code'].nunique():,} HHs across {pan['panel_year'].nunique()} years")

# Sample households
np.random.seed(RANDOM_SEED)
all_hh = pan['household_code'].unique()
sample_hh = set(np.random.choice(all_hh, size=int(len(all_hh) * SAMPLE_FRAC), replace=False))
pan = pan[pan['household_code'].isin(sample_hh)]
log(f"  Sampled {len(sample_hh):,} households ({SAMPLE_FRAC:.0%})")

pan_cols = ['household_code', 'panel_year', 'projection_factor', 'real_income',
            'hh_real_income_avg', 'avg_age_hh_head', 'household_size']
if zip_col is not None:
    pan_cols.append('zip_code')
for v in EXTRA_PANELIST_VARS:
    if v in pan.columns:
        pan_cols.append(v)
    else:
        log(f"  Warning: EXTRA_PANELIST_VARS column '{v}' not found in panelists")
pan_merge = pan[pan_cols].copy()

# ============================================================
# PURCHASES
# ============================================================
log("Processing purchases...")
hh_year_list = []

for year in YEARS:
    purch_path = PURCHASES / f'panel_year={year}'
    if not purch_path.exists():
        log(f"  {year}: no data, skipping")
        continue
    log(f"  {year}...")
    purch = pd.read_parquet(purch_path)
    purch['panel_year'] = year
    purch = purch[purch['household_code'].isin(sample_hh)]
    purch = purch[purch['department_descr'] != 'MAGNET DATA']

    purch['upc_13'] = '0' + purch['upc'].astype(str).str.zfill(12)
    purch = purch.merge(syn_wide, on='upc_13', how='left')

    n_total   = len(purch)
    n_matched = purch['cal_per_100g'].notna().sum()

    purch['g_conv']      = purch['size1_units'].map(UNIT_TO_GRAMS) * purch['size1_amount']
    purch['g_pkg']       = purch['g_total'].where(purch['g_total'] > 0, purch['g_conv'])
    purch['cals_per_row'] = purch['quantity'] * purch['g_pkg'] * purch['cal_per_100g'] / 100
    purch = purch[purch['cals_per_row'].notna() & (purch['cals_per_row'] > 0)]

    purch['is_fruit'] = (
        purch['product_group'].isin(FRUIT_GROUPS) |
        purch['product_module_code'].isin(FRUIT_MODULES_FRESH) |
        purch['product_module'].str.contains('FROZEN FRUITS|FRUIT JUICE|FRUIT DRINK', case=False, na=False)
    )
    purch['is_veg'] = (
        purch['product_group'].isin(VEG_GROUPS) |
        purch['product_module_code'].isin(VEG_MODULES_FRESH) |
        purch['product_module'].str.contains(
            'VEGETABLE.*FROZEN|TOMATO PASTE|TOMATO SAUCE|TOMATO PUREE|TOMATOES.*CANNED|TOMATOES.*STEWED|MUSHROOM',
            case=False, na=False)
    )
    # Bread and whole grain: both restricted to modules 4000-4002 (bread, rolls, english muffins)
    _is_bread_module = purch['product_module_code'].isin(BREAD_MODULES)
    _d = purch['upc_descr']
    _ww = _d.str.contains('WW', na=False) & ~_d.str.startswith('WW', na=False)
    _whole_desc = _d.str.contains(
        r'W-W|WWH|WHL|GWHY|WH-G|RY-WL|SPT|G-WH|G-WM| WG |WGW|WW100%', na=False) | _ww
    purch['is_bread'] = _is_bread_module
    purch['is_whole'] = _is_bread_module & _whole_desc

    is_fv = purch['is_fruit'] | purch['is_veg']
    purch['hi_per_100g'] = np.where(
        is_fv,
        purch['is_fruit'].astype(float) * 100/320 + purch['is_veg'].astype(float) * 100/390,
        purch['fiber_per_100g'].fillna(0) / 29.5
        - purch['sugar_per_100g'].fillna(0) / 32.8
        - purch['satfat_per_100g'].fillna(0) / 17.2
        - purch['sodium_per_100g'].fillna(0) / 2.3
        - purch['chol_per_100g'].fillna(0) / 0.3
    )
    purch['hi_per_1000cal']   = purch['hi_per_100g'] / purch['cal_per_100g'] * 1000
    purch['sugar_per_1000cal'] = purch['sugar_per_100g'] / purch['cal_per_100g'] * 1000

    purch['wt_hi']    = purch['hi_per_1000cal']    * purch['cals_per_row']
    purch['wt_sugar'] = purch['sugar_per_1000cal'] * purch['cals_per_row']
    purch['fruit_cals'] = purch['cals_per_row'] * purch['is_fruit'].astype(float)
    purch['veg_cals']   = purch['cals_per_row'] * purch['is_veg'].astype(float)
    purch['bread_cals'] = purch['cals_per_row'] * purch['is_bread'].astype(float)
    purch['whole_cals'] = purch['cals_per_row'] * purch['is_whole'].astype(float)

    agg = purch.groupby('household_code').agg(
        total_cals  = ('cals_per_row', 'sum'),
        sum_wt_hi   = ('wt_hi',    'sum'),
        sum_wt_sugar= ('wt_sugar', 'sum'),
        fruit_cals  = ('fruit_cals',  'sum'),
        veg_cals    = ('veg_cals',    'sum'),
        bread_cals  = ('bread_cals',  'sum'),
        whole_cals  = ('whole_cals',  'sum'),
    ).reset_index()
    agg['panel_year']        = year
    agg['rHI_per_1000cal']   = agg['sum_wt_hi']    / agg['total_cals']
    agg['sugar_per_1000cal'] = agg['sum_wt_sugar']  / agg['total_cals']
    agg['produce'] = (agg['fruit_cals'] + agg['veg_cals']) / agg['total_cals']
    agg['whole']   = agg['whole_cals'] / agg['bread_cals'].replace(0, np.nan)

    hh_year_list.append(agg[['household_code', 'panel_year', 'total_cals',
                               'rHI_per_1000cal', 'sugar_per_1000cal', 'produce', 'whole']])
    log(f"    matched {n_matched/n_total*100:.0f}%, {len(agg):,} HHs")
    del purch, agg
    gc.collect()

# ============================================================
# COMBINE & MERGE PANELISTS
# ============================================================
log("Combining years...")
hhy = pd.concat(hh_year_list, ignore_index=True)
hhy = hhy[hhy['total_cals'] >= MIN_ANNUAL_CALS]
hhy = hhy.merge(pan_merge, on=['household_code', 'panel_year'], how='inner')
log(f"  {len(hhy):,} HH-year obs, {hhy['household_code'].nunique():,} HHs")

# ============================================================
# NORMALIZE HEALTH INDEX (two versions)
# ============================================================
log("Normalizing Health Index...")
mask = hhy['rHI_per_1000cal'].notna()
w = hhy.loc[mask, 'projection_factor'].values
y = hhy.loc[mask, 'rHI_per_1000cal'].values
wmean = np.average(y, weights=w)

# hi: pooled mean and pooled SD (no year-residualization)
wsd = np.sqrt(np.average((y - wmean) ** 2, weights=w))
hhy['hi'] = (hhy['rHI_per_1000cal'] - wmean) / wsd
log(f"  hi (pooled):      mean(raw)={wmean:.4f}, sd={wsd:.4f}")

# hi_allcott: pooled mean, but SD from year-demeaned residuals (Allcott-style)
yr_means = hhy[mask].groupby('panel_year').apply(
    lambda g: np.average(g['rHI_per_1000cal'], weights=g['projection_factor']))
resid = hhy['rHI_per_1000cal'] - hhy['panel_year'].map(yr_means)
r_valid = resid.notna()
wsd_a = np.sqrt(np.average(
    (resid[r_valid] - np.average(resid[r_valid], weights=hhy.loc[r_valid, 'projection_factor']))**2,
    weights=hhy.loc[r_valid, 'projection_factor'].values))
hhy['hi_allcott'] = (hhy['rHI_per_1000cal'] - wmean) / wsd_a
log(f"  hi_allcott (year-demeaned sd): sd(resid)={wsd_a:.4f}")

# ============================================================
# SAVE
# ============================================================
hhy.to_parquet(OUT_PATH, index=False)
log(f"\nSaved: {OUT_PATH}")
log(f"Columns: {list(hhy.columns)}")
log(f"Years:   {sorted(hhy['panel_year'].unique())}")
log("Done!")
