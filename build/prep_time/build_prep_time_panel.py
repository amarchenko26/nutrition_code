"""
build_prep_time_panel.py

Aggregates module-level prep time scores to household × year level.

Method: spending-weighted average prep time score across all non-excluded
modules purchased by the household in that year. Modules scored None
(condiments, sauces, toppings) are dropped from both numerator and
denominator so they don't dilute the index.

Requires: prep_time_scores.parquet (run build_prep_time_index.py first)

Output: interim/panel_dataset/panel_prep_time.parquet
  household_code, panel_year, prep_time_raw, prep_time (normalized),
  sh_open, sh_little, sh_cook: % of the household's spending on scored modules that goes to open-and-eat (score 1), little
  preparation (2-3) and cooking (4-9) modules, the denominator of prep_time_raw (added 2026-11-15 for the income IV on time
  intensity, analyze/what_moves/build_iv_time.do)
  sh_rte: % of that spending on ready-to-eat meals (entrees, dinners, pizza, sandwiches, frozen breakfasts, soup, chili, stews,
  canned pasta meals, prepared meals and salads; RTE_RULE below); sh_open_rte: open-and-eat or ready-to-eat; sh_scratch: % on
  scratch ingredients (raw unbreaded meat, poultry and seafood, eggs, fresh vegetables that need cooking, flour and meal, dry beans
  and grains, regular rice, dry pasta, baking supplies; SCRATCH_RULE below) (all three added 2026-11-15)
  ing_per_trip: distinct ingredient barcodes per food shopping trip, the household's trip mean over the year; an ingredient is a
  barcode in NOVA group 1 (unprocessed or minimally processed: fresh produce, milk, eggs, plain frozen vegetables, dry pasta, rice,
  flour) or 2 (culinary ingredients: sugar, oils, butter, salt) in interim/health/upc_dictionary.parquet (`nova`, own ingredient
  list or the nearest labelled relative); trips = distinct trip_code_uc with a food purchase (added 2026-11-15)
  prep_min_1000kcal: minutes of cooking per 1,000 kcal bought (2026-11-18; replaces prep_min_ae_day, minutes per adult equivalent
  per day from the module score and module spending, which is no longer built): units bought x
  minutes per package of the barcode (interim/prep_time/upc_prep_package.parquet: build_prep_minutes_upc.py applies the rules of
  prep_minutes_rules.py to each barcode's Nielsen attributes, build_prep_minutes_package.py turns minutes per cooking session
  into minutes per package: four label servings per session, one session for packages cooked whole), summed over the year's
  purchases with a package size and a calorie density, over their calories; condiments, sauces and other zero-minute foods add
  calories and no minutes. A ratio, so it is defined for every household; missing where the year's calories are at or below 5%
  of the household's calorie need (utils.food_measures.in_sample). prep_min_1000kcal_serv4 and prep_min_1000kcal_once: the same
  with four label servings per session for every barcode, and with one session per package for every barcode (the two checks).
"""

import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import pandas as pd
import numpy as np
import gc
from utils.food_measures import in_sample
import warnings
warnings.filterwarnings('ignore')

def log(msg):
    print(msg, flush=True)

PURCHASES = idir / 'purchases_food'
PANELISTS = idir / 'panelists' / 'panelists_all_years.parquet'
SCORES    = idir / 'rms_variety' / 'prep_time_scores.parquet'
OUT_DIR   = idir / 'panel_dataset'
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH  = OUT_DIR / 'panel_prep_time.parquet'

YEARS           = range(2004, 2021)
CAL_PANEL       = idir / 'health' / 'nova_calorie_panel.parquet'
DICT            = idir / 'health' / 'upc_dictionary.parquet'
PKG             = idir / 'prep_time' / 'upc_prep_package.parquet'
RTE_GROUPS = {'PREPARED FOODS-FROZEN', 'PIZZA/SNACKS/HORS DOEURVES-FRZN', 'PREPARED FOOD-READY-TO-SERVE', 'SOUP',
              'DRESSINGS/SALADS/PREP FOODS-DELI', 'BREAKFAST FOODS-FROZEN'}
RTE_RULE = (r'ENTREE|DINNER|PIZZA|SANDWICH|COMBINATION LUNCH|BREAKFAST|WAFFLE|SOUP|CHOWDER|CHILI|STEW|RAVIOLI|SPAGHETTI - CANNED|'
            r'LASAGNA|MACARONI PRODUCTS|INSTANT MEALS|PREPARED MEAL|PREPARED APPETIZER|HORS D|CORN DOGS|POT PIES|HASH|CHOW MEIN|'
            r'MEXICAN DINNERS|BEANS WITH MEAT|READY MADE SALADS|PREPARED SALAD|POTATO SALAD')
RTE_EXCLUDE = r'MIX|CRUST|SPREAD|STARTER|BOUILLON|BROTH'
SCRATCH_GROUPS = {'FRESH MEAT', 'UNPREP MEAT/POULTRY/SEAFOOD-FRZN', 'EGGS', 'FLOUR', 'BAKING SUPPLIES', 'VEGETABLES AND GRAINS - DRIED',
                  'PASTA', 'FRESH PRODUCE'}
SCRATCH_EXCLUDE = (r'(?<!UN)BREADED|INSTANT|STUFFING|CROUTON|FROSTING|DECORATION|ICING|SHELL|GRAHAM|GLAZED|COCONUT|TAPIOCA|CAPERS|'
                   r'PROTECTOR|MATZO|ORIENTAL NOODLES|SPECIALTY DRIED BREAD|BREADING')
MIN_ANNUAL_SPEND = 50.0   # drop HH-years below this real spend threshold (2013$)

# ============================================================
# LOAD MODULE SCORES
# ============================================================
log("Loading prep time scores...")
scores_df = pd.read_parquet(SCORES)
# Keep only explicitly scored (non-excluded) modules
scores_df = scores_df[scores_df['prep_time'].notna()].copy()
score_map = dict(zip(scores_df['product_module_code'].astype(int),
                     scores_df['prep_time'].astype(float)))
log(f"  {len(score_map):,} modules with prep time scores")
# ready-to-eat meals and scratch ingredients, among scored modules
d = scores_df['product_module_descr'].fillna('')
rte = scores_df['product_group_descr'].isin(RTE_GROUPS) & d.str.contains(RTE_RULE) & ~d.str.contains(RTE_EXCLUDE)
scr = (scores_df['product_group_descr'].isin(SCRATCH_GROUPS) & ~d.str.contains(SCRATCH_EXCLUDE)
       & ((scores_df['product_group_descr'] != 'FRESH PRODUCE') | (scores_df['prep_time'] >= 4)))
RTE_MODS = set(scores_df.loc[rte, 'product_module_code'].astype(int))
SCR_MODS = set(scores_df.loc[scr, 'product_module_code'].astype(int))
log(f"  ready-to-eat modules {len(RTE_MODS)}: " + '; '.join(sorted(scores_df.loc[rte, 'product_module_descr'])))
log(f"  scratch-ingredient modules {len(SCR_MODS)}: " + '; '.join(sorted(scores_df.loc[scr, 'product_module_descr'])))
# ingredient barcodes: NOVA group 1 or 2
dic = pd.read_parquet(DICT, columns=['upc_13', 'nova'])
ING = set(dic.loc[dic['nova'].isin([1, 2]), 'upc_13'])
log(f"  {len(ING):,} ingredient barcodes (NOVA 1-2) of {len(dic):,} in the dictionary")
del dic

# minutes of cooking and calories per package, by barcode (barcodes with a package size and a calorie density)
pk = pd.read_parquet(PKG, columns=['upc', 'min_pkg', 'min_pkg_serv4', 'min_pkg_once', 'kcal_pkg'])
pk = pk[pk['kcal_pkg'] > 0].set_index('upc')
log(f"  {len(pk):,} barcodes with minutes and calories per package")

# ============================================================
# LOAD PANELISTS (projection factors for weighted normalization)
# ============================================================
log("Loading panelists...")
pan = pd.read_parquet(PANELISTS, columns=['household_code', 'panel_year', 'projection_factor'])
pan = pan[pan['panel_year'].isin(YEARS)]
log(f"  {pan['household_code'].nunique():,} HHs")

# ============================================================
# PROCESS PURCHASES YEAR BY YEAR
# ============================================================
log("Processing purchases...")
hh_year_list = []

for year in YEARS:
    purch_path = PURCHASES / f'panel_year={year}'
    if not purch_path.exists():
        log(f"  {year}: no data, skipping")
        continue
    log(f"  {year}...")

    purch = pd.read_parquet(purch_path, columns=[
        'household_code', 'product_module_code', 'total_price_paid_real_2013',
        'department_descr', 'trip_code_uc', 'upc', 'quantity',
    ])
    purch = purch[purch['department_descr'] != 'MAGNET DATA']
    purch['product_module_code'] = purch['product_module_code'].astype(int)

    # Merge prep time score; rows without a score (excluded or unscored) become NaN
    purch['prep_time'] = purch['product_module_code'].map(score_map)
    purch['spend']     = purch['total_price_paid_real_2013'].clip(lower=0)

    # barcode minutes and calories: units bought x per-package values (rows without a size or a density add to neither)
    j = pk.reindex(purch['upc'].astype(str).str.zfill(12).values)
    q = purch['quantity'].clip(lower=0).values
    purch['bc_kcal'] = q * j['kcal_pkg'].values
    purch['bc_min'] = q * j['min_pkg'].values
    purch['bc_min_serv4'] = q * j['min_pkg_serv4'].values
    purch['bc_min_once'] = q * j['min_pkg_once'].values
    del j, q

    # Keep only rows with a valid score and positive spend
    purch_scored = purch[purch['prep_time'].notna() & (purch['spend'] > 0)].copy()

    n_total  = len(purch)
    n_scored = len(purch_scored)

    # Spending-weighted average prep time per HH
    purch_scored['wt_prep'] = purch_scored['prep_time'] * purch_scored['spend']
    agg = purch_scored.groupby('household_code').agg(
        sum_wt_prep  = ('wt_prep', 'sum'),
        sum_spend    = ('spend',   'sum'),
    ).reset_index()
    agg['panel_year']    = year
    agg['prep_time_raw'] = agg['sum_wt_prep'] / agg['sum_spend']
    # spending shares by preparation group, same denominator
    purch_scored['grp'] = np.where(purch_scored['prep_time'] <= 1, 'sh_open', np.where(purch_scored['prep_time'] <= 3, 'sh_little', 'sh_cook'))
    gs = purch_scored.pivot_table(index='household_code', columns='grp', values='spend', aggfunc='sum', fill_value=0)
    gs = gs.reindex(columns=['sh_open', 'sh_little', 'sh_cook'], fill_value=0).reset_index()
    agg = agg.merge(gs, on='household_code', how='left')
    for g in ['sh_open', 'sh_little', 'sh_cook']:
        agg[g] = 100 * agg[g] / agg['sum_spend']
    purch_scored['rte'] = purch_scored['product_module_code'].isin(RTE_MODS)
    purch_scored['scr'] = purch_scored['product_module_code'].isin(SCR_MODS)
    purch_scored['s_rte'] = purch_scored['spend'] * purch_scored['rte']
    purch_scored['s_scr'] = purch_scored['spend'] * purch_scored['scr']
    purch_scored['s_ore'] = purch_scored['spend'] * (purch_scored['rte'] | (purch_scored['prep_time'] <= 1))
    ex = purch_scored.groupby('household_code')[['s_rte', 's_scr', 's_ore']].sum().reset_index()
    agg = agg.merge(ex, on='household_code', how='left')
    agg['sh_rte'] = 100 * agg['s_rte'] / agg['sum_spend']
    agg['sh_scratch'] = 100 * agg['s_scr'] / agg['sum_spend']
    agg['sh_open_rte'] = 100 * agg['s_ore'] / agg['sum_spend']
    agg = agg.merge(purch.groupby('household_code')[['bc_kcal', 'bc_min', 'bc_min_serv4', 'bc_min_once']].sum().reset_index(), on='household_code', how='left')
    # distinct ingredient barcodes per food trip
    ing = purch.loc[('0' + purch['upc'].astype(str).str.zfill(12)).isin(ING), ['household_code', 'trip_code_uc', 'upc']].drop_duplicates()
    agg = agg.merge(ing.groupby('household_code').size().rename('ing_pairs'), on='household_code', how='left')
    agg = agg.merge(purch.groupby('household_code')['trip_code_uc'].nunique().rename('food_trips'), on='household_code', how='left')
    agg['ing_per_trip'] = agg['ing_pairs'].fillna(0) / agg['food_trips']
    del ing

    # Also track total real spend (for the minimum spend filter)
    total_spend = purch.groupby('household_code')['spend'].sum().rename('total_spend')
    agg = agg.merge(total_spend, on='household_code', how='left')

    hh_year_list.append(agg[['household_code', 'panel_year', 'prep_time_raw', 'sh_open', 'sh_little', 'sh_cook', 'sh_rte', 'sh_open_rte',
                             'sh_scratch', 'ing_per_trip', 'total_spend', 'bc_kcal', 'bc_min', 'bc_min_serv4', 'bc_min_once']])
    log(f"    scored rows: {n_scored/n_total*100:.0f}%, {len(agg):,} HHs, {agg['ing_per_trip'].mean():.2f} ingredient barcodes per trip")
    del purch, purch_scored, agg
    gc.collect()

# ============================================================
# COMBINE, FILTER, MERGE PROJECTION FACTORS
# ============================================================
log("Combining years...")
panel = pd.concat(hh_year_list, ignore_index=True)
panel = panel[panel['total_spend'] >= MIN_ANNUAL_SPEND]
panel = panel.merge(pan, on=['household_code', 'panel_year'], how='left')
log(f"  {len(panel):,} HH-year obs, {panel['household_code'].nunique():,} HHs")

# ============================================================
# NORMALIZE (projection-factor weighted, pooled across years)
# ============================================================
log("Normalizing...")
mask = panel['prep_time_raw'].notna() & panel['projection_factor'].notna()
w = panel.loc[mask, 'projection_factor'].values
y = panel.loc[mask, 'prep_time_raw'].values
wmean = np.average(y, weights=w)
wsd   = np.sqrt(np.average((y - wmean) ** 2, weights=w))
panel['prep_time'] = (panel['prep_time_raw'] - wmean) / wsd
log(f"  raw mean={wmean:.3f}, sd={wsd:.3f}")
log(f"  normalized: mean≈0, sd≈1 (projection-factor weighted)")

# ============================================================
# SAVE
# ============================================================
ae = pd.read_parquet(CAL_PANEL, columns=['household_code', 'panel_year', 'ae_total'])
panel = panel.merge(ae, on=['household_code', 'panel_year'], how='left')
ok = in_sample(panel['bc_kcal'], panel['ae_total']) & (panel['bc_kcal'] > 0)
for c in ['', '_serv4', '_once']:
    panel[f'prep_min_1000kcal{c}'] = (1000 * panel[f'bc_min{c}'] / panel['bc_kcal']).where(ok)
log(f"  minutes of cooking per 1,000 kcal: mean {panel['prep_min_1000kcal'].mean():.2f}, median {panel['prep_min_1000kcal'].median():.2f}; "
    f"four servings per session for every barcode {panel['prep_min_1000kcal_serv4'].mean():.2f}; one session per package {panel['prep_min_1000kcal_once'].mean():.2f}; "
    f"household-years with a value {panel['prep_min_1000kcal'].notna().sum():,} of {len(panel):,}")
out = panel[['household_code', 'panel_year', 'prep_time_raw', 'prep_time', 'sh_open', 'sh_little', 'sh_cook', 'sh_rte', 'sh_open_rte',
             'sh_scratch', 'ing_per_trip', 'prep_min_1000kcal', 'prep_min_1000kcal_serv4', 'prep_min_1000kcal_once']].copy()
out.to_parquet(OUT_PATH, index=False)
stata_path = OUT_PATH.with_suffix('.dta')
out.to_stata(stata_path, write_index=False)
log(f"\nSaved: {OUT_PATH}")
log(f"Saved: {stata_path}")
log(f"Columns: {list(out.columns)}")
log(f"Years:   {sorted(out['panel_year'].unique())}")
log(f"Shape:   {out.shape}")
log("Done!")
