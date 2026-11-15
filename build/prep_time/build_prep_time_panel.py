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
        'department_descr',
    ])
    purch = purch[purch['department_descr'] != 'MAGNET DATA']
    purch['product_module_code'] = purch['product_module_code'].astype(int)

    # Merge prep time score; rows without a score (excluded or unscored) become NaN
    purch['prep_time'] = purch['product_module_code'].map(score_map)
    purch['spend']     = purch['total_price_paid_real_2013'].clip(lower=0)

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

    # Also track total real spend (for the minimum spend filter)
    total_spend = purch.groupby('household_code')['spend'].sum().rename('total_spend')
    agg = agg.merge(total_spend, on='household_code', how='left')

    hh_year_list.append(agg[['household_code', 'panel_year', 'prep_time_raw', 'sh_open', 'sh_little', 'sh_cook', 'total_spend']])
    log(f"    scored rows: {n_scored/n_total*100:.0f}%, {len(agg):,} HHs")
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
out = panel[['household_code', 'panel_year', 'prep_time_raw', 'prep_time', 'sh_open', 'sh_little', 'sh_cook']].copy()
out.to_parquet(OUT_PATH, index=False)
stata_path = OUT_PATH.with_suffix('.dta')
out.to_stata(stata_path, write_index=False)
log(f"\nSaved: {OUT_PATH}")
log(f"Saved: {stata_path}")
log(f"Columns: {list(out.columns)}")
log(f"Years:   {sorted(out['panel_year'].unique())}")
log(f"Shape:   {out.shape}")
log("Done!")
