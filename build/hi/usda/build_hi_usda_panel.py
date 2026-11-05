"""Build a full-panel household-year Health Index from a chosen nutrition source, using the
IDENTICAL formula/standardization as build_hi_panel.py so hi_usda is directly comparable to hi.

Usage:
    python build_hi_usda_panel.py usda       # -> hi_usda_panel.parquet  (hi_usda, ...)
    python build_hi_usda_panel.py syndigo    # -> hi_syn_check.parquet   (validation vs existing hi)

Only difference vs build_hi_panel.py: the nutrient source (syndigo_wide vs usda_wide) and, for USDA,
package grams fall back to Nielsen size1 (USDA has no package-weight field; build_hi_panel already
uses Nielsen size1 as its fallback when Syndigo g_total is missing).
"""
import sys, gc, warnings
import pandas as pd
import numpy as np
from pathlib import Path
warnings.filterwarnings('ignore')
_r = Path(__file__).resolve().parent
while not (_r / 'paths.py').exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from utils.food_measures import package_grams, is_fruit, is_veg, adult_equivalents, in_sample, MIN_PACKAGE_KCAL

SOURCE = (sys.argv[1] if len(sys.argv) > 1 else 'usda').lower()
assert SOURCE in ('usda', 'syndigo')

BASE      = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
PURCHASES = BASE / 'interim' / 'purchases_food'
PANELISTS = BASE / 'interim' / 'panelists' / 'panelists_all_years.parquet'
OUT_DIR   = BASE / 'interim' / 'panel_dataset'
WIDE = {   # one set of label values per UPC, all tiers (build/health/build_upc_dictionary.py, 2026-11-05)
    'syndigo': BASE / 'interim' / 'health' / 'upc_dictionary_syndigo.parquet',
    'usda':    BASE / 'interim' / 'health' / 'upc_dictionary.parquet',
}[SOURCE]
SUFFIX   = 'usda' if SOURCE == 'usda' else 'syn_check'
OUT_PATH = OUT_DIR / f'hi_{SUFFIX}_panel.parquet'

YEARS           = range(2004, 2021)
BREAD_MODULES       = {4000,4001,4002}
NUTCOLS = ['cal_per_100g','sugar_per_100g','satfat_per_100g','sodium_per_100g','chol_per_100g','fiber_per_100g',
           'addsugar_per_100g']  # added sugar: USDA-only; absent from syndigo_wide and skipped there

def log(m): print(m, flush=True)

log(f"=== build_hi_usda_panel  SOURCE={SOURCE}  ->  {OUT_PATH.name} ===")
wide = pd.read_parquet(WIDE)
keep = ['upc_13'] + [c for c in NUTCOLS if c in wide.columns]
wide = wide[keep]
log(f"  nutrient dict: {len(wide):,} UPCs")

hh_year_list = []
for year in YEARS:
    p = PURCHASES / f'panel_year={year}'
    if not p.exists():
        continue
    purch = pd.read_parquet(p)
    purch['panel_year'] = year
    purch = purch[purch['department_descr'] != 'MAGNET DATA']
    purch['upc_13'] = '0' + purch['upc'].astype(str).str.zfill(12)
    purch = purch.merge(wide, on='upc_13', how='left')

    n_total, n_matched = len(purch), purch['cal_per_100g'].notna().sum()
    # Allcott et al. rules (utils/food_measures.py): multipacks, pounds, count-sized items, powdered mixes;
    # rows at or below 1 kcal per package leave the per-calorie measures
    purch['kcal_pkg'] = package_grams(purch) * purch['cal_per_100g'] / 100
    purch['cals_per_row'] = purch['quantity'] * purch['kcal_pkg']
    purch = purch[purch['cals_per_row'].notna() & (purch['kcal_pkg'] > MIN_PACKAGE_KCAL)]

    purch['is_fruit'] = is_fruit(purch['product_module_code'], purch['product_group_code'])
    purch['is_veg'] = is_veg(purch['product_module_code'], purch['product_group_code'])
    _is_bread = purch['product_module_code'].isin(BREAD_MODULES)
    _d = purch['upc_descr']
    _ww = _d.str.contains('WW', na=False) & ~_d.str.startswith('WW', na=False)
    _whole = _d.str.contains(r'W-W|WWH|WHL|GWHY|WH-G|RY-WL|SPT|G-WH|G-WM| WG |WGW|WW100%', na=False) | _ww
    purch['is_bread'] = _is_bread
    purch['is_whole'] = _is_bread & _whole

    is_fv = purch['is_fruit'] | purch['is_veg']
    purch['hi_per_100g'] = np.where(is_fv,
        purch['is_fruit'].astype(float)*100/320 + purch['is_veg'].astype(float)*100/390,
        purch['fiber_per_100g'].fillna(0)/29.5 - purch['sugar_per_100g'].fillna(0)/32.8
        - purch['satfat_per_100g'].fillna(0)/17.2 - purch['sodium_per_100g'].fillna(0)/2.3
        - purch['chol_per_100g'].fillna(0)/0.3)
    purch['hi_per_1000cal']    = purch['hi_per_100g']    / purch['cal_per_100g'] * 1000
    purch['sugar_per_1000cal'] = purch['sugar_per_100g'] / purch['cal_per_100g'] * 1000
    purch['sodium_per_1000cal'] = purch['sodium_per_100g'] / purch['cal_per_100g'] * 1000
    if 'addsugar_per_100g' in purch.columns:
        purch['addsugar_per_1000cal'] = purch['addsugar_per_100g'] / purch['cal_per_100g'] * 1000
    else:
        purch['addsugar_per_1000cal'] = np.nan
    purch['wt_hi']    = purch['hi_per_1000cal']    * purch['cals_per_row']
    purch['wt_sugar'] = purch['sugar_per_1000cal'] * purch['cals_per_row']
    purch['wt_sodium']   = purch['sodium_per_1000cal']   * purch['cals_per_row']
    purch['wt_addsugar'] = purch['addsugar_per_1000cal'] * purch['cals_per_row']
    # calorie share of rows that actually carry an added-sugar value (coverage honesty)
    purch['addsugar_cals'] = purch['cals_per_row'] * purch['addsugar_per_1000cal'].notna().astype(float)
    purch['fruit_cals'] = purch['cals_per_row'] * purch['is_fruit'].astype(float)
    purch['veg_cals']   = purch['cals_per_row'] * purch['is_veg'].astype(float)
    purch['bread_cals'] = purch['cals_per_row'] * purch['is_bread'].astype(float)
    purch['whole_cals'] = purch['cals_per_row'] * purch['is_whole'].astype(float)

    agg = purch.groupby('household_code').agg(
        total_cals=('cals_per_row','sum'), sum_wt_hi=('wt_hi','sum'), sum_wt_sugar=('wt_sugar','sum'),
        sum_wt_sodium=('wt_sodium','sum'), sum_wt_addsugar=('wt_addsugar','sum'),
        addsugar_cals=('addsugar_cals','sum'),
        fruit_cals=('fruit_cals','sum'), veg_cals=('veg_cals','sum'),
        bread_cals=('bread_cals','sum'), whole_cals=('whole_cals','sum')).reset_index()
    agg['panel_year'] = year
    agg['rHI_per_1000cal']   = agg['sum_wt_hi']    / agg['total_cals']
    agg['sugar_per_1000cal'] = agg['sum_wt_sugar'] / agg['total_cals']
    agg['sodium_per_1000cal']   = agg['sum_wt_sodium']   / agg['total_cals']
    agg['addsugar_per_1000cal'] = agg['sum_wt_addsugar'] / agg['total_cals']
    agg['addsugar_cal_share']   = agg['addsugar_cals']   / agg['total_cals']
    agg['produce'] = (agg['fruit_cals'] + agg['veg_cals']) / agg['total_cals']
    agg['whole']   = agg['whole_cals'] / agg['bread_cals'].replace(0, np.nan)
    hh_year_list.append(agg[['household_code','panel_year','total_cals','rHI_per_1000cal',
                             'sugar_per_1000cal','sodium_per_1000cal','addsugar_per_1000cal',
                             'addsugar_cal_share','produce','whole']])
    log(f"  {year}: matched {n_matched/n_total*100:4.0f}%   {len(agg):,} HHs")
    del purch, agg; gc.collect()

hhy = pd.concat(hh_year_list, ignore_index=True)

# panelists: projection_factor for weighting + inner-join support (mirrors build_hi_panel)
pan = pd.read_parquet(PANELISTS, columns=['household_code','panel_year','projection_factor','household_size'])
pan = pan[pan['panel_year'].isin(YEARS)].copy()
pan['projection_factor'] = pan['projection_factor'].astype(float)
hhy = hhy.merge(pan, on=['household_code','panel_year'], how='inner')
# sample rule of Allcott et al.: purchases above 5% of the household's calorie need (was a floor of 50,000 kcal)
_ae = adult_equivalents(YEARS)
hhy = hhy.merge(_ae[['household_code','panel_year','ae_total']], on=['household_code','panel_year'], how='left')
_keep = in_sample(hhy['total_cals'], hhy['ae_total'].fillna(hhy['household_size']))
log(f"  below 5% of calorie need: {(~_keep).sum():,} of {len(hhy):,} HH-years dropped")
hhy = hhy[_keep].drop(columns=['ae_total','household_size'])
log(f"  {len(hhy):,} HH-year obs, {hhy['household_code'].nunique():,} HHs")

# standardize EXACTLY like build_hi_panel: pooled projection-weighted mean/sd
mask = hhy['rHI_per_1000cal'].notna()
w = hhy.loc[mask,'projection_factor'].values; y = hhy.loc[mask,'rHI_per_1000cal'].values
wmean = np.average(y, weights=w); wsd = np.sqrt(np.average((y-wmean)**2, weights=w))
hicol = f'hi_{SUFFIX}' if SOURCE=='usda' else 'hi_syn_check'
hhy[hicol] = (hhy['rHI_per_1000cal'] - wmean) / wsd
log(f"  standardized {hicol}: raw mean {wmean:.4f}, sd {wsd:.4f}")

# hi_allcott variant (year-demeaned sd), same as build_hi_panel
yr_means = hhy[mask].groupby('panel_year').apply(
    lambda g: np.average(g['rHI_per_1000cal'], weights=g['projection_factor']))
resid = hhy['rHI_per_1000cal'] - hhy['panel_year'].map(yr_means)
rv = resid.notna()
wsd_a = np.sqrt(np.average((resid[rv]-np.average(resid[rv],weights=hhy.loc[rv,'projection_factor']))**2,
                           weights=hhy.loc[rv,'projection_factor'].values))
hhy[f'{hicol}_allcott'] = (hhy['rHI_per_1000cal'] - wmean) / wsd_a

COMPONENTS = ['sugar_per_1000cal','sodium_per_1000cal','addsugar_per_1000cal',
              'addsugar_cal_share','produce','whole']
out = hhy.rename(columns={'total_cals':f'total_cals_{SUFFIX}',
                          'rHI_per_1000cal':f'rHI_{SUFFIX}',
                          **{c: f'{c}_{SUFFIX}' for c in COMPONENTS}})[
    ['household_code','panel_year', hicol, f'{hicol}_allcott', f'rHI_{SUFFIX}', f'total_cals_{SUFFIX}']
    + [f'{c}_{SUFFIX}' for c in COMPONENTS]]
OUT_DIR.mkdir(parents=True, exist_ok=True)
out.to_parquet(OUT_PATH, index=False)
log(f"  saved {OUT_PATH}  ({len(out):,} rows, cols={list(out.columns)})")
