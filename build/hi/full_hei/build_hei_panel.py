"""Build the household-year USDA HEI-2020 (full Healthy Eating Index) panel.

Components & standards (epi.grants.cancer.gov/hei, HEI-2020 = HEI-2015):
  Adequacy (density per 1,000 kcal unless noted; linear between min/max):
    Total Fruits >=0.8 cup-eq (5) | Whole Fruits >=0.4 (5) | Total Veg >=1.1 (5)
    Greens&Beans >=0.2 (5) | Whole Grains >=1.5 oz-eq (10) | Dairy >=1.3 (10)
    Total Protein >=2.5 oz-eq (5) | Seafood&Plant >=0.8 (5)
    Fatty Acids (PUFA+MUFA)/SFA >=2.5 (10), 0 at <=1.2
  Moderation (reverse-scored):
    Refined Grains <=1.8 oz-eq (10), 0 at >=4.3 | Sodium <=1.1 g (10), 0 at >=2.0
    Added Sugars <=6.5 %energy (10), 0 at >=26 | Sat Fat <=8 %energy (10), 0 at >=16

CHANGED 2026-11-05: sizes follow utils/food_measures.py (the rules of the Allcott et al. replication: multipacks,
pounds, count-sized items, powdered mixes), and nutrition facts come from interim/health/upc_dictionary.parquet (one
value per UPC and nutrient, own label else the nearest labelled relatives). So every component now uses the full
basket and one energy denominator, and the sample rule is purchases above 5% of the household's calorie need. Until
then multipacks counted as one unit, pound- and count-sized rows were dropped, label components used the matched
basket and added sugars only the rows with that field, whose number rose after 2016.

Conventions before 2026-11-05 (superseded where they conflict with the paragraph above):
  * Food-pattern equivalents come from the module crosswalk (build_hei_crosswalk.py)
    x purchase grams -- computed on the FULL basket; the energy denominator for
    these components imputes calories for USDA-unmatched rows with the module's
    spend-weighted median cal_per_100g (produce matches poorly, but its
    equivalents must count).
  * Label components (sodium, added sugars, sat fat, fatty acids) use the
    USDA-MATCHED basket only (own energy denominator); added sugars additionally
    restrict the denominator to rows carrying the added-sugar field.
  * Fatty acids: MUFA/PUFA labels cover ~12% of UPCs -> module-median per-100g
    values imputed within module; hei_usda_90 (12 components, rescaled x100/90)
    is exported for robustness without the FA component.
    CHANGED 2026-11-04: the module median is now POOLED over the whole UPC
    universe (products.tsv x usda_wide, one observation per labelled UPC) and
    labels whose fats are implausible are dropped first (9 x (MUFA+PUFA+SFA)
    > 1.1 x kcal, or MUFA+PUFA+SFA > total fat + 1 g; these are USDA records
    matched to a reused store-brand UPC). Until then the median was taken over
    the YEAR's purchase rows, so one wrongly matched fruit-drink label (867 kcal,
    80 g MUFA+PUFA per 100 g) set the median of FRUIT DRINKS-OTHER CONTAINER to
    80 g/100 g in 2008 and 0 in 2009: hei_c_fattyacid averaged 6.2 in 2004-2008
    and 2.9 from 2009 (a 3.4-point step in hei_usda in every income quartile).
    With the pooled rule the component is about 3.1-3.4 in every year.
    Rows whose own label is implausible are imputed like unlabelled rows.
  * Sample rule matches build_hi_usda_panel.py: MIN_ANNUAL_CALS on matched cals.

Outputs: interim/panel_dataset/hei_usda_panel.parquet + .dta
         (hei_usda 0-100, hei_usda_90, 13 component scores hei_c_*, lhei_usda = the uncapped linear HEI of Allcott et al.)
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import gc
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from utils.food_measures import package_grams, adult_equivalents, in_sample, MIN_PACKAGE_KCAL

PURCHASES = idir / 'purchases_food'
USDA      = idir / 'health' / 'upc_dictionary.parquet'
CROSSWALK = idir / 'hei' / 'module_hei_crosswalk.parquet'
PANELISTS = idir / 'panelists' / 'panelists_all_years.parquet'
PRODUCTS  = rdir / 'products.tsv'
OUT_DIR   = idir / 'panel_dataset'

YEARS           = range(2004, 2021)
PURCH_COLS = ['upc','household_code','quantity','multi','size1_amount','size1_units',
              'department_descr','product_module_code','product_group_code']

def log(m): print(m, flush=True)

log("Loading USDA dictionary + crosswalk...")
wide = pd.read_parquet(USDA, columns=['upc_13','cal_per_100g','satfat_per_100g','sodium_per_100g','addsugar_per_100g',
                                      'mufat_per_100g','pufat_per_100g'])
cw = pd.read_parquet(CROSSWALK, columns=['product_module_code','hei_group','grams_per_equiv','whole_share'])
log(f"  dict {len(wide):,} UPCs; crosswalk {len(cw):,} modules")

hh_parts = []
for year in YEARS:
    p = PURCHASES / f'panel_year={year}'
    if not p.exists():
        continue
    purch = pd.read_parquet(p, columns=PURCH_COLS)
    purch = purch[purch['department_descr'] != 'MAGNET DATA']
    purch['upc_13'] = '0' + purch['upc'].astype(str).str.zfill(12)
    purch = purch.merge(wide, on='upc_13', how='left').merge(cw, on='product_module_code', how='left')

    purch['pkg_g'] = package_grams(purch)
    purch['grams'] = purch['quantity'] * purch['pkg_g']
    purch = purch[purch['grams'].notna() & (purch['grams'] > 0)]
    # rows at or below 1 kcal per package leave the per-calorie measures (UPCDataPrep.do)
    purch = purch[(purch['pkg_g'] * purch['cal_per_100g'] / 100) > MIN_PACKAGE_KCAL]
    purch['cals_full'] = purch['grams'] * purch['cal_per_100g'] / 100

    # ---- food-pattern equivalents (full basket) ----
    eq = purch['grams'] / purch['grams_per_equiv']
    g = purch['hei_group']
    ws = purch['whole_share'].fillna(0)
    purch['eq_fw'] = np.where(g == 'fruit_whole', eq, 0.0)
    purch['eq_fj'] = np.where(g == 'fruit_juice', eq, 0.0)
    purch['eq_v']  = np.where(g == 'veg', eq, 0.0)
    purch['eq_gb'] = np.where(g == 'greens_beans', eq, 0.0)
    purch['eq_lg'] = np.where(g == 'legume', eq, 0.0)
    purch['eq_wg'] = np.where(g == 'grain', eq * ws, 0.0)
    purch['eq_rg'] = np.where(g == 'grain', eq * (1 - ws), 0.0)
    purch['eq_dy'] = np.where(g == 'dairy', eq, 0.0)
    purch['eq_pr'] = np.where(g == 'protein', eq, 0.0)
    purch['eq_sp'] = np.where(g == 'seafood_plant', eq, 0.0)
    for c in ['eq_fw','eq_fj','eq_v','eq_gb','eq_lg','eq_wg','eq_rg','eq_dy','eq_pr','eq_sp']:
        purch[c] = purch[c].fillna(0)

    # ---- label components (full basket, dictionary values) ----
    for nut, col in (('sodium','sodium_g'), ('satfat','satfat_g'), ('addsugar','addsug_g'), ('mufat','mufat_g'), ('pufat','pufat_g')):
        purch[col] = purch['grams'] * purch[f'{nut}_per_100g'].fillna(0) / 100

    agg = purch.groupby('household_code').agg(
        cals_full=('cals_full','sum'),
        eq_fw=('eq_fw','sum'), eq_fj=('eq_fj','sum'), eq_v=('eq_v','sum'),
        eq_gb=('eq_gb','sum'), eq_lg=('eq_lg','sum'), eq_wg=('eq_wg','sum'),
        eq_rg=('eq_rg','sum'), eq_dy=('eq_dy','sum'), eq_pr=('eq_pr','sum'),
        eq_sp=('eq_sp','sum'),
        sodium_g=('sodium_g','sum'), satfat_g=('satfat_g','sum'),
        addsug_g=('addsug_g','sum'),
        mufat_g=('mufat_g','sum'), pufat_g=('pufat_g','sum')).reset_index()
    agg['panel_year'] = year
    hh_parts.append(agg)
    log(f"  {year}: {len(agg):,} HHs")
    del purch, agg
    gc.collect()

hh = pd.concat(hh_parts, ignore_index=True)
_ae = adult_equivalents(YEARS)
_ps = pd.read_parquet(PANELISTS, columns=['household_code','panel_year','household_size'])
hh = hh.merge(_ae[['household_code','panel_year','ae_total']], on=['household_code','panel_year'], how='left') \
       .merge(_ps, on=['household_code','panel_year'], how='left')
_keep = in_sample(hh['cals_full'], hh['ae_total'].fillna(hh['household_size']))
log(f"Below 5% of calorie need: {(~_keep).sum():,} of {len(hh):,} HH-years dropped")
hh = hh[_keep].drop(columns=['ae_total','household_size'])

# ============================================================
# DENSITIES + HEI SCORING
# ============================================================
kf = hh['cals_full'] / 1000.0        # thousand kcal, full basket
d = pd.DataFrame({'household_code': hh['household_code'], 'panel_year': hh['panel_year']})
tf = (hh['eq_fw'] + hh['eq_fj']) / kf
wf = hh['eq_fw'] / kf
tv = (hh['eq_v'] + hh['eq_gb'] + hh['eq_lg']) / kf
gb = (hh['eq_gb'] + hh['eq_lg']) / kf
wg = hh['eq_wg'] / kf
rg = hh['eq_rg'] / kf
dy = hh['eq_dy'] / kf
pr = (hh['eq_pr'] + hh['eq_sp'] + hh['eq_lg']) / kf
sp = (hh['eq_sp'] + hh['eq_lg']) / kf
sod = hh['sodium_g'] / kf
fa  = (hh['mufat_g'] + hh['pufat_g']) / hh['satfat_g'].replace(0, np.nan)
satE = 100 * 9 * hh['satfat_g'] / hh['cals_full']
asE  = 100 * 4 * hh['addsug_g'] / hh['cals_full']

def adequacy(x, std, pts):
    return pts * np.clip(x / std, 0, 1)

def moderation(x, good, bad, pts):
    return pts * np.clip((bad - x) / (bad - good), 0, 1)

d['hei_c_totfruit']  = adequacy(tf, 0.8, 5)
d['hei_c_wholfruit'] = adequacy(wf, 0.4, 5)
d['hei_c_totveg']    = adequacy(tv, 1.1, 5)
d['hei_c_greensbeans'] = adequacy(gb, 0.2, 5)
d['hei_c_wholegrain'] = adequacy(wg, 1.5, 10)
d['hei_c_dairy']     = adequacy(dy, 1.3, 10)
d['hei_c_totprot']   = adequacy(pr, 2.5, 5)
d['hei_c_seaplant']  = adequacy(sp, 0.8, 5)
d['hei_c_fattyacid'] = moderation(-fa, -2.5, -1.2, 10)   # higher ratio better
d['hei_c_refgrain']  = moderation(rg, 1.8, 4.3, 10)
d['hei_c_sodium']    = moderation(sod, 1.1, 2.0, 10)
d['hei_c_addsugar']  = moderation(asE, 6.5, 26.0, 10)
d['hei_c_satfat']    = moderation(satE, 8.0, 16.0, 10)

comp = [c for c in d.columns if c.startswith('hei_c_')]
d['hei_usda'] = d[comp].sum(axis=1)
d['hei_usda_90'] = d[[c for c in comp if c != 'hei_c_fattyacid']].sum(axis=1) * 100 / 90
d['addsug_energy_share'] = asE
d['fa_ratio'] = fa
# Linear HEI: the primary measure of Allcott et al. (2019) ($HealthVar = lHEI_per1000Cal, GetlinearHEI.do), which they call the
# Health Index: the same standards without the caps and without the fatty-acid ratio. Saturated fat stands in for their solid
# fats, which labels do not report; the HEI-2015 standards for the two are the same 8 and 16 percent of energy.
d['lhei_usda'] = (5 * tf / 0.8 + 5 * wf / 0.4 + 5 * tv / 1.1 + 5 * gb / 0.2 + 10 * wg / 1.5 + 10 * dy / 1.3 + 5 * pr / 2.5 + 5 * sp / 0.8
                  + 10 * (4.3 - rg) / (4.3 - 1.8) + 10 * (2.0 - sod) / (2.0 - 1.1) + 10 * (26.0 - asE) / (26.0 - 6.5) + 10 * (16.0 - satE) / (16.0 - 8.0))

# panelists inner-merge (mirrors the other HI builders)
pan = pd.read_parquet(PANELISTS, columns=['household_code','panel_year','projection_factor'])
pan = pan[pan['panel_year'].isin(YEARS)]
d = d.merge(pan, on=['household_code','panel_year'], how='inner')
log(f"Final: {len(d):,} HH-years")
log("Component means:")
log(d[comp + ['hei_usda','hei_usda_90']].mean().round(2).to_string())

OUT_DIR.mkdir(parents=True, exist_ok=True)
d.to_parquet(OUT_DIR / 'hei_usda_panel.parquet', index=False)
dd = d.copy()
dd['household_code'] = dd['household_code'].astype('int64')
dd['panel_year'] = dd['panel_year'].astype('int32')
for c in dd.columns:
    if c not in ('household_code','panel_year'):
        dd[c] = dd[c].astype('float64')
dd.to_stata(OUT_DIR / 'hei_usda_panel.dta', write_index=False, version=118)
log(f"Saved {OUT_DIR/'hei_usda_panel.parquet'} + .dta")
