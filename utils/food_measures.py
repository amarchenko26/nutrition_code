"""One set of measurement rules for Nielsen purchases, used by every nutrition builder (2026-11-05).

Follows the replication code of Allcott, Diamond, Dube, Handbury, Rahkovsky and Schnell (2019),
food_deserts_replication/Code/DataPrep/OtherNielsen/GetGrams.do and UPCDataPrep.do. Until this module
each builder carried its own unit map and food-group lists, and the maps had no pound code.

Package grams (package_grams):
  * size1_amount x multi x unit, for OZ, PO (pounds), QT, LI, ML, GR. Fluid ounces and liters at the
    density of water. Multipacks count in full: a 12-pack of 12 oz cans is 144 oz.
  * Count-sized products (CT): the z-score rule of GetGrams.do. A product's total count is placed in the
    distribution of total counts of its module's count-sized UPCs, and gets the grams at the same z-score
    of the distribution of grams of the module's weight-sized UPCs, trimmed to that distribution's min and
    max; the product group's distributions are the fallback. Eggs are 60 g each, as in GetGrams.do.
  * Powdered mixes sized in quarts of prepared drink (modules 1048, 1050, 1298). Not in GetGrams.do, whose
    calories come from label totals rather than a density x grams: here a label density of 100 kcal/100 g or
    more is per 100 g of powder, so the package holds size x grams of powder per prepared quart (from
    build/health/build_package_params.py); a lower density is per 100 g of prepared drink, so the package
    counts at 946 g per quart. Without this a 34-quart can of lemonade mix counts as 32 kg of powder.

Food groups (is_fruit, is_veg): the module and group code lists of UPCDataPrep.do. Fruit juice and fruit
drinks are not fruit, rice and dry beans are not vegetables, precut salad is a vegetable.

Sample rule (in_sample): the household bought more than 5% of its calorie need in the year
(SetGlobals.do, CollapseTransactions.do).
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import numpy as np
import pandas as pd

UNIT_TO_GRAMS = {'GR': 1.0, 'OZ': 28.35, 'PO': 453.59, 'LI': 1000.0, 'ML': 1.0, 'QT': 946.35295,
                 # spellings not in the 2004-2020 food purchases, kept so a new file cannot silently drop rows
                 'LB': 453.59, 'FL OZ': 28.35, 'GAL': 3785.41, 'LT': 1000.0, 'KG': 1000.0, 'GM': 1.0}
GRAMS_PER_QT_PREPARED = 946.35295
POWDER_MODULES = {1048, 1050, 1298}          # breakfast drinks, soft drinks, milk: powdered, sized in prepared quarts
POWDER_DENSITY_MIN = 100.0                   # kcal/100 g: at or above, the label is per 100 g of powder
EGG_GROUP = 2505
GRAMS_PER_EGG = 60.0
MIN_CALORIE_SHARE = 0.05                     # SetGlobals.do, MinInSampleCalorieShareObserved
MIN_PACKAGE_KCAL = 1.0                       # UPCDataPrep.do: per-1,000-kcal measures missing at or below 1 kcal

FRESH_FRUIT_MODULES = {453, 3560, 3563, 4010, 4085, 4180, 4225, 4230, 4355, 4470, 6049, 6050}
FRUIT_GROUPS = {504, 1010}                   # fruit canned, dried
FRUIT_MODULES = {6, 42, 2664}                # unclassified fruit, frozen fruit
FRESH_VEG_MODULES = {460, 3544, 4015, 4020, 4023, 4050, 4055, 4060, 4140, 4275, 4280, 4350, 4400, 4415, 4460, 4475, 6064, 6070}
VEG_GROUPS = {514, 2010}                     # vegetables canned, frozen
VEG_MODULES = {24, 96, 1316, 3565}           # unclassified vegetables, dried peas, lentils and corn, sauerkraut
VEG_EXCLUDED_MODULES = {1071, 2618, 2635, 2637, 2638, 2639}   # creamed corn, vegetables in pastry, breaded, in sauce

PARAMS_DIR = idir / 'health'
_cache = {}


def _load_params():
    if 'ct' not in _cache:
        _cache['ct'] = pd.read_parquet(PARAMS_DIR / 'count_gram_params.parquet')
        _cache['powder'] = pd.read_parquet(PARAMS_DIR / 'powder_grams_per_quart.parquet').set_index('upc_13')['g_per_qt']
    return _cache['ct'], _cache['powder']


def is_fruit(module, group):
    m, g = pd.to_numeric(pd.Series(module), errors='coerce'), pd.to_numeric(pd.Series(group), errors='coerce')
    return (m.isin(FRESH_FRUIT_MODULES | FRUIT_MODULES) | g.isin(FRUIT_GROUPS)).values


def is_veg(module, group):
    m, g = pd.to_numeric(pd.Series(module), errors='coerce'), pd.to_numeric(pd.Series(group), errors='coerce')
    return ((m.isin(FRESH_VEG_MODULES | VEG_MODULES) | g.isin(VEG_GROUPS)) & ~m.isin(VEG_EXCLUDED_MODULES)).values


def _count_grams(total_ct, module, group, ct):
    """z-score rule of GetGrams.do; module distributions first, then group distributions."""
    out = np.full(len(total_ct), np.nan)
    for level, key in (('module', module), ('group', group)):
        p = ct[ct['level'] == level].set_index('code')
        k = pd.Series(key).values
        mean_ct, sd_ct = p['mean_ct'].reindex(k).values, p['sd_ct'].reindex(k).values
        mean_g, sd_g = p['mean_g'].reindex(k).values, p['sd_g'].reindex(k).values
        lo, hi = p['min_g'].reindex(k).values, p['max_g'].reindex(k).values
        z = (total_ct - mean_ct) / np.where(sd_ct > 0, sd_ct, np.nan)
        g = np.where(np.isnan(z), mean_g, z * np.nan_to_num(sd_g) + mean_g)
        g = np.minimum(np.maximum(g, lo), hi)
        out = np.where(np.isnan(out), g, out)
    return out


def package_grams(d, cal_col='cal_per_100g'):
    """Grams in one package, for a frame with upc_13, product_module_code, product_group_code, multi,
    size1_amount, size1_units and (for powdered mixes) the label density in cal_col. Multiply by quantity."""
    ct, powder = _load_params()
    unit = d['size1_units'].astype(str).str.strip()
    size = pd.to_numeric(d['size1_amount'], errors='coerce').values
    multi = pd.to_numeric(d['multi'], errors='coerce').fillna(1).clip(lower=1).values
    grams = size * multi * unit.map(UNIT_TO_GRAMS).values
    module = pd.to_numeric(d['product_module_code'], errors='coerce').values
    group = pd.to_numeric(d['product_group_code'], errors='coerce').values
    # count-sized products
    is_ct = (unit == 'CT').values
    if is_ct.any():
        grams[is_ct] = _count_grams(size[is_ct] * multi[is_ct], module[is_ct], group[is_ct], ct)
    # eggs
    eggs = is_ct & (group == EGG_GROUP)
    grams[eggs] = GRAMS_PER_EGG * size[eggs] * multi[eggs]
    # powdered mixes sized in prepared quarts
    pw = (unit == 'QT').values & np.isin(module, list(POWDER_MODULES))
    if pw.any():
        per_qt = d['upc_13'].values[pw]
        g_qt = powder.reindex(per_qt).values
        if cal_col in d.columns:
            dens = pd.to_numeric(d[cal_col], errors='coerce').values[pw]
            g_qt = np.where(dens < POWDER_DENSITY_MIN, GRAMS_PER_QT_PREPARED, g_qt)
        grams[pw] = size[pw] * multi[pw] * g_qt
    grams[~(grams > 0)] = np.nan
    return grams


def calorie_need(age, male):
    """USDA Dietary Guidelines daily calorie need, moderately active, by age and sex (midpoints)."""
    bins = [4, 9, 14, 19, 31, 51, np.inf]
    male_need = [1200, 1500, 2000, 2600, 2700, 2500, 2300]
    female_need = [1200, 1400, 1800, 2000, 2100, 2000, 1800]
    idx = np.searchsorted(bins, age, side='right')
    return np.where(male, np.take(male_need, idx), np.take(female_need, idx))


def adult_equivalents(years=range(2004, 2021)):
    """household_code, panel_year, ae_total (sum of members' calorie need / 2,000), ae_complete (roster = size)."""
    cols = (['household_code', 'panel_year', 'household_size', 'male_head_birth', 'female_head_birth'] +
            [f'member_{i}_{v}' for i in range(1, 8) for v in ('birth', 'relationship_sex')])
    pan = pd.read_parquet(idir / 'panelists' / 'panelists_all_years.parquet', columns=cols)
    pan = pan[pan['panel_year'].isin(years)]
    people = []
    for role, sexmale in [('male_head_birth', True), ('female_head_birth', False)]:
        p = pan[['household_code', 'panel_year', role]].rename(columns={role: 'birth'}).dropna()
        p['male'] = sexmale
        people.append(p)
    for i in range(1, 8):
        p = pan[['household_code', 'panel_year', f'member_{i}_birth', f'member_{i}_relationship_sex']] \
            .rename(columns={f'member_{i}_birth': 'birth', f'member_{i}_relationship_sex': 'rel'}).dropna()
        p['male'] = p['rel'].isin([1, 3, 5])
        people.append(p[['household_code', 'panel_year', 'birth', 'male']])
    people = pd.concat(people, ignore_index=True)
    people['age'] = (people['panel_year'] - people['birth']).clip(0, 100)
    people['ae'] = calorie_need(people['age'].values, people['male'].values) / 2000.0
    ae = people.groupby(['household_code', 'panel_year']).agg(ae_total=('ae', 'sum'), n_counted=('ae', 'size')).reset_index()
    ae = ae.merge(pan[['household_code', 'panel_year', 'household_size']], on=['household_code', 'panel_year'])
    ae['ae_complete'] = (ae['n_counted'] == ae['household_size']).astype(int)
    return ae[['household_code', 'panel_year', 'ae_total', 'ae_complete']]


def in_sample(total_kcal, ae_total):
    """True when the year's purchased calories exceed 5% of the household's annual calorie need."""
    return np.asarray(total_kcal) / (np.asarray(ae_total) * 2000.0 * 365.0) > MIN_CALORIE_SHARE
