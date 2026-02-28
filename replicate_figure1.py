"""
Replication of Allcott, Diamond, Dubé, Handbury, Rahkovsky, Schnell (2019)
Figure 1: Healthfulness of Grocery Purchases by Household Income

4-panel binscatter:
  A) Sugar per 1000 Cal (total sugars — added sugars unavailable from Syndigo)
  B) Whole grain share (share of bread/baked goods calories from whole grain products)
  C) Produce (fruit + veg calorie share)
  D) Health Index per 1000 Cal (simple HI: fiber - sugar - satfat - sodium - cholesterol)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import gc
import warnings
import sys

warnings.filterwarnings('ignore')
def log(msg):
    print(msg, flush=True)

# ============================================================
# PATHS & SETTINGS
# ============================================================
BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
PURCHASES = BASE / 'interim' / 'purchases_food'
SYNDIGO = BASE / 'interim' / 'syndigo' / 'syndigo_nutrients_master.parquet'
PANELISTS = BASE / 'interim' / 'panelists' / 'panelists_all_years.parquet'
OUTPUT = Path('/Users/anyamarchenko/Documents/GitHub/corn/output')
OUTPUT.mkdir(exist_ok=True)

YEARS = range(2004, 2017)
SAMPLE_FRAC = 0.4
MIN_ANNUAL_CALS = 50_000
N_QUANTILES = 15

UNIT_TO_GRAMS = {
    'OZ': 28.3495, 'LB': 453.592, 'FL OZ': 29.5735, 'QT': 946.353,
    'GAL': 3785.41, 'ML': 1.0, 'LT': 1000.0, 'KG': 1000.0, 'GM': 1.0, 'GR': 1.0,
}

# Product classification sets
FRUIT_GROUPS = {'FRUIT - CANNED', 'FRUIT - DRIED', 'FRUIT'}
FRUIT_MODULES_FRESH = {4010, 4085, 4180, 4225, 4355, 4470}
VEG_GROUPS = {'VEGETABLES - CANNED', 'VEGETABLES-FROZEN', 'VEGETABLES AND GRAINS - DRIED'}
VEG_MODULES_FRESH = {4015, 4020, 4023, 4050, 4055, 4060, 4140, 4230, 4275, 4280, 4350, 4400, 4415, 4460, 4475}
BREAD_GROUPS = {'BREAD AND BAKED GOODS'}
BREAD_MODULES = {4000, 4001, 4002, 4003, 4008, 4009, 4011}

# ============================================================
# STEP 1: SYNDIGO NUTRITION LOOKUP
# ============================================================
log("Step 1: Preparing Syndigo nutrition lookup...")
syn = pd.read_parquet(SYNDIGO)

nutrient_map = {
    1: 'cals_per_100g', 5: 'satfat_per_100g', 8: 'cholest_per_100g',
    9: 'sodium_per_100g', 12: 'fiber_per_100g', 15: 'sugar_per_100g'
}
syn_filt = syn[syn['nutrient_id'].isin(nutrient_map.keys())].copy()
syn_filt['nut_name'] = syn_filt['nutrient_id'].map(nutrient_map)
pkg = syn[syn['nutrient_id'] == 1][['upc', 'g_total']].drop_duplicates('upc')

syn_wide = syn_filt.pivot_table(
    index='upc', columns='nut_name', values='nut_per_100g', aggfunc='first'
).reset_index()
syn_wide = syn_wide.merge(pkg, on='upc', how='left')
syn_wide.rename(columns={'upc': 'upc_13'}, inplace=True)
log(f"  Syndigo: {len(syn_wide):,} unique UPCs")
del syn, syn_filt, pkg
gc.collect()

# ============================================================
# STEP 2: PANELISTS
# ============================================================
log("Step 2: Loading panelists...")
pan = pd.read_parquet(PANELISTS)

cpi = {2004: 188.9, 2005: 195.3, 2006: 201.6, 2007: 207.3, 2008: 215.3,
       2009: 214.5, 2010: 218.1, 2011: 224.9, 2012: 229.6, 2013: 233.0,
       2014: 236.7, 2015: 237.0, 2016: 240.0}
cpi_2010 = cpi[2010]

pan = pan[pan['panel_year'].isin(YEARS)].copy()
pan['real_income'] = pan['household_income_midpoint'] * (cpi_2010 / pan['panel_year'].map(cpi)) / 1000

hh_av = pan.groupby('household_code')['real_income'].mean().rename('HHAvIncome')
pan = pan.merge(hh_av, on='household_code', how='left')

age_map = {1: 27, 2: 32, 3: 37, 4: 42, 5: 47, 6: 52, 7: 57, 8: 62, 9: 67, 0: np.nan}
pan['male_age'] = pan['male_head_age'].map(age_map)
pan['female_age'] = pan['female_head_age'].map(age_map)
pan['AgeInt'] = pan[['male_age', 'female_age']].mean(axis=1).clip(23, 90).round().fillna(45).astype(int)
pan['projection_factor'] = pan['projection_factor'].astype(float)

log(f"  {pan['household_code'].nunique():,} unique HHs")

# ============================================================
# STEP 3: 20% HOUSEHOLD SAMPLE
# ============================================================
np.random.seed(42)
all_hh = pan['household_code'].unique()
sample_hh = set(np.random.choice(all_hh, size=int(len(all_hh) * SAMPLE_FRAC), replace=False))
pan = pan[pan['household_code'].isin(sample_hh)]
log(f"Step 3: Sampled {len(sample_hh):,} households")

# ============================================================
# STEP 4: PROCESS PURCHASES YEAR BY YEAR
# ============================================================
log("Step 4: Processing purchases year by year...")
hh_year_list = []

for year in YEARS:
    log(f"  {year}...")
    purch = pd.read_parquet(PURCHASES / f'panel_year={year}')
    purch['panel_year'] = year
    purch = purch[purch['household_code'].isin(sample_hh)]
    purch = purch[purch['department_descr'] != 'MAGNET DATA']

    # UPC harmonization: Nielsen 12-digit -> prepend '0' -> 13-digit
    purch['upc_13'] = '0' + purch['upc'].astype(str).str.zfill(12)
    purch = purch.merge(syn_wide, on='upc_13', how='left')

    n_total = len(purch)
    n_matched = purch['cals_per_100g'].notna().sum()

    # Package weight: Syndigo g_total first, then unit conversion
    purch['g_conv'] = purch['size1_units'].map(UNIT_TO_GRAMS) * purch['size1_amount']
    purch['g_pkg'] = purch['g_total'].where(purch['g_total'] > 0, purch['g_conv'])

    # Calories per row
    purch['cals_per_row'] = purch['quantity'] * purch['g_pkg'] * purch['cals_per_100g'] / 100
    purch = purch[purch['cals_per_row'].notna() & (purch['cals_per_row'] > 0)]

    # -- Classify products (vectorized) --
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
    purch['is_bread'] = (
        purch['product_group'].isin(BREAD_GROUPS) |
        purch['product_module_code'].isin(BREAD_MODULES)
    )
    purch['is_whole'] = purch['is_bread'] & purch['upc_descr'].str.contains(
        'WHOLE WHEAT|WHOLE GRAIN|WHL WHT|WHL GRN|100% WHEAT|MULTIGRAIN|MULTI.GRAIN',
        case=False, na=False)

    # -- Compute per-row measures --
    purch['sugar_per_1000cal'] = purch['sugar_per_100g'] / purch['cals_per_100g'] * 1000

    # Health Index per 100g: fruit/veg get fixed score, others use 5-nutrient formula
    is_fv = purch['is_fruit'] | purch['is_veg']
    purch['hi_per_100g'] = np.where(
        is_fv,
        purch['is_fruit'].astype(float) * 100/320 + purch['is_veg'].astype(float) * 100/390,
        purch['fiber_per_100g'].fillna(0) / 29.5
        - purch['sugar_per_100g'].fillna(0) / 32.8
        - purch['satfat_per_100g'].fillna(0) / 17.2
        - (purch['sodium_per_100g'].fillna(0) / 1000) / 2.3
        - (purch['cholest_per_100g'].fillna(0) / 1000) / 0.3
    )
    purch['hi_per_1000cal'] = purch['hi_per_100g'] / purch['cals_per_100g'] * 1000

    # -- Pre-multiply for efficient calorie-weighted aggregation --
    purch['wt_sugar'] = purch['sugar_per_1000cal'] * purch['cals_per_row']
    purch['wt_hi'] = purch['hi_per_1000cal'] * purch['cals_per_row']
    purch['fruit_cals'] = purch['cals_per_row'] * purch['is_fruit'].astype(float)
    purch['veg_cals'] = purch['cals_per_row'] * purch['is_veg'].astype(float)
    purch['bread_cals'] = purch['cals_per_row'] * purch['is_bread'].astype(float)
    purch['whole_cals'] = purch['cals_per_row'] * purch['is_whole'].astype(float)

    # Collapse to HH-year
    agg = purch.groupby('household_code').agg(
        total_cals=('cals_per_row', 'sum'),
        sum_wt_sugar=('wt_sugar', 'sum'),
        sum_wt_hi=('wt_hi', 'sum'),
        fruit_cals=('fruit_cals', 'sum'),
        veg_cals=('veg_cals', 'sum'),
        bread_cals=('bread_cals', 'sum'),
        whole_cals=('whole_cals', 'sum'),
    ).reset_index()

    agg['panel_year'] = year
    agg['sugar_per_1000cal'] = agg['sum_wt_sugar'] / agg['total_cals']
    agg['rHI_per_1000cal'] = agg['sum_wt_hi'] / agg['total_cals']
    agg['Produce'] = (agg['fruit_cals'] + agg['veg_cals']) / agg['total_cals']
    agg['Whole'] = np.where(agg['bread_cals'] > 0, agg['whole_cals'] / agg['bread_cals'], np.nan)

    hh_year_list.append(agg[['household_code', 'panel_year', 'total_cals',
                              'sugar_per_1000cal', 'rHI_per_1000cal', 'Produce', 'Whole']])

    log(f"    matched {n_matched/n_total*100:.0f}%, {len(purch):,} rows -> {len(agg):,} HHs")
    del purch, agg
    gc.collect()

# ============================================================
# STEP 5: COMBINE & MERGE DEMOGRAPHICS
# ============================================================
log("Step 5: Combining years & merging demographics...")
hhy = pd.concat(hh_year_list, ignore_index=True)
hhy = hhy[hhy['total_cals'] >= MIN_ANNUAL_CALS]

hhy = hhy.merge(
    pan[['household_code', 'panel_year', 'HHAvIncome', 'AgeInt', 'household_size', 'projection_factor']],
    on=['household_code', 'panel_year'], how='inner'
)

# Trim income outliers
p1, p99 = hhy['HHAvIncome'].quantile(0.01), hhy['HHAvIncome'].quantile(0.99)
hhy = hhy[(hhy['HHAvIncome'] >= p1) & (hhy['HHAvIncome'] <= p99)]
log(f"  {len(hhy):,} HH-year obs, {hhy['household_code'].nunique():,} HHs")

# ============================================================
# STEP 6: NORMALIZE HEALTH INDEX
# ============================================================
log("Step 6: Normalizing health index...")
mask = hhy['rHI_per_1000cal'].notna()
w = hhy.loc[mask, 'projection_factor'].values
y = hhy.loc[mask, 'rHI_per_1000cal'].values
wmean = np.average(y, weights=w)

# Year-demeaned residual SD
yr_means = hhy[mask].groupby('panel_year').apply(
    lambda g: np.average(g['rHI_per_1000cal'], weights=g['projection_factor']))
yr_effect = hhy['panel_year'].map(yr_means)
resid = hhy['rHI_per_1000cal'] - yr_effect
r_valid = resid.notna()
wsd = np.sqrt(np.average(
    (resid[r_valid] - np.average(resid[r_valid], weights=hhy.loc[r_valid, 'projection_factor']))**2,
    weights=hhy.loc[r_valid, 'projection_factor'].values))

hhy['HI_per_1000cal'] = (hhy['rHI_per_1000cal'] - wmean) / wsd
log(f"  HI: mean(raw)={wmean:.4f}, sd(resid)={wsd:.4f}")

# ============================================================
# STEP 7: BINSCATTER
# ============================================================
log("Step 7: Creating binscatter figure...")

def binscatter(df, yvar, xvar='HHAvIncome', controls=None, wvar='projection_factor', nq=15):
    """Weighted binscatter with controls partialed out."""
    cols = [yvar, xvar, wvar] + (controls or [])
    d = df[cols].dropna().copy()
    y = d[yvar].values.astype(float)
    x = d[xvar].values.astype(float)
    w = d[wvar].values.astype(float)

    if controls:
        C = d[controls].values.astype(float)
        y = _resid_wls(y, C, w)
        x = _resid_wls(x, C, w)
        # Add back weighted means
        y += np.average(d[yvar].values, weights=w)
        x += np.average(d[xvar].values, weights=w)

    edges = np.percentile(x, np.linspace(0, 100, nq + 1))
    edges[-1] += 1
    b = np.digitize(x, edges)

    xm, ym = [], []
    for i in range(1, nq + 1):
        m = b == i
        if m.sum() > 0:
            xm.append(np.average(x[m], weights=w[m]))
            ym.append(np.average(y[m], weights=w[m]))
    return np.array(xm), np.array(ym)


def _resid_wls(y, X, w):
    """Memory-efficient weighted OLS residuals."""
    sw = np.sqrt(w)
    Xc = np.column_stack([np.ones(len(y)), X])
    Xw = Xc * sw[:, None]
    yw = y * sw
    beta, _, _, _ = np.linalg.lstsq(Xw, yw, rcond=None)
    return y - Xc @ beta


# Controls: year dummies + age dummies + household_size
# Use a simpler control set to avoid huge dummy matrices
# Bin age into 5 groups instead of individual values
hhy['age_bin'] = pd.cut(hhy['AgeInt'], bins=[0, 35, 45, 55, 65, 100], labels=False)
age_dum = pd.get_dummies(hhy['age_bin'], prefix='a', drop_first=True, dtype=float)
yr_dum = pd.get_dummies(hhy['panel_year'], prefix='y', drop_first=True, dtype=float)
ctl_cols = list(age_dum.columns) + list(yr_dum.columns) + ['household_size']
hhy = pd.concat([hhy.reset_index(drop=True), age_dum.reset_index(drop=True), yr_dum.reset_index(drop=True)], axis=1)

# ---- Figure ----
fig, axes = plt.subplots(2, 2, figsize=(12, 10))

panels = [
    ('sugar_per_1000cal', 'Sugars (g per 1,000 Cal)', 'Panel A: Sugars'),
    ('Whole',             'Share whole grain',          'Panel B: Whole Grains'),
    ('Produce',           'Calorie share from produce', 'Panel C: Produce'),
    ('HI_per_1000cal',    'Health Index (std. dev.)',    'Panel D: Health Index'),
]

for ax, (var, ylabel, title) in zip(axes.flat, panels):
    log(f"  Plotting {var}...")
    xb, yb = binscatter(hhy, var, 'HHAvIncome', ctl_cols, 'projection_factor', N_QUANTILES)
    ax.scatter(xb, yb, color='#2c5f8a', s=50, zorder=5, edgecolors='white', linewidth=0.5)
    ax.set_xlabel('Household income ($000s)', fontsize=11)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_title(title, fontsize=13, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

plt.tight_layout(pad=2.0)
plt.savefig(OUTPUT / 'Healthfulness_Income.png', bbox_inches='tight', dpi=150)
log(f"\nSaved: {OUTPUT / 'Healthfulness_Income.png'}")

# Summary stats
log("\n=== Summary Statistics ===")
for var, label, _ in panels:
    v = hhy[var].dropna()
    log(f"  {label}: mean={v.mean():.3f}, sd={v.std():.3f}, N={len(v):,}")
log(f"  HHAvIncome: mean={hhy['HHAvIncome'].mean():.1f}, "
    f"p25={hhy['HHAvIncome'].quantile(0.25):.1f}, p75={hhy['HHAvIncome'].quantile(0.75):.1f}")
log("Done!")
