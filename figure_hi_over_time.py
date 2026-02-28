"""
Nutritional inequality over time: Health Index by income quintile for selected years.
Uses the same pipeline as replicate_figure1.py (Syndigo nutrition, simple HI).
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import gc
import warnings
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
FIG_DIR = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs')
FIG_DIR.mkdir(parents=True, exist_ok=True)

PLOT_YEARS = [2005, 2010, 2015, 2020]
ALL_YEARS = range(2004, 2021)  # need all years for HHAvIncome averaging
SAMPLE_FRAC = 0.1
MIN_ANNUAL_CALS = 50_000

UNIT_TO_GRAMS = {
    'OZ': 28.3495, 'LB': 453.592, 'FL OZ': 29.5735, 'QT': 946.353,
    'GAL': 3785.41, 'ML': 1.0, 'LT': 1000.0, 'KG': 1000.0, 'GM': 1.0, 'GR': 1.0,
}

FRUIT_GROUPS = {'FRUIT - CANNED', 'FRUIT - DRIED', 'FRUIT'}
FRUIT_MODULES_FRESH = {4010, 4085, 4180, 4225, 4355, 4470}
VEG_GROUPS = {'VEGETABLES - CANNED', 'VEGETABLES-FROZEN', 'VEGETABLES AND GRAINS - DRIED'}
VEG_MODULES_FRESH = {4015, 4020, 4023, 4050, 4055, 4060, 4140, 4230, 4275, 4280, 4350, 4400, 4415, 4460, 4475}

# ============================================================
# STEP 1: SYNDIGO
# ============================================================
log("Step 1: Preparing Syndigo...")
syn = pd.read_parquet(SYNDIGO)
nutrient_map = {
    1: 'cals_per_100g', 5: 'satfat_per_100g', 8: 'cholest_per_100g',
    9: 'sodium_per_100g', 12: 'fiber_per_100g', 15: 'sugar_per_100g'
}
syn_filt = syn[syn['nutrient_id'].isin(nutrient_map.keys())].copy()
syn_filt['nut_name'] = syn_filt['nutrient_id'].map(nutrient_map)
pkg = syn[syn['nutrient_id'] == 1][['upc', 'g_total']].drop_duplicates('upc')
syn_wide = syn_filt.pivot_table(index='upc', columns='nut_name', values='nut_per_100g', aggfunc='first').reset_index()
syn_wide = syn_wide.merge(pkg, on='upc', how='left')
syn_wide.rename(columns={'upc': 'upc_13'}, inplace=True)

# Cap outliers (sodium/cholesterol UOM misclassification)
syn_wide.loc[syn_wide['sodium_per_100g'] > 5, 'sodium_per_100g'] = np.nan
syn_wide.loc[syn_wide['cholest_per_100g'] > 2, 'cholest_per_100g'] = np.nan

log(f"  {len(syn_wide):,} UPCs")
del syn, syn_filt, pkg
gc.collect()

# ============================================================
# STEP 2: PANELISTS
# ============================================================
log("Step 2: Loading panelists...")
pan = pd.read_parquet(PANELISTS)
pan = pan[pan['panel_year'].isin(ALL_YEARS)].copy()

# CPI deflator to 2010 dollars
cpi = {2004:188.9, 2005:195.3, 2006:201.6, 2007:207.3, 2008:215.3,
       2009:214.5, 2010:218.1, 2011:224.9, 2012:229.6, 2013:233.0,
       2014:236.7, 2015:237.0, 2016:240.0, 2017:245.1, 2018:251.1,
       2019:255.7, 2020:258.8}
cpi_2010 = cpi[2010]
pan['real_income'] = pan['household_income_midpoint'] * (cpi_2010 / pan['panel_year'].map(cpi)) / 1000

hh_av = pan.groupby('household_code')['real_income'].mean().rename('HHAvIncome')
pan = pan.merge(hh_av, on='household_code', how='left')

age_map = {1: 27, 2: 32, 3: 37, 4: 42, 5: 47, 6: 52, 7: 57, 8: 62, 9: 67, 0: np.nan}
pan['male_age'] = pan['male_head_age'].map(age_map)
pan['female_age'] = pan['female_head_age'].map(age_map)
pan['AgeInt'] = pan[['male_age', 'female_age']].mean(axis=1).clip(23, 90).round().fillna(45).astype(int)
pan['projection_factor'] = pan['projection_factor'].astype(float)

# Income quintiles (weighted, using HHAvIncome)
# Compute weighted quintile cutpoints from the full panel.
all_inc = pan.drop_duplicates('household_code')[
    ['household_code', 'HHAvIncome', 'projection_factor']
].dropna()
all_inc = all_inc[all_inc['projection_factor'] > 0].sort_values('HHAvIncome').reset_index(drop=True)

cumwt = all_inc['projection_factor'].cumsum().to_numpy()
cumwt = cumwt / cumwt[-1]
income_vals = all_inc['HHAvIncome'].to_numpy()
cuts = np.interp([0.2, 0.4, 0.6, 0.8], cumwt, income_vals).tolist()

# If ties produce duplicate cutpoints, nudge slightly upward to keep bins valid.
for i in range(1, len(cuts)):
    if cuts[i] <= cuts[i - 1]:
        cuts[i] = np.nextafter(cuts[i - 1], np.inf)

pan['IncomeQuintile'] = pd.cut(
    pan['HHAvIncome'],
    bins=[-np.inf] + cuts + [np.inf],
    labels=[1, 2, 3, 4, 5],
    include_lowest=True
).astype(float)

log(f"  {pan['household_code'].nunique():,} HHs")

# ============================================================
# STEP 3: SAMPLE
# ============================================================
np.random.seed(42)
all_hh = pan['household_code'].unique()
sample_hh = set(np.random.choice(all_hh, size=int(len(all_hh) * SAMPLE_FRAC), replace=False))
pan = pan[pan['household_code'].isin(sample_hh)]
log(f"Step 3: Sampled {len(sample_hh):,} households")

# ============================================================
# STEP 4: PROCESS PURCHASES (only plot years)
# ============================================================
log("Step 4: Processing purchases...")
hh_year_list = []

for year in PLOT_YEARS:
    log(f"  {year}...")
    purch = pd.read_parquet(PURCHASES / f'panel_year={year}')
    purch['panel_year'] = year
    purch = purch[purch['household_code'].isin(sample_hh)]
    purch = purch[purch['department_descr'] != 'MAGNET DATA']

    purch['upc_13'] = '0' + purch['upc'].astype(str).str.zfill(12)
    purch = purch.merge(syn_wide, on='upc_13', how='left')

    n_total = len(purch)
    n_matched = purch['cals_per_100g'].notna().sum()

    purch['g_conv'] = purch['size1_units'].map(UNIT_TO_GRAMS) * purch['size1_amount']
    purch['g_pkg'] = purch['g_total'].where(purch['g_total'] > 0, purch['g_conv'])
    purch['cals_per_row'] = purch['quantity'] * purch['g_pkg'] * purch['cals_per_100g'] / 100
    purch = purch[purch['cals_per_row'].notna() & (purch['cals_per_row'] > 0)]

    # Classify fruit/veg
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

    # Health Index
    is_fv = purch['is_fruit'] | purch['is_veg']
    purch['hi_per_100g'] = np.where(
        is_fv,
        purch['is_fruit'].astype(float) * 100/320 + purch['is_veg'].astype(float) * 100/390,
        purch['fiber_per_100g'].fillna(0) / 29.5
        - purch['sugar_per_100g'].fillna(0) / 32.8
        - purch['satfat_per_100g'].fillna(0) / 17.2
        - purch['sodium_per_100g'].fillna(0) / 2.3
        - purch['cholest_per_100g'].fillna(0) / 0.3
    )
    purch['hi_per_1000cal'] = purch['hi_per_100g'] / purch['cals_per_100g'] * 1000

    # Calorie-weighted collapse
    purch['wt_hi'] = purch['hi_per_1000cal'] * purch['cals_per_row']
    agg = purch.groupby('household_code').agg(
        total_cals=('cals_per_row', 'sum'),
        sum_wt_hi=('wt_hi', 'sum'),
    ).reset_index()
    agg['panel_year'] = year
    agg['rHI'] = agg['sum_wt_hi'] / agg['total_cals']
    hh_year_list.append(agg[['household_code', 'panel_year', 'total_cals', 'rHI']])

    log(f"    matched {n_matched/n_total*100:.0f}%, {len(agg):,} HHs")
    del purch, agg
    gc.collect()

# ============================================================
# STEP 5: COMBINE & NORMALIZE
# ============================================================
log("Step 5: Combining & normalizing...")
hhy = pd.concat(hh_year_list, ignore_index=True)
hhy = hhy[hhy['total_cals'] >= MIN_ANNUAL_CALS]

hhy = hhy.merge(
    pan[['household_code', 'panel_year', 'HHAvIncome', 'IncomeQuintile', 'projection_factor']],
    on=['household_code', 'panel_year'], how='inner'
)

# Normalize HI: pooled mean and within-year residual SD (across all plot years)
mask = hhy['rHI'].notna()
wmean = np.average(hhy.loc[mask, 'rHI'], weights=hhy.loc[mask, 'projection_factor'])
yr_means = hhy[mask].groupby('panel_year').apply(
    lambda g: np.average(g['rHI'], weights=g['projection_factor']))
resid = hhy['rHI'] - hhy['panel_year'].map(yr_means)
r_valid = resid.notna()
wsd = np.sqrt(np.average(
    (resid[r_valid] - np.average(resid[r_valid], weights=hhy.loc[r_valid, 'projection_factor']))**2,
    weights=hhy.loc[r_valid, 'projection_factor'].values))
hhy['HI'] = (hhy['rHI'] - wmean) / wsd

log(f"  {len(hhy):,} HH-year obs, mean(raw)={wmean:.4f}, sd={wsd:.4f}")

# ============================================================
# STEP 6: COMPUTE QUINTILE MEANS BY YEAR
# ============================================================
log("Step 6: Computing quintile means by year...")
results = []
for year in PLOT_YEARS:
    for q in [1, 2, 3, 4, 5]:
        sub = hhy[(hhy['panel_year'] == year) & (hhy['IncomeQuintile'] == q)]
        if len(sub) > 0:
            wm = np.average(sub['HI'], weights=sub['projection_factor'])
            results.append({'year': year, 'quintile': int(q), 'HI': wm, 'n': len(sub)})

res = pd.DataFrame(results)
log(res.pivot(index='quintile', columns='year', values='HI').to_string())

# ============================================================
# STEP 7: FIGURE
# ============================================================
log("Step 7: Creating figure...")

fig, ax = plt.subplots(figsize=(8, 5.5))

colors = {2005: '#a6cee3', 2010: '#1f78b4', 2015: '#b2df8a', 2020: '#33a02c'}
markers = {2005: 'o', 2010: 's', 2015: '^', 2020: 'D'}
quintile_labels = ['Q1\n(Lowest)', 'Q2', 'Q3', 'Q4', 'Q5\n(Highest)']

for year in PLOT_YEARS:
    yr_data = res[res['year'] == year].sort_values('quintile')
    ax.plot(yr_data['quintile'], yr_data['HI'],
            marker=markers[year], color=colors[year], linewidth=2, markersize=8,
            label=str(year), zorder=5)

ax.set_xticks([1, 2, 3, 4, 5])
ax.set_xticklabels(quintile_labels, fontsize=11)
ax.set_xlabel('Household Income Quintile', fontsize=12)
ax.set_ylabel('Health Index (std. dev.)', fontsize=12)
ax.set_title('Healthfulness of Grocery Purchases by Income Over Time', fontsize=14, fontweight='bold')
ax.legend(title='Year', fontsize=11, title_fontsize=11)
ax.grid(True, alpha=0.3)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.axhline(0, color='gray', linewidth=0.5, linestyle='--', alpha=0.5)

plt.tight_layout()
plt.savefig(FIG_DIR / 'hi_by_income_over_time.pdf', bbox_inches='tight')
plt.savefig(FIG_DIR / 'hi_by_income_over_time.png', bbox_inches='tight', dpi=150)
log(f"\nSaved to {FIG_DIR / 'hi_by_income_over_time.pdf'}")
log("Done!")
