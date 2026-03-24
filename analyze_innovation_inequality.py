"""
analyze_innovation_inequality.py

Two figures relating nutrition, innovation, and household income:

  Fig 1: Expenditure share on new products (SSNP) by income quintile over time.
         Approach: compute county-level SSNP from RMS (spending-weighted across
         food modules), then match each panelist HH to their county's SSNP.
         This captures the innovation environment each HH faces in their local market.

  Fig 2: Expenditure share on new products by healthiness of product category
         (Claude HI quartiles). Spending-weighted, module level.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
RMS_VAR = BASE / 'interim' / 'rms_variety'
FIG_DIR = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs')
FIG_DIR.mkdir(parents=True, exist_ok=True)

N_INCOME_BINS = 4
bin_labels    = list(range(1, N_INCOME_BINS + 1))

# ============================================================
# LOAD
# ============================================================
print("Loading data...")

# RMS module×FIPS×year SSNP (drop 2006: left-censoring)
rms = pd.read_parquet(RMS_VAR / 'rms_variety_module_fips_year.parquet',
                      columns=['product_module_code', 'fips', 'year', 'ssnp',
                               'total_spending'])
rms = rms[(rms['year'] != 2006) & (rms['year'] <= 2020)].dropna(subset=['ssnp'])

# Collapse RMS to FIPS×year level (spending-weighted across modules)
# = "what fraction of total county food spending is on new products?"
rms['ssnp_wt'] = rms['ssnp'] * rms['total_spending']
fips_ssnp = (rms.groupby(['fips', 'year'], as_index=False)
             .agg(total_spending=('total_spending', 'sum'),
                  ssnp_num=('ssnp_wt', 'sum')))
fips_ssnp['ssnp_county'] = fips_ssnp['ssnp_num'] / fips_ssnp['total_spending']
fips_ssnp = fips_ssnp[['fips', 'year', 'ssnp_county']]

# Panelists: HH×year → FIPS + income
pan = pd.read_parquet(BASE / 'interim/panelists/panelists_all_years.parquet',
                      columns=['household_code', 'panel_year',
                               'fips_state_code', 'fips_county_code', 'projection_factor'])
pan = pan.rename(columns={'panel_year': 'year'})
pan['fips'] = (pan['fips_state_code'].astype(str).str.extract(r'(\d+)')[0].str.zfill(2) +
               pan['fips_county_code'].astype(str).str.extract(r'(\d+)')[0].str.zfill(3))

# Income quintiles from panel_hh_year
hhy = pd.read_parquet(BASE / 'interim/panel_dataset/panel_hh_year.parquet',
                      columns=['household_code', 'panel_year', 'real_income'])
hhy = hhy.rename(columns={'panel_year': 'year'})

# Assign within-year income quintiles (rank-based, same as figure_hi_over_time.py)
hhy['IncomeBin'] = np.nan
for year in sorted(hhy['year'].unique()):
    valid = (hhy['year'] == year) & hhy['real_income'].notna()
    if valid.sum() < N_INCOME_BINS:
        continue
    hhy.loc[valid, 'IncomeBin'] = pd.qcut(
        hhy.loc[valid, 'real_income'].rank(method='first'),
        q=N_INCOME_BINS, labels=bin_labels
    ).astype(float)

# ============================================================
# BUILD HH-LEVEL SSNP: match each HH to their county's SSNP
# ============================================================
print("Matching HHs to county SSNP...")

hh_ssnp = (pan[['household_code', 'year', 'fips', 'projection_factor']]
           .merge(fips_ssnp, on=['fips', 'year'], how='inner')
           .merge(hhy[['household_code', 'year', 'IncomeBin']],
                  on=['household_code', 'year'], how='inner'))
hh_ssnp = hh_ssnp[(hh_ssnp['year'] >= 2007) & (hh_ssnp['year'] <= 2020)]
hh_ssnp = hh_ssnp.dropna(subset=['IncomeBin', 'ssnp_county', 'projection_factor'])

print(f"  HH-year obs: {len(hh_ssnp):,} ({hh_ssnp['household_code'].nunique():,} HHs)")

# ============================================================
# BUILD MODULE × INCOME-QUINTILE SSNP
# Strategy: assign each FIPS county to an income quintile based on
# the projection-factor-weighted median income of its panelists,
# then average module SSNP within healthiness quartile × county income quintile.
# ============================================================
print("Building county income quintiles...")

# Weighted median income per county (across all years)
county_income = (pan[pan['year'] <= 2020]
                 .merge(hhy[['household_code', 'year', 'real_income']], on=['household_code', 'year'], how='inner')
                 .dropna(subset=['real_income', 'projection_factor']))
county_income = (county_income.groupby('fips', as_index=False)
                 .apply(lambda g: pd.Series({
                     'avg_income': np.average(g['real_income'], weights=g['projection_factor'])
                 }), include_groups=False)
                 .reset_index(drop=True))
county_income['county_inc_bin'] = pd.qcut(
    county_income['avg_income'].rank(method='first'),
    q=N_INCOME_BINS, labels=bin_labels
).astype(int)
print(f"  Counties with income data: {len(county_income):,}")

# Load Claude HI and coverage filter
claude_hi     = pd.read_parquet(RMS_VAR / 'claude_hi_scores.parquet',
                                 columns=['product_module_code', 'claude_hi'])
module_health = pd.read_parquet(RMS_VAR / 'module_healthiness.parquet',
                                 columns=['product_module_code', 'pct_coverage'])

# Assign healthiness quartile to each module
rms['ssnp_wt'] = rms['ssnp'] * rms['total_spending']
mod_base = (rms.groupby('product_module_code', as_index=False)
            .agg(total_spending_all=('total_spending', 'sum')))
mod_base = (mod_base
            .merge(claude_hi, on='product_module_code', how='inner')
            .merge(module_health, on='product_module_code', how='inner'))
mod_base = mod_base[mod_base['pct_coverage'] >= 0.01].copy()
lo, hi_val = mod_base['claude_hi'].quantile([0.01, 0.99])
w = mod_base['claude_hi'].clip(lo, hi_val)
mod_base['claude_hi_z'] = (w - w.mean()) / w.std()
mod_base['hi_quartile'] = pd.qcut(
    mod_base['claude_hi_z'], 4,
    labels=['Q1\n(least\nhealthy)', 'Q2', 'Q3', 'Q4\n(most\nhealthy)']
)
valid_modules = set(mod_base['product_module_code'])

# Merge RMS with county income quintile + healthiness quartile
rms_inc = (rms[rms['product_module_code'].isin(valid_modules)]
           .merge(county_income[['fips', 'county_inc_bin']], on='fips', how='inner')
           .merge(mod_base[['product_module_code', 'hi_quartile']], on='product_module_code', how='inner'))

# Collapse to healthiness quartile × income quintile (spending-weighted)
cell = (rms_inc.groupby(['hi_quartile', 'county_inc_bin'], as_index=False, observed=True)
        .agg(total_spending=('total_spending', 'sum'),
             ssnp_num=('ssnp_wt', 'sum')))
cell['ssnp'] = cell['ssnp_num'] / cell['total_spending']

# ============================================================
# FIGURE: SSNP by healthiness quartile × income quintile
# ============================================================
print("Creating figure (SSNP by healthiness × income)...")

q_labels = {1: 'Q1 (Lowest income)', 2: 'Q2', 3: 'Q3', 4: 'Q4 (Highest income)'}
inc_colors = plt.cm.RdYlGn(np.linspace(0.15, 0.85, N_INCOME_BINS))
hi_xticks  = ['Q1\n(least\nhealthy)', 'Q2', 'Q3', 'Q4\n(most\nhealthy)']
xs = list(range(4))

fig, ax = plt.subplots(figsize=(7, 5))
ax.grid(axis='y', color='#e5e5e5', linewidth=1.0, zorder=0)
ax.set_axisbelow(True)
for spine in ['top', 'right']:
    ax.spines[spine].set_visible(False)
for spine in ['left', 'bottom']:
    ax.spines[spine].set_color('#cccccc')

for q in bin_labels:
    d = cell[cell['county_inc_bin'] == q].copy()
    d = d.set_index('hi_quartile').reindex(hi_xticks)
    lw = 2.0 if q in (1, 5) else 1.2
    ls = '-' if q in (1, 5) else '--'
    ax.plot(xs, d['ssnp'].values, color=inc_colors[q - 1], linewidth=lw,
            linestyle=ls, marker='o', markersize=7, label=q_labels[q],
            markeredgewidth=0, zorder=3)

ax.set_xticks(xs)
ax.set_xticklabels(hi_xticks, fontsize=10)
ax.set_xlabel(r'$\bf{Nutrition}$ of product category', fontsize=11, labelpad=8)
ax.set_ylabel(r'$\bf{Expenditure\ share}$ on new products', fontsize=11, labelpad=8)
ax.tick_params(axis='both', length=0)
ax.legend(title='County income quartile', fontsize=9, title_fontsize=9,
          framealpha=0.9, loc='upper right')
plt.tight_layout()
plt.savefig(FIG_DIR / 'ssnp_by_healthiness_income.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved ssnp_by_healthiness_income.png")

# ============================================================
# PRINT SUMMARY
# ============================================================
print("\nSSNP by healthiness quartile × income quintile:")
print(cell.pivot(index='hi_quartile', columns='county_inc_bin', values='ssnp').to_string())

print("\nDone.")
