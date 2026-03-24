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

N_INCOME_BINS = 5
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
# FIGURE 1: SSNP by income quintile over time
# ============================================================
print("Creating Figure 1 (SSNP by income quintile over time)...")

all_years = sorted(hh_ssnp['year'].unique())
print(f"  Years covered: {all_years}")
q_colors  = plt.cm.RdYlGn(np.linspace(0.15, 0.85, N_INCOME_BINS))
q_labels  = {1: 'Q1 (Lowest)', 2: 'Q2', 3: 'Q3', 4: 'Q4', 5: 'Q5 (Highest)'}

rows = []
for year in all_years:
    for q in bin_labels:
        sub = hh_ssnp[(hh_ssnp['year'] == year) & (hh_ssnp['IncomeBin'] == q)]
        if len(sub) < 5:
            continue
        rows.append({
            'year': year, 'income_bin': int(q),
            'ssnp': np.average(sub['ssnp_county'], weights=sub['projection_factor'])
        })
res = pd.DataFrame(rows)

fig, ax = plt.subplots(figsize=(10, 5.5))
for q in bin_labels:
    d = res[res['income_bin'] == q].sort_values('year')
    lw = 2.5 if q in (1, 5) else 1.2
    ls = '-' if q in (1, 5) else '--'
    ax.plot(d['year'], d['ssnp'], color=q_colors[q - 1], linewidth=lw,
            linestyle=ls, marker='o', markersize=4 if q in (1, 5) else 2,
            label=q_labels[q], alpha=0.9)

ax.set_xlabel('Year', fontsize=12)
ax.set_ylabel('Expenditure share on new products (SSNP)', fontsize=12)
ax.set_title('Expenditure Share on New Products by Income Quintile Over Time',
             fontsize=13, fontweight='bold')
ax.legend(title='Income quintile', fontsize=10, title_fontsize=10,
          loc='best', framealpha=0.9)
ax.grid(True, alpha=0.3, axis='y')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.tight_layout()
plt.savefig(FIG_DIR / 'ssnp_by_income_over_time.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved ssnp_by_income_over_time.png")

# ============================================================
# FIGURE 2: SSNP by healthiness quartile (Claude HI)
# Module-level, spending-weighted collapse
# ============================================================
print("Creating Figure 2 (SSNP by healthiness quartile)...")

claude_hi = pd.read_parquet(RMS_VAR / 'claude_hi_scores.parquet',
                             columns=['product_module_code', 'claude_hi'])
module_health = pd.read_parquet(RMS_VAR / 'module_healthiness.parquet',
                                 columns=['product_module_code', 'pct_coverage'])

# Collapse RMS to module level (spending-weighted across FIPS, averaged across years)
rms['ssnp_wt'] = rms['ssnp'] * rms['total_spending']
mod = (rms.groupby('product_module_code', as_index=False)
       .agg(total_spending=('total_spending', 'sum'),
            ssnp_num=('ssnp_wt', 'sum')))
mod['ssnp'] = mod['ssnp_num'] / mod['total_spending']

# Merge health
mod = mod.merge(claude_hi, on='product_module_code', how='inner')
mod = mod.merge(module_health, on='product_module_code', how='inner')
mod = mod[mod['pct_coverage'] >= 0.01].copy()

# Winsorize + z-score Claude HI
lo, hi_val = mod['claude_hi'].quantile([0.01, 0.99])
w = mod['claude_hi'].clip(lo, hi_val)
mod['claude_hi_z'] = (w - w.mean()) / w.std()

# Quartile dot plot
mask = mod['claude_hi_z'].notna() & mod['ssnp'].notna()
d = mod[mask].copy()
d['quartile'] = pd.qcut(d['claude_hi_z'], 4,
                         labels=['Q1\n(least\nhealthy)', 'Q2', 'Q3', 'Q4\n(most\nhealthy)'])
stats = d.groupby('quartile', observed=True)['ssnp'].agg(['mean', 'sem'])
ci    = 1.96 * stats['sem']
means = stats['mean'].values
xs    = list(range(4))
colors = ['#d73027', '#fc8d59', '#a1d99b', '#2ca25f']

fig, ax = plt.subplots(figsize=(6, 5))
ax.grid(axis='y', color='#e5e5e5', linewidth=1.0, zorder=0)
ax.set_axisbelow(True)
for spine in ['top', 'right']:
    ax.spines[spine].set_visible(False)
for spine in ['left', 'bottom']:
    ax.spines[spine].set_color('#cccccc')

ax.plot(xs, means, color='#999999', linewidth=1.2, zorder=1, linestyle='--')
for i, (m, e, c) in enumerate(zip(means, ci.values, colors)):
    ax.errorbar(i, m, yerr=e, fmt='o', color=c,
                markersize=10, capsize=5, linewidth=1.8, capthick=1.8,
                markeredgewidth=0, zorder=2)

ax.set_xticks(xs)
ax.set_xticklabels(stats.index, fontsize=10)
ax.set_xlabel(r'$\bf{Nutrition}$ of product category', fontsize=11, labelpad=8)
ax.set_ylabel(r'$\bf{Expenditure\ share}$ on new products', fontsize=11, labelpad=8)
ax.tick_params(axis='both', length=0)
pad = (means.max() - means.min()) * 0.8
ax.set_ylim(means.min() - ci.max() - pad, means.max() + ci.max() + pad)
plt.tight_layout()
plt.savefig(FIG_DIR / 'ssnp_by_healthiness_quartile.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved ssnp_by_healthiness_quartile.png")

# ============================================================
# PRINT SUMMARY
# ============================================================
print("\nSummary — mean SSNP by income quintile (all years pooled):")
for q in bin_labels:
    sub = hh_ssnp[hh_ssnp['IncomeBin'] == q]
    m = np.average(sub['ssnp_county'], weights=sub['projection_factor'])
    print(f"  Q{q}: {m:.4f}")

print("\nSummary — mean SSNP by healthiness quartile:")
print(stats[['mean']].to_string())

print("\nDone.")
