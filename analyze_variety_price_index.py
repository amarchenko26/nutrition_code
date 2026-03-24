"""
analyze_variety_price_index.py

Computes the Feenstra (1994) variety correction to the price index,
following Jaravel (2019).

The full variety-adjusted price index is:
    pi^VA_{t,t+1} = pi^CES_{t,t+1} * (lambda_t / lambda_{t-1})^{1/(sigma-1)}

where pi^CES is the Sato-Vartia price index for *continuing* products
(requires UPC-level prices — only available on OSCAR), and the lambda ratio
is the Feenstra variety correction:

    lambda_t     = 1 - SSNP_t    (share of t spending on continuing products)
    lambda_{t-1} = 1 - SSEP_{t-1} (share of t-1 spending on continuing products)

We compute only the variety correction component (pi^CES = 1).
This gives the welfare-equivalent price reduction from new product entry,
holding continuing-product prices fixed. sigma = 5 (common assumption).

Figures:
  1. Cumulative variety-adjusted price level by healthiness quartile
  2. Same, broken out by county income quartile x healthiness quartile
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
RMS_VAR = BASE / 'interim' / 'rms_variety'
FIG_DIR = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs')
FIG_DIR.mkdir(parents=True, exist_ok=True)

SIGMA = 5  # elasticity of substitution; 1/(sigma-1) = 0.25
N_INCOME_BINS = 4
bin_labels = list(range(1, N_INCOME_BINS + 1))

# ============================================================
# LOAD RMS VARIETY DATA
# ============================================================
print("Loading RMS variety data...")
rms = pd.read_parquet(RMS_VAR / 'rms_variety_module_fips_year.parquet',
                      columns=['product_module_code', 'fips', 'year',
                               'ssnp', 'ssep', 'total_spending'])
rms = rms[(rms['year'] >= 2007) & (rms['year'] <= 2020)].dropna(subset=['ssnp', 'ssep'])
print(f"  {len(rms):,} module-FIPS-year obs")

# ============================================================
# FEENSTRA VARIETY CORRECTION
# lambda_t   = 1 - SSNP_t     (continuing share in t)
# lambda_t-1 = 1 - SSEP_{t-1} (continuing share in t-1, from exit side)
# variety_correction = (lambda_t / lambda_{t-1})^{1/(sigma-1)}
# ============================================================
rms = rms.sort_values(['product_module_code', 'fips', 'year'])

# Shift SSEP to get ssep_{t-1} aligned with ssnp_t
rms['ssep_lag'] = (rms
                   .groupby(['product_module_code', 'fips'])['ssep']
                   .shift(1))

rms = rms.dropna(subset=['ssep_lag'])  # drops first observed year per module-FIPS
rms['lambda_t']   = (1 - rms['ssnp']).clip(0.01, 1)
rms['lambda_tm1'] = (1 - rms['ssep_lag']).clip(0.01, 1)
rms['variety_correction'] = (rms['lambda_t'] / rms['lambda_tm1']) ** (1 / (SIGMA - 1))

# ============================================================
# CUMULATE: variety-adjusted price level (base = 1 at first year)
# P_{m,c,T} = prod_{t=2008}^{T} variety_correction_{m,c,t}
# ============================================================
rms['log_vc'] = np.log(rms['variety_correction'])
rms['cum_log_vc'] = rms.groupby(['product_module_code', 'fips'])['log_vc'].cumsum()
rms['price_level'] = np.exp(rms['cum_log_vc'])  # 1 = no change from base year

print(f"  Variety correction range: {rms['variety_correction'].min():.4f} – {rms['variety_correction'].max():.4f}")
print(f"  Mean annual correction: {rms['variety_correction'].mean():.4f}")

# ============================================================
# LOAD CLAUDE HI + COVERAGE, ASSIGN HEALTHINESS QUARTILE
# ============================================================
claude_hi     = pd.read_parquet(RMS_VAR / 'claude_hi_scores.parquet',
                                 columns=['product_module_code', 'claude_hi'])
module_health = pd.read_parquet(RMS_VAR / 'module_healthiness.parquet',
                                 columns=['product_module_code', 'pct_coverage'])

mod_info = (rms.groupby('product_module_code', as_index=False)['total_spending'].sum()
            .merge(claude_hi, on='product_module_code')
            .merge(module_health, on='product_module_code'))
mod_info = mod_info[mod_info['pct_coverage'] >= 0.01]

lo, hi_val = mod_info['claude_hi'].quantile([0.01, 0.99])
w = mod_info['claude_hi'].clip(lo, hi_val)
mod_info['claude_hi_z'] = (w - w.mean()) / w.std()
mod_info['hi_quartile'] = pd.qcut(
    mod_info['claude_hi_z'], 4,
    labels=['Q1\n(least healthy)', 'Q2', 'Q3', 'Q4\n(most healthy)']
)
valid_modules = set(mod_info['product_module_code'])

rms_hi = rms[rms['product_module_code'].isin(valid_modules)].merge(
    mod_info[['product_module_code', 'hi_quartile']], on='product_module_code', how='inner')

# ============================================================
# FIGURE 1: Cumulative price level by healthiness quartile
# Collapse: spending-weighted mean price_level per quartile × year
# ============================================================
print("Creating Figure 1 (price level by healthiness quartile over time)...")

grp1 = (rms_hi.groupby(['hi_quartile', 'year'], observed=True)
        .apply(lambda g: np.average(g['price_level'], weights=g['total_spending']))
        .rename('price_level').reset_index())

hi_labels = ['Q1\n(least healthy)', 'Q2', 'Q3', 'Q4\n(most healthy)']
hi_colors = ['#d73027', '#fc8d59', '#a1d99b', '#2ca25f']

fig, ax = plt.subplots(figsize=(8, 5))
ax.grid(axis='y', color='#e5e5e5', linewidth=1.0, zorder=0)
ax.set_axisbelow(True)
for spine in ['top', 'right']:
    ax.spines[spine].set_visible(False)
for spine in ['left', 'bottom']:
    ax.spines[spine].set_color('#cccccc')

for q, c in zip(hi_labels, hi_colors):
    d = grp1[grp1['hi_quartile'] == q].sort_values('year')
    ax.plot(d['year'], d['price_level'], color=c, linewidth=2.2,
            marker='o', markersize=5, label=q.replace('\n', ' '), markeredgewidth=0)

ax.axhline(1, color='#aaaaaa', linewidth=0.8, linestyle='--')
ax.set_xlabel('Year', fontsize=11, labelpad=8)
ax.set_ylabel(r'$\bf{Variety{-}adjusted}$ price level' + '\n(base = 1 at 2008)', fontsize=11, labelpad=8)
ax.legend(title='Nutrition of category', fontsize=9, title_fontsize=9,
          framealpha=0.9, loc='lower left')
ax.tick_params(axis='both', length=0)
plt.tight_layout()
plt.savefig(FIG_DIR / 'variety_price_level_by_healthiness.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved variety_price_level_by_healthiness.png")

# ============================================================
# FIGURE 2: Price level by healthiness quartile × income quartile
# Use same county-income-quartile assignment as analyze_innovation_inequality.py
# ============================================================
print("Creating Figure 2 (price level by healthiness × income)...")

pan = pd.read_parquet(BASE / 'interim/panelists/panelists_all_years.parquet',
                      columns=['household_code', 'panel_year',
                               'fips_state_code', 'fips_county_code', 'projection_factor'])
pan = pan.rename(columns={'panel_year': 'year'})
pan['fips'] = (pan['fips_state_code'].astype(str).str.extract(r'(\d+)')[0].str.zfill(2) +
               pan['fips_county_code'].astype(str).str.extract(r'(\d+)')[0].str.zfill(3))

hhy = pd.read_parquet(BASE / 'interim/panel_dataset/panel_hh_year.parquet',
                      columns=['household_code', 'panel_year', 'real_income'])
hhy = hhy.rename(columns={'panel_year': 'year'})

county_income = (pan[pan['year'] <= 2020]
                 .merge(hhy[['household_code', 'year', 'real_income']],
                        on=['household_code', 'year'], how='inner')
                 .dropna(subset=['real_income', 'projection_factor']))
county_income = (county_income.groupby('fips')
                 .apply(lambda g: np.average(g['real_income'], weights=g['projection_factor']))
                 .rename('avg_income').reset_index())
county_income['county_inc_bin'] = pd.qcut(
    county_income['avg_income'].rank(method='first'),
    q=N_INCOME_BINS, labels=bin_labels
).astype(int)

# Collapse price_level to healthiness quartile × income quartile (spending-weighted, averaged over years)
rms_inc = (rms_hi
           .merge(county_income[['fips', 'county_inc_bin']], on='fips', how='inner'))

grp2 = (rms_inc.groupby(['hi_quartile', 'county_inc_bin'], observed=True)
        .apply(lambda g: np.average(g['price_level'], weights=g['total_spending']))
        .rename('price_level').reset_index())

inc_colors = plt.cm.RdYlGn(np.linspace(0.15, 0.85, N_INCOME_BINS))
inc_labels = {1: 'Q1 (Lowest income)', 2: 'Q2', 3: 'Q3', 4: 'Q4 (Highest income)'}
xs = list(range(4))

fig, ax = plt.subplots(figsize=(7, 5))
ax.grid(axis='y', color='#e5e5e5', linewidth=1.0, zorder=0)
ax.set_axisbelow(True)
for spine in ['top', 'right']:
    ax.spines[spine].set_visible(False)
for spine in ['left', 'bottom']:
    ax.spines[spine].set_color('#cccccc')

for q in bin_labels:
    d = grp2[grp2['county_inc_bin'] == q].copy()
    d = d.set_index('hi_quartile').reindex(hi_labels)
    lw = 2.2 if q in (1, N_INCOME_BINS) else 1.3
    ls = '-' if q in (1, N_INCOME_BINS) else '--'
    ax.plot(xs, d['price_level'].values, color=inc_colors[q - 1], linewidth=lw,
            linestyle=ls, marker='o', markersize=7, label=inc_labels[q],
            markeredgewidth=0, zorder=3)

ax.axhline(1, color='#aaaaaa', linewidth=0.8, linestyle=':', zorder=1)
ax.set_xticks(xs)
ax.set_xticklabels([l.replace('\n', '\n') for l in hi_labels], fontsize=10)
ax.set_xlabel(r'$\bf{Nutrition}$ of product category', fontsize=11, labelpad=8)
ax.set_ylabel(r'$\bf{Variety{-}adjusted}$ price level' + '\n(base = 1 at 2008, averaged 2008–2020)', fontsize=11, labelpad=8)
ax.tick_params(axis='both', length=0)
ax.legend(title='County income quartile', fontsize=9, title_fontsize=9,
          framealpha=0.9, loc='upper right')
plt.tight_layout()
plt.savefig(FIG_DIR / 'variety_price_level_by_healthiness_income.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved variety_price_level_by_healthiness_income.png")

# ============================================================
# SUMMARY
# ============================================================
print("\nMean price level by healthiness quartile (all years, spending-weighted):")
for q in hi_labels:
    sub = rms_hi[rms_hi['hi_quartile'] == q]
    pl = np.average(sub['price_level'], weights=sub['total_spending'])
    print(f"  {q.replace(chr(10), ' '):25s}: {pl:.4f}")

print("\nMean price level by healthiness × income quartile:")
print(grp2.pivot(index='hi_quartile', columns='county_inc_bin', values='price_level').to_string())

print("\nDone.")
