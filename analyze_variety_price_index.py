"""
analyze_variety_price_index.py

Uses the price index computed on OSCAR (build_price_index.py) to plot:

  Fig 1: Price level by healthiness quartile over time
         Two sets of 4 lines: unadjusted CES and variety-adjusted (VA)

  Fig 2: P_healthy / P_unhealthy (Q4 / Q1) over time
         Two lines: unadjusted CES vs Feenstra variety-adjusted
         Shows how much the relative price increase of healthy food is
         understated when ignoring innovation.

  Fig 3: Price level by healthiness quartile x county income quartile
         (variety-adjusted)
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
# LOAD PRICE INDEX + HEALTHINESS
# ============================================================
print("Loading price index data...")
pi = pd.read_parquet(RMS_VAR / 'price_index_module_year.parquet')
print(f"  {len(pi):,} module-year rows, years {pi['year'].min()}–{pi['year'].max()}")

claude_hi     = pd.read_parquet(RMS_VAR / 'claude_hi_scores.parquet',
                                 columns=['product_module_code', 'claude_hi'])
module_health = pd.read_parquet(RMS_VAR / 'module_healthiness.parquet',
                                 columns=['product_module_code', 'pct_coverage'])

# Merge healthiness
pi = pi.merge(claude_hi, on='product_module_code', how='inner')
pi = pi.merge(module_health, on='product_module_code', how='inner')
pi = pi[pi['pct_coverage'] >= 0.01].copy()

# Assign healthiness quartiles (based on overall module distribution)
mod_info = pi.drop_duplicates('product_module_code')[['product_module_code', 'claude_hi']].copy()
lo, hi_val = mod_info['claude_hi'].quantile([0.01, 0.99])
w = mod_info['claude_hi'].clip(lo, hi_val)
mod_info['claude_hi_z'] = (w - w.mean()) / w.std()
mod_info['hi_quartile'] = pd.qcut(
    mod_info['claude_hi_z'], 4,
    labels=['Q1\n(least healthy)', 'Q2', 'Q3', 'Q4\n(most healthy)']
)
pi = pi.merge(mod_info[['product_module_code', 'hi_quartile']], on='product_module_code')

print(f"  Modules after coverage filter: {pi['product_module_code'].nunique()}")

# ============================================================
# COLLAPSE TO HEALTHINESS QUARTILE x YEAR (spending-weighted)
# ============================================================
hi_labels      = ['Q1\n(least healthy)', 'Q2', 'Q3', 'Q4\n(most healthy)']
hi_colors      = ['#d73027', '#fc8d59', '#a1d99b', '#2ca25f']
hi_colors_dark = ['#b2182b', '#d6604d', '#4dac26', '#1a7837']
hi_colors_vivid = ['#c0392b', '#e67e22', '#27ae60', '#1a5e33']  # for x-axis tick labels
inc_colors     = plt.cm.Blues(np.linspace(0.35, 0.85, N_INCOME_BINS))  # income: light→dark blue

def style_ax(ax):
    """Apply standard figure style."""
    ax.grid(axis='y', color='#e5e5e5', linewidth=1.0, zorder=0)
    ax.set_axisbelow(True)
    for spine in ['top', 'right']:
        ax.spines[spine].set_visible(False)
    for spine in ['left', 'bottom']:
        ax.spines[spine].set_color('#cccccc')
    ax.tick_params(axis='both', length=0)

def color_hi_xticks(ax, labels, colors=hi_colors_vivid, fontsize=12):
    """Color x-axis tick labels by healthiness quartile."""
    ax.set_xticks(list(range(len(labels))))
    ax.set_xticklabels(labels, fontsize=fontsize, fontweight='bold')
    for tick, c in zip(ax.get_xticklabels(), colors):
        tick.set_color(c)

def wavg_by_group(df, group_cols, val_cols, wt_col):
    rows = []
    for keys, g in df.groupby(group_cols, observed=True):
        row = dict(zip(group_cols, keys if isinstance(keys, tuple) else [keys]))
        for v in val_cols:
            row[v] = np.average(g[v], weights=g[wt_col])
        rows.append(row)
    return pd.DataFrame(rows)

grp = wavg_by_group(pi, ['hi_quartile', 'year'],
                    ['level_ces', 'level_va'], 'total_spending')
grp_spend = pi.groupby(['hi_quartile', 'year'], observed=True)['total_spending'].sum().reset_index()
grp2 = grp.merge(grp_spend, on=['hi_quartile', 'year'])

# ============================================================
# FIGURE 1: Price level by healthiness quartile over time
# Shows both unadjusted (CES) and variety-adjusted lines
# ============================================================
print("Creating Figure 1 (price levels by healthiness quartile)...")

fig, ax = plt.subplots(figsize=(9, 5.5))
ax.grid(axis='y', color='#e5e5e5', linewidth=1.0, zorder=0)
ax.set_axisbelow(True)
for spine in ['top', 'right']:
    ax.spines[spine].set_visible(False)
for spine in ['left', 'bottom']:
    ax.spines[spine].set_color('#cccccc')

for q, c in zip(hi_labels, hi_colors):
    d = grp[grp['hi_quartile'] == q].sort_values('year')
    label_ces = q.replace('\n', ' ') + ' (unadj.)'
    label_va  = q.replace('\n', ' ') + ' (adj.)'
    ax.plot(d['year'], d['level_ces'], color=c, linewidth=1.5,
            linestyle='--', marker='o', markersize=4, markeredgewidth=0,
            label=label_ces, alpha=0.7)
    ax.plot(d['year'], d['level_va'], color=c, linewidth=2.2,
            linestyle='-', marker='o', markersize=4, markeredgewidth=0,
            label=label_va)

ax.axhline(1, color='#aaaaaa', linewidth=0.8, linestyle=':')
ax.set_xlabel('Year', fontsize=11, labelpad=8)
ax.set_ylabel(r'Price level (base = 1 at 2008)', fontsize=11, labelpad=8)
ax.legend(fontsize=7.5, framealpha=0.9, ncol=2,
          title='Solid = variety-adjusted  |  Dashed = unadjusted CES',
          title_fontsize=8)
ax.tick_params(axis='both', length=0)
plt.tight_layout()
plt.savefig(FIG_DIR / 'variety_price_level_by_healthiness.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved variety_price_level_by_healthiness.png")

# ============================================================
# FIGURE 2: P_healthy / P_unhealthy ratio — adjusted vs unadjusted
# ============================================================
print("Creating Figure 2 ((Q3+Q4)/(Q1+Q2) ratio)...")

years = sorted(grp['year'].unique())
unhealthy_qs = ['Q1\n(least healthy)', 'Q2']
healthy_qs   = ['Q3', 'Q4\n(most healthy)']

def ratio_by_year(col):
    ratios = []
    for yr in years:
        g = grp2[grp2['year'] == yr]
        p_h = np.average(g.loc[g['hi_quartile'].isin(healthy_qs),   col],
                         weights=g.loc[g['hi_quartile'].isin(healthy_qs),   'total_spending'])
        p_u = np.average(g.loc[g['hi_quartile'].isin(unhealthy_qs), col],
                         weights=g.loc[g['hi_quartile'].isin(unhealthy_qs), 'total_spending'])
        ratios.append(p_h / p_u)
    return ratios

ratio_ces = ratio_by_year('level_ces')
ratio_va  = ratio_by_year('level_va')

# Drop 2008 (commodity-crisis outlier)
start_idx = years.index(2009)
years_plot    = years[start_idx:]
ratio_ces_plot = ratio_ces[start_idx:]
ratio_va_plot  = ratio_va[start_idx:]

fig, ax = plt.subplots(figsize=(8, 5))
ax.grid(axis='y', color='#e5e5e5', linewidth=1.0, zorder=0)
ax.set_axisbelow(True)
for spine in ['top', 'right']:
    ax.spines[spine].set_visible(False)
for spine in ['left', 'bottom']:
    ax.spines[spine].set_color('#cccccc')

ax.plot(years_plot, ratio_ces_plot, color='#999999', linewidth=2.0, linestyle='--',
        marker='o', markersize=5, markeredgewidth=0,
        label='Unadjusted (CES, continuing products only)')
ax.plot(years_plot, ratio_va_plot, color='#2c5f8a', linewidth=2.2, linestyle='-',
        marker='o', markersize=5, markeredgewidth=0,
        label='Feenstra variety-adjusted')
ax.fill_between(years_plot, ratio_ces_plot, ratio_va_plot,
                alpha=0.12, color='#2c5f8a', label='_nolegend_')

ax.axhline(1, color='#444444', linewidth=1.2, linestyle='--', zorder=1)
ax.set_xlabel('Year', fontsize=11, labelpad=8)
ax.set_ylabel(r'$P_{\mathrm{healthy}}\ /\ P_{\mathrm{unhealthy}}$' +
              '\n(Q3+Q4 / Q1+Q2, spending-weighted, base = 1 at 2008)', fontsize=11, labelpad=8)
ax.legend(fontsize=10, framealpha=0.9, loc='upper left')
ax.tick_params(axis='both', length=0)
plt.tight_layout()
plt.savefig(FIG_DIR / 'variety_price_ratio_adjusted_vs_not.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved variety_price_ratio_adjusted_vs_not.png")

# ============================================================
# FIGURE 3: Variety-adjusted price level by healthiness x income quartile
# ============================================================
print("Creating Figure 3 (price level by healthiness x income)...")

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
                 .merge(hhy, on=['household_code', 'year'], how='inner')
                 .dropna(subset=['real_income', 'projection_factor']))
county_income = (county_income.groupby('fips')
                 .apply(lambda g: np.average(g['real_income'], weights=g['projection_factor']))
                 .rename('avg_income').reset_index())
county_income['county_inc_bin'] = pd.qcut(
    county_income['avg_income'].rank(method='first'),
    q=N_INCOME_BINS, labels=bin_labels
).astype(int)

# Join FIPS-level price index with county income
rms_fips = pd.read_parquet(RMS_VAR / 'rms_variety_module_fips_year.parquet',
                            columns=['product_module_code', 'fips', 'year', 'total_spending'])
rms_fips = rms_fips[(rms_fips['year'] >= 2008) & (rms_fips['year'] <= 2020)]

# Use module-year price levels (both CES and VA), join to fips for income assignment
pi_mod = pi[['product_module_code', 'year', 'level_ces', 'level_va', 'hi_quartile']].copy()
rms_fips = (rms_fips
            .merge(pi_mod, on=['product_module_code', 'year'], how='inner')
            .merge(county_income[['fips', 'county_inc_bin']], on='fips', how='inner'))

grp3 = wavg_by_group(rms_fips, ['hi_quartile', 'county_inc_bin'],
                     ['level_ces', 'level_va'], 'total_spending')

# Variety-adjusted price level by healthiness x income (4 income lines)
xs = list(range(4))
inc_labels = {1: 'Q1 (Lowest income)', 2: 'Q2', 3: 'Q3', 4: 'Q4 (Highest income)'}

fig, ax = plt.subplots(figsize=(7, 5))
style_ax(ax)

for q in bin_labels:
    d = grp3[grp3['county_inc_bin'] == q].set_index('hi_quartile').reindex(hi_labels)
    lw = 2.2 if q in (1, N_INCOME_BINS) else 1.3
    ls = '-' if q in (1, N_INCOME_BINS) else '--'
    ax.plot(xs, d['level_va'].values, color=inc_colors[q - 1], linewidth=lw,
            linestyle=ls, marker='o', markersize=7, markeredgewidth=0,
            label=inc_labels[q], zorder=3)

color_hi_xticks(ax, hi_labels)
ax.set_xlabel(r'$\bf{Nutrition}$ of product category', fontsize=11, labelpad=8)
ax.set_ylabel('Variety-adjusted price level\n(averaged 2008–2020)', fontsize=11, labelpad=8)
ax.legend(title='County income quartile', fontsize=9, title_fontsize=9,
          framealpha=0.9, loc='lower right')
plt.tight_layout()
plt.savefig(FIG_DIR / 'variety_price_level_by_healthiness_income.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved variety_price_level_by_healthiness_income.png")

# Rich-poor gap: price level for top income quartile minus bottom, by healthiness

fig, ax = plt.subplots(figsize=(7, 5))
ax.grid(axis='y', color='#e5e5e5', linewidth=1.0, zorder=0)
ax.set_axisbelow(True)
for spine in ['top', 'right']:
    ax.spines[spine].set_visible(False)
for spine in ['left', 'bottom']:
    ax.spines[spine].set_color('#cccccc')

for col, label, color, ls in [
    ('level_ces', 'Unadjusted (CES)', '#999999', '--'),
    ('level_va',  'Feenstra variety-adjusted', '#2c5f8a', '-'),
]:
    rich = grp3[grp3['county_inc_bin'] == N_INCOME_BINS].set_index('hi_quartile').reindex(hi_labels)
    poor = grp3[grp3['county_inc_bin'] == 1          ].set_index('hi_quartile').reindex(hi_labels)
    gap  = rich[col].values - poor[col].values
    ax.plot(xs, gap, color=color, linewidth=2.2, linestyle=ls,
            marker='o', markersize=7, markeredgewidth=0, label=label, zorder=3)

ax.axhline(0, color='#cccccc', linewidth=0.8, linestyle=':')
color_hi_xticks(ax, hi_labels)
ax.set_xlabel(r'$\bf{Nutrition}$ of product category', fontsize=11, labelpad=8)
ax.set_ylabel('Rich–poor price gap\n(Q4 minus Q1 county income, averaged 2008–2020)', fontsize=11, labelpad=8)
ax.legend(fontsize=10, framealpha=0.9, loc='upper right')
plt.tight_layout()
plt.savefig(FIG_DIR / 'variety_price_gap_income_by_healthiness.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved variety_price_gap_income_by_healthiness.png")

# ============================================================
# FIGURE 3b: CES vs VA price level by healthiness (no income split)
# Shows that CES slopes DOWN and VA slopes UP — opposite directions
# ============================================================
print("Creating Figure 3b (CES vs VA price level by healthiness)...")

# Spending-weighted average across all years and FIPS
grp3b = wavg_by_group(rms_fips, ['hi_quartile'], ['level_ces', 'level_va'], 'total_spending')
grp3b = grp3b.set_index('hi_quartile').reindex(hi_labels)

ces_vals = grp3b['level_ces'].values
va_vals  = grp3b['level_va'].values

def make_ces_va_fig(show_va):
    fig, ax1 = plt.subplots(figsize=(7, 5))
    ax2 = ax1.twinx()

    style_ax(ax1)
    ax2.spines['top'].set_visible(False)
    ax2.spines['left'].set_visible(False)
    ax2.spines['bottom'].set_visible(False)
    ax2.spines['right'].set_color('#cccccc')
    ax2.tick_params(axis='both', length=0)

    ax1.plot(xs, ces_vals, color='#666666', linewidth=2.4, linestyle='--',
             marker='o', markersize=6, markeredgewidth=0, zorder=3,
             label='Unadjusted price')
    if show_va:
        ax2.plot(xs, va_vals, color='#2c5f8a', linewidth=2.4, linestyle='-',
                 marker='o', markersize=6, markeredgewidth=0, zorder=3,
                 label='Variety-adjusted')

    ax1.set_ylim(1.08, 1.13)
    ax2.set_ylim(1.01, 1.06)

    color_hi_xticks(ax1, hi_labels)
    ax1.set_xlabel(r'$\bf{Nutrition}$ of product category', fontsize=11, labelpad=8)
    ax1.set_ylabel('CES price level (averaged 2008–2020)', fontsize=11, labelpad=8)
    ax2.set_ylabel('Variety-adjusted price level (averaged 2008–2020)', fontsize=11, labelpad=8)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=10, framealpha=0.9, loc='upper right')

    plt.tight_layout()
    return fig

fig = make_ces_va_fig(show_va=False)
plt.savefig(FIG_DIR / 'variety_price_level_ces_vs_va_step1.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved variety_price_level_ces_vs_va_step1.png")

fig = make_ces_va_fig(show_va=True)
plt.savefig(FIG_DIR / 'variety_price_level_ces_vs_va_by_healthiness.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved variety_price_level_ces_vs_va_by_healthiness.png")

# ============================================================
# SUMMARY
# ============================================================
print("\nCumulative price levels by healthiness quartile (spending-weighted, 2020):")
last = grp[grp['year'] == 2020]
for q in hi_labels:
    row = last[last['hi_quartile'] == q].iloc[0]
    print(f"  {q.replace(chr(10),' '):25s}  CES={row['level_ces']:.3f}  VA={row['level_va']:.3f}")

print(f"\nP_(Q3+Q4)/P_(Q1+Q2) ratio by 2020:")
print(f"  Unadjusted CES: {ratio_ces[-1]:.4f}")
print(f"  Variety-adjusted: {ratio_va[-1]:.4f}")
print(f"  Gap (variety adjustment adds): {ratio_va[-1] - ratio_ces[-1]:.4f}")

# (Q3+Q4) / (Q1+Q2) ratio over time — spending-weighted collapse
print("\n(Q3+Q4) / (Q1+Q2) price ratio by year:")
print(f"  {'Year':>4}   CES     VA")
unhealthy = ['Q1\n(least healthy)', 'Q2']
healthy   = ['Q3', 'Q4\n(most healthy)']
all_years = sorted(grp2['year'].unique())
for yr in all_years:
    g = grp2[grp2['year'] == yr]
    def wavg_level(qs, col):
        sub = g[g['hi_quartile'].isin(qs)]
        return np.average(sub[col], weights=sub['total_spending'])
    r_ces = wavg_level(healthy, 'level_ces') / wavg_level(unhealthy, 'level_ces')
    r_va  = wavg_level(healthy, 'level_va')  / wavg_level(unhealthy, 'level_va')
    print(f"  {yr:>4}   {r_ces:.4f}  {r_va:.4f}")

print("\nDone.")

# ============================================================
# FIGURE 4: Rich–poor HH price gap by healthiness quartile
# Uses actual Nielsen HH purchase weights (not county RMS weights)
# ============================================================
print("\nCreating Figure 4 (HH-level rich–poor price gap by healthiness)...")

PURCHASES_DIR = BASE / 'interim' / 'purchases_food'

# Aggregate purchases to HH x module x year (2008-2020)
print("  Loading and aggregating purchases...")
chunks = []
for yr in range(2008, 2021):
    ydir = PURCHASES_DIR / f'panel_year={yr}'
    if not ydir.exists():
        continue
    df = pd.read_parquet(ydir, columns=['household_code', 'product_module_code', 'total_price_paid'])
    df = df[df['total_price_paid'] > 0]
    agg = df.groupby(['household_code', 'product_module_code'], as_index=False)['total_price_paid'].sum()
    agg['year'] = yr
    chunks.append(agg)
    print(f"    {yr}: {len(agg):,} HH-module cells")

hh_mod = pd.concat(chunks, ignore_index=True)
del chunks

# Merge price levels + healthiness quartile (module x year)
pi_hh = pi[['product_module_code', 'year', 'level_ces', 'level_va', 'hi_quartile']].copy()
hh_mod = hh_mod.merge(pi_hh, on=['product_module_code', 'year'], how='inner')

# Merge HH income
hhy4 = pd.read_parquet(BASE / 'interim/panel_dataset/panel_hh_year.parquet',
                       columns=['household_code', 'panel_year', 'real_income'])
hhy4 = hhy4.rename(columns={'panel_year': 'year'})
hh_mod = hh_mod.merge(hhy4, on=['household_code', 'year'], how='inner')
hh_mod = hh_mod.dropna(subset=['real_income'])

# Assign within-year income quartiles using rank-based qcut
hh_mod['inc_bin'] = np.nan
for yr in sorted(hh_mod['year'].unique()):
    mask = hh_mod['year'] == yr
    hh_mod.loc[mask, 'inc_bin'] = pd.qcut(
        hh_mod.loc[mask, 'real_income'].rank(method='first'),
        q=N_INCOME_BINS, labels=bin_labels
    ).astype(float)

# Collapse to (inc_bin, hi_quartile): spending-weighted avg price level
grp4 = wavg_by_group(hh_mod, ['inc_bin', 'hi_quartile'],
                     ['level_ces', 'level_va'], 'total_price_paid')
grp4_spend = (hh_mod.groupby(['inc_bin', 'hi_quartile'], observed=True)['total_price_paid']
              .sum().reset_index())
grp4 = grp4.merge(grp4_spend, on=['inc_bin', 'hi_quartile'])

# Plot rich–poor gap (Q4 - Q1 income) by healthiness quartile
fig, ax = plt.subplots(figsize=(7, 5))
ax.grid(axis='y', color='#e5e5e5', linewidth=1.0, zorder=0)
ax.set_axisbelow(True)
for spine in ['top', 'right']:
    ax.spines[spine].set_visible(False)
for spine in ['left', 'bottom']:
    ax.spines[spine].set_color('#cccccc')

rich_bins = [3, 4]
poor_bins  = [1, 2]

for col, label, color, ls in [
    ('level_ces', 'Unadjusted (CES)', '#999999', '--'),
    ('level_va',  'Feenstra variety-adjusted', '#2c5f8a', '-'),
]:
    gap = []
    for q in hi_labels:
        sub = grp4[grp4['hi_quartile'] == q]
        p_rich = np.average(sub.loc[sub['inc_bin'].isin(rich_bins), col],
                            weights=sub.loc[sub['inc_bin'].isin(rich_bins), 'total_price_paid'])
        p_poor = np.average(sub.loc[sub['inc_bin'].isin(poor_bins), col],
                            weights=sub.loc[sub['inc_bin'].isin(poor_bins), 'total_price_paid'])
        gap.append(p_rich - p_poor)
    ax.plot(xs, gap, color=color, linewidth=2.2, linestyle=ls,
            marker='o', markersize=7, markeredgewidth=0, label=label, zorder=3)

ax.axhline(0, color='#cccccc', linewidth=0.8, linestyle=':')
color_hi_xticks(ax, hi_labels)
ax.set_xlabel(r'$\bf{Nutrition}$ of product category', fontsize=11, labelpad=8)
ax.set_ylabel('Rich–poor price gap\n(Q3+Q4 minus Q1+Q2 HH income, averaged 2008–2020)', fontsize=11, labelpad=8)
ax.legend(fontsize=10, framealpha=0.9, loc='upper right')
plt.tight_layout()
plt.savefig(FIG_DIR / 'variety_price_gap_hh_income_by_healthiness.png', bbox_inches='tight', dpi=150)
plt.close()
print("  Saved variety_price_gap_hh_income_by_healthiness.png")
