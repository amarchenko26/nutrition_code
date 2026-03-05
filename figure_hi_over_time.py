"""
Nutritional inequality over time: Health Index by income quintile.
Loads the pre-built panel dataset from build_hi_panel.py.

Saves two figures:
  hi_by_income_over_time.png  -- lines per year, x=income bin
  hi_inequality_over_time.png -- gap chart, x=year, vertical bars Q1-Q5
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def log(msg):
    print(msg, flush=True)

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
DATASET = BASE / 'interim' / 'panel_dataset' / 'panel_hh_year.parquet'
FIG_DIR = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs')
FIG_DIR.mkdir(parents=True, exist_ok=True)

PLOT_YEARS    = [2005, 2010, 2013, 2015, 2018, 2020]
N_INCOME_BINS = 5
CHANGE_YEARS  = (2005, 2020)

# ============================================================
# LOAD
# ============================================================
log("Loading dataset...")
hhy = pd.read_parquet(DATASET)
log(f"  {len(hhy):,} HH-year obs, {hhy['household_code'].nunique():,} HHs")

# ============================================================
# INCOME BINS (weighted quantile cuts across all unique HHs)
# ============================================================
all_hh = hhy.drop_duplicates('household_code')[
    ['household_code', 'hh_real_income_avg', 'projection_factor']].dropna()
all_hh = all_hh[all_hh['projection_factor'] > 0].sort_values('hh_real_income_avg').reset_index(drop=True)
cumwt = all_hh['projection_factor'].cumsum().to_numpy()
cumwt = cumwt / cumwt[-1]
cuts = np.interp(np.arange(1, N_INCOME_BINS) / N_INCOME_BINS, cumwt, all_hh['hh_real_income_avg'].to_numpy()).tolist()
for i in range(1, len(cuts)):
    if cuts[i] <= cuts[i - 1]:
        cuts[i] = np.nextafter(cuts[i - 1], np.inf)

bin_labels = list(range(1, N_INCOME_BINS + 1))
hhy['IncomeBin'] = pd.cut(
    hhy['hh_real_income_avg'], bins=[-np.inf] + cuts + [np.inf],
    labels=bin_labels, include_lowest=True).astype(float)

# ============================================================
# INCOME-BIN MEANS — ALL YEARS (for gap chart)
# ============================================================
log("Computing income-bin means by year (all years)...")
all_years = sorted(hhy['panel_year'].unique())
results_all = []
for year in all_years:
    for q in bin_labels:
        sub = hhy[(hhy['panel_year'] == year) & (hhy['IncomeBin'] == q)]
        if len(sub) > 0:
            results_all.append({
                'year': year, 'income_bin': int(q), 'n': len(sub),
                'hi_allcott': np.average(sub['hi_allcott'], weights=sub['projection_factor']),
            })
res_all = pd.DataFrame(results_all)

# ============================================================
# INCOME-BIN MEANS — PLOT_YEARS (for existing figure)
# ============================================================
results = []
for year in PLOT_YEARS:
    for q in bin_labels:
        sub = hhy[(hhy['panel_year'] == year) & (hhy['IncomeBin'] == q)]
        if len(sub) > 0:
            results.append({
                'year': year, 'income_bin': int(q), 'n': len(sub),
                'hi_allcott': np.average(sub['hi_allcott'], weights=sub['projection_factor']),
            })
res = pd.DataFrame(results)
log(res.pivot(index='income_bin', columns='year', values='hi_allcott').to_string())

delta_label = None
y0, y1 = CHANGE_YEARS
res_wide = res.pivot(index='income_bin', columns='year', values='hi_allcott')
if y0 in res_wide.columns and y1 in res_wide.columns:
    delta = res_wide[y1] - res_wide[y0]
    delta_label = (
        f"ΔHI ({y1} - {y0})\n"
        f"Lowest bin: {delta.loc[delta.index.min()]:+.3f}\n"
        f"Highest bin: {delta.loc[delta.index.max()]:+.3f}"
    )

# ============================================================
# FIGURE 1: lines per year, x = income bin
# ============================================================
log("Creating figure 1 (by income bin)...")
fig, ax = plt.subplots(figsize=(8, 5.5))
years_sorted = sorted(res['year'].unique())
palette      = plt.cm.tab10(np.linspace(0, 1, max(len(years_sorted), 1)))
marker_cycle = ['o', 's', '^', 'D', 'v', 'P', 'X', '*', '<', '>']
colors  = {y: palette[i % len(palette)]      for i, y in enumerate(years_sorted)}
markers = {y: marker_cycle[i % len(marker_cycle)] for i, y in enumerate(years_sorted)}

if N_INCOME_BINS == 5:
    xlabels = ['Q1\n(Lowest)', 'Q2', 'Q3', 'Q4', 'Q5\n(Highest)']
elif N_INCOME_BINS == 10:
    xlabels = [f'D{i}' for i in range(1, 11)]
    xlabels[0] = 'D1\n(Lowest)'; xlabels[-1] = 'D10\n(Highest)'
elif N_INCOME_BINS == 4:
    xlabels = ['Q1\n(Lowest)', 'Q2', 'Q3', 'Q4\n(Highest)']
else:
    xlabels = [str(i) for i in range(1, N_INCOME_BINS + 1)]
    xlabels[0] = f'1\n(Lowest)'; xlabels[-1] = f'{N_INCOME_BINS}\n(Highest)'

for year in years_sorted:
    yr_data = res[res['year'] == year].sort_values('income_bin')
    ax.plot(yr_data['income_bin'], yr_data['hi_allcott'],
            marker=markers[year], color=colors[year], linewidth=2, markersize=8,
            label=str(year), zorder=5)

ax.set_xticks(bin_labels)
ax.set_xticklabels(xlabels, fontsize=11)
ax.set_xlabel(f'Household Income Bin ({N_INCOME_BINS} groups)', fontsize=12)
ax.set_ylabel('Health Index (std. dev.)', fontsize=12)
ax.set_title('Healthfulness of Grocery Purchases by Income Over Time', fontsize=14, fontweight='bold')
ax.legend(title='Year', fontsize=11, title_fontsize=11)
if delta_label:
    ax.text(0.02, 0.98, delta_label, transform=ax.transAxes, va='top', ha='left', fontsize=9,
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8, edgecolor='gray'))
ax.grid(True, alpha=0.3)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.axhline(0, color='gray', linewidth=0.5, linestyle='--', alpha=0.5)

plt.tight_layout()
plt.savefig(FIG_DIR / 'hi_by_income_over_time.png', bbox_inches='tight', dpi=150)
plt.close()
log(f"Saved: {FIG_DIR / 'hi_by_income_over_time.png'}")

# ============================================================
# FIGURE 2: gap chart — x = year, vertical bars Q1 to Q5
# ============================================================
log("Creating figure 2 (inequality gap over time)...")

# Color quintiles from dark (low income) to light (high income)
q_colors = plt.cm.RdYlGn(np.linspace(0.15, 0.85, N_INCOME_BINS))
q_labels = {1: 'Q1 (Lowest)', 2: 'Q2', 3: 'Q3', 4: 'Q4', 5: 'Q5 (Highest)'}

fig, ax = plt.subplots(figsize=(10, 5.5))

# only graph through 2020
res_all = res_all[res_all['year'] <= 2020]
for year in all_years:
    yr = res_all[res_all['year'] == year].sort_values('income_bin')
    if len(yr) < 2:
        continue
    hi_vals = yr['hi_allcott'].values
    # Vertical line from Q1 to Q5
    ax.plot([year, year], [hi_vals[0], hi_vals[-1]], color='gray', linewidth=1.5, zorder=2)

for q_idx, q in enumerate(bin_labels):
    q_data = res_all[res_all['income_bin'] == q].sort_values('year')
    ax.scatter(q_data['year'], q_data['hi_allcott'],
               color=q_colors[q_idx], s=60, zorder=5,
               label=q_labels.get(q, f'Q{q}'), edgecolors='white', linewidth=0.5)

ax.set_xlabel('Year', fontsize=12)
ax.set_ylabel('Health Index (std. dev.)', fontsize=12)
ax.set_title('Nutritional Inequality Over Time\n(vertical bars span Q1–Q5 within each year)',
             fontsize=13, fontweight='bold')
ax.legend(title='Income quintile', fontsize=10, title_fontsize=10,
          loc='upper left', framealpha=0.9)
ax.grid(True, alpha=0.3, axis='y')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
# ax.axhline(0, color='gray', linewidth=0.5, linestyle='--', alpha=0.5)

plt.tight_layout()
plt.savefig(FIG_DIR / 'hi_inequality_over_time.png', bbox_inches='tight', dpi=150)
plt.close()
log(f"Saved: {FIG_DIR / 'hi_inequality_over_time.png'}")

# ============================================================
# FIGURE 3: adjacent quintile gaps over time
# ============================================================
log("Creating figure 3 (adjacent quintile gaps over time)...")

res_wide_all = res_all.pivot(index='year', columns='income_bin', values='hi_allcott')
gap_labels = {
    (1, 4): 'Q4 − Q1',
}
gap_colors = ['#fc8d59']

fig, ax = plt.subplots(figsize=(10, 5.5))
for (q_lo, q_hi), color in zip(gap_labels, gap_colors):
    if q_lo in res_wide_all.columns and q_hi in res_wide_all.columns:
        gap = res_wide_all[q_hi] - res_wide_all[q_lo]
        ax.plot(gap.index, gap.values, marker='o', linewidth=2, markersize=6,
                label=gap_labels[(q_lo, q_hi)], color=color)

ax.axhline(0, color='gray', linewidth=0.8, linestyle='--', alpha=0.6)
ax.set_xlabel('Year', fontsize=12)
ax.set_ylabel('HI gap (std. dev.)', fontsize=12)
ax.set_title('Nutritional Inequality (Top Minus Bottom Income Quintile) Over Time',
             fontsize=13, fontweight='bold')
ax.legend(fontsize=10, framealpha=0.9)
ax.grid(True, alpha=0.3, axis='y')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig(FIG_DIR / 'hi_quintile_gaps_over_time.png', bbox_inches='tight', dpi=150)
plt.close()
log(f"Saved: {FIG_DIR / 'hi_quintile_gaps_over_time.png'}")
log("Done!")

# ============================================================
# FIGURE 4: HI distribution percentiles over time
# ============================================================
log("Creating figure 4 (HI percentile trajectories)...")

PERCENTILES = [10, 25, 50, 75, 90]

def weighted_percentile(values, weights, pcts):
    """Weighted quantile — returns array of len(pcts)."""
    sorter = np.argsort(values)
    sv = values[sorter]
    sw = weights[sorter]
    cw = np.cumsum(sw)
    cw /= cw[-1]
    return np.interp([p / 100 for p in pcts], cw, sv)

pct_rows = []
for year in all_years:
    sub = hhy[hhy['panel_year'] == year].dropna(subset=['hi_allcott', 'projection_factor'])
    if len(sub) < 10:
        continue
    pcts = weighted_percentile(sub['hi_allcott'].values,
                               sub['projection_factor'].values,
                               PERCENTILES)
    for p, v in zip(PERCENTILES, pcts):
        pct_rows.append({'year': year, 'percentile': p, 'hi': v})

pct_df = pd.DataFrame(pct_rows)

pct_colors  = plt.cm.RdYlGn(np.linspace(0.15, 0.85, len(PERCENTILES)))
pct_styles  = ['-', '--', '-', '--', '-']

fig, ax = plt.subplots(figsize=(10, 5.5))
for i, p in enumerate(PERCENTILES):
    d = pct_df[pct_df['percentile'] == p].sort_values('year')
    ax.plot(d['year'], d['hi'], color=pct_colors[i], linestyle=pct_styles[i],
            linewidth=2, marker='o', markersize=4, label=f'p{p}')

ax.axhline(0, color='gray', linewidth=0.5, linestyle='--', alpha=0.5)
ax.set_xlabel('Year', fontsize=12)
ax.set_ylabel('Health Index (std. dev.)', fontsize=12)
ax.set_title('Shift in HI Distribution Over Time\n(weighted percentiles)', fontsize=13, fontweight='bold')
ax.legend(title='Percentile', fontsize=10, title_fontsize=10)
ax.grid(True, alpha=0.3, axis='y')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.tight_layout()
plt.savefig(FIG_DIR / 'hi_percentiles_over_time.png', bbox_inches='tight', dpi=150)
plt.close()
log(f"Saved: {FIG_DIR / 'hi_percentiles_over_time.png'}")

# ============================================================
# FIGURES 5-7: Inequality in produce / whole / sugar by income quintile
# ============================================================
log("Creating figures 5-7 (simple measure inequality by income quintile)...")

SIMPLE_MEASURES = [
    ('produce',          'Produce Share (fraction of calories)', 'produce_inequality_over_time.png'),
    ('whole',            'Whole Grain Bread Share (fraction of bread cals)', 'whole_inequality_over_time.png'),
    ('sugar_per_1000cal','Sugar per 1000 kcal (grams)', 'sugar_inequality_over_time.png'),
]

q_colors_2 = {1: '#d73027', 2: '#fc8d59', 3: '#fee090', 4: '#91bfdb', 5: '#4575b4'}

for var, ylabel, fname in SIMPLE_MEASURES:
    if var not in hhy.columns:
        log(f"  Skipping {var} — not in dataset")
        continue

    rows = []
    for year in all_years:
        for q in bin_labels:
            sub = hhy[(hhy['panel_year'] == year) & (hhy['IncomeBin'] == q)].dropna(subset=[var, 'projection_factor'])
            if len(sub) < 5:
                continue
            rows.append({
                'year': year, 'income_bin': int(q),
                'value': np.average(sub[var], weights=sub['projection_factor'])
            })
    df_m = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=(10, 5.5))
    for q in bin_labels:
        d = df_m[df_m['income_bin'] == q].sort_values('year')
        lw = 2.5 if q in (1, 5) else 1.2
        ls = '-' if q in (1, 5) else '--'
        lbl = q_labels.get(q, f'Q{q}')
        ax.plot(d['year'], d['value'], color=q_colors[q - 1], linewidth=lw,
                linestyle=ls, marker='o', markersize=4 if q in (1, 5) else 2,
                label=lbl, alpha=0.9)

    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(f'{ylabel}\nBy Income Quintile Over Time', fontsize=13, fontweight='bold')
    ax.legend(title='Income quintile', fontsize=10, title_fontsize=10,
              loc='best', framealpha=0.9)
    ax.grid(True, alpha=0.3, axis='y')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig(FIG_DIR / fname, bbox_inches='tight', dpi=150)
    plt.close()
    log(f"Saved: {FIG_DIR / fname}")

log("All done!")
