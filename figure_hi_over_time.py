"""
Nutritional inequality over time: Health Index by income quartile for selected years.
Loads the pre-built panel dataset from build_panel_dataset.py.
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

PLOT_YEARS    = [2005, 2010, 2015, 2020, 2024]
N_INCOME_BINS = 4
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
    ['household_code', 'HHAvIncome', 'projection_factor']].dropna()
all_hh = all_hh[all_hh['projection_factor'] > 0].sort_values('HHAvIncome').reset_index(drop=True)
cumwt = all_hh['projection_factor'].cumsum().to_numpy()
cumwt = cumwt / cumwt[-1]
cuts = np.interp(np.arange(1, N_INCOME_BINS) / N_INCOME_BINS, cumwt, all_hh['HHAvIncome'].to_numpy()).tolist()
for i in range(1, len(cuts)):
    if cuts[i] <= cuts[i - 1]:
        cuts[i] = np.nextafter(cuts[i - 1], np.inf)

bin_labels = list(range(1, N_INCOME_BINS + 1))
hhy['IncomeBin'] = pd.cut(
    hhy['HHAvIncome'], bins=[-np.inf] + cuts + [np.inf],
    labels=bin_labels, include_lowest=True).astype(float)

hhy = hhy[hhy['panel_year'].isin(PLOT_YEARS)]
log(f"  {len(hhy):,} HH-year obs after filtering to plot years")

# ============================================================
# INCOME-BIN MEANS BY YEAR
# ============================================================
log("Computing income-bin means by year...")
results = []
for year in PLOT_YEARS:
    for q in bin_labels:
        sub = hhy[(hhy['panel_year'] == year) & (hhy['IncomeBin'] == q)]
        if len(sub) > 0:
            results.append({
                'year': year, 'income_bin': int(q), 'n': len(sub),
                'HI': np.average(sub['HI'], weights=sub['projection_factor']),
            })
res = pd.DataFrame(results)
log(res.pivot(index='income_bin', columns='year', values='HI').to_string())

delta_label = None
y0, y1 = CHANGE_YEARS
res_wide = res.pivot(index='income_bin', columns='year', values='HI')
if y0 in res_wide.columns and y1 in res_wide.columns:
    delta = res_wide[y1] - res_wide[y0]
    delta_label = (
        f"ΔHI ({y1} - {y0})\n"
        f"Mean bins: {delta.mean():+.3f}\n"
        f"Lowest bin: {delta.loc[delta.index.min()]:+.3f}\n"
        f"Highest bin: {delta.loc[delta.index.max()]:+.3f}"
    )

# ============================================================
# FIGURE
# ============================================================
log("Creating figure...")
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
    ax.plot(yr_data['income_bin'], yr_data['HI'],
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
plt.savefig(FIG_DIR / 'hi_by_income_over_time.pdf', bbox_inches='tight')
plt.savefig(FIG_DIR / 'hi_by_income_over_time.png', bbox_inches='tight', dpi=150)
log(f"Saved: {FIG_DIR / 'hi_by_income_over_time.pdf'}")
log("Done!")
