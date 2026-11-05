"""Is USDA coverage selective on product healthiness? (new note figure)

Among nutrient-scored (non-produce) UPCs that Syndigo covers, plot the
spend-weighted probability that USDA also covers the UPC, by ventile of the
product's Syndigo Health Index. A flat profile = USDA coverage is not
selecting on healthiness; a slope = coverage selectivity.

Input:   interim/usda_comparison/upc_universe.parquet
Output:  Overleaf figs/usda_diagnostics/usda_coverage_by_syndigo_hi.pdf
Prints:  spend-weighted coverage by healthiness quintile + the UPC-level
         correlation between Syndigo HI and USDA coverage.
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
FIG_DIR = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/usda_diagnostics')
plt.rcParams.update({
    'axes.labelsize': 18, 'axes.titlesize': 19, 'xtick.labelsize': 15,
    'ytick.labelsize': 15, 'legend.fontsize': 15, 'lines.linewidth': 3,
    'axes.spines.top': False, 'axes.spines.right': False, 'figure.dpi': 150,
})
USDA_C = '#d62728'

u = pd.read_parquet(BASE / 'interim' / 'usda_comparison' / 'upc_universe.parquet')
# Syndigo-scored, nutrient-based (non-produce) products with positive spend
s = u[u['syn_cal'] & ~(u['is_fruit'] | u['is_veg']) & (u['spend'] > 0)
      & u['hi1000_syn'].notna()].copy()
# trim the heavy per-1000kcal tails for binning only
lo, hi = s['hi1000_syn'].quantile([0.01, 0.99])
s['hi_t'] = s['hi1000_syn'].clip(lo, hi)
print(f"Sample: {len(s):,} Syndigo-scored non-produce UPCs, "
      f"${s['spend'].sum()/1e9:.1f}B spend")

# spend-weighted ventiles of Syndigo product HI
s = s.sort_values('hi_t')
cum = s['spend'].cumsum() / s['spend'].sum()
s['bin'] = np.minimum((cum * 20).astype(int), 19)
g = s.groupby('bin').apply(
    lambda d: pd.Series({
        'hi_mid': np.average(d['hi_t'], weights=d['spend']),
        'cov': np.average(d['usda_cal'].astype(float), weights=d['spend']),
    }), include_groups=False).reset_index()

fig, ax = plt.subplots(figsize=(9, 6.5))
ax.plot(g['hi_mid'], g['cov'], 'o-', color=USDA_C, markersize=9)
wmean = np.average(s['usda_cal'].astype(float), weights=s['spend'])
ax.axhline(wmean, color='gray', linestyle='--', linewidth=2,
           label=f'Overall spend-wt. coverage ({wmean:.0%})')
ax.set_xlabel('Product healthiness: Syndigo HI per 1,000 kcal')
ax.set_ylabel('Share of spend covered by USDA')
ax.set_title('USDA coverage by product healthiness')
ax.set_ylim(0, 1)
ax.legend(frameon=False, loc='lower right')
fig.tight_layout()
fig.savefig(FIG_DIR / 'usda_coverage_by_syndigo_hi.pdf')

# headline stats
r_upc = s['hi1000_syn'].rank().corr(s['usda_cal'].astype(float).rank())
print(f"UPC-level rank correlation, Syndigo HI vs USDA coverage: {r_upc:.3f}")
s['quint'] = np.minimum((cum * 5).astype(int), 4)
q = s.groupby('quint').apply(
    lambda d: np.average(d['usda_cal'].astype(float), weights=d['spend']),
    include_groups=False)
print("Spend-wt. USDA coverage by Syndigo-HI quintile (least->most healthy):")
print("  " + "  ".join(f"Q{i+1} {v:.1%}" for i, v in q.items()))
print("Saved usda_coverage_by_syndigo_hi.pdf")
