"""Coverage of the Nielsen purchase universe by source and tier (full universe).

Input:   interim/usda_comparison/upc_universe.parquet  (from 1_build_comparison_universe.py)
Output:  Overleaf figs/usda_diagnostics/coverage_bars.pdf
Prints:  the note's match-rate table (tab:match) and overlap table (tab:overlap).
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
SYN_C, USDA_C = '#1f77b4', '#d62728'

u = pd.read_parquet(BASE / 'interim' / 'usda_comparison' / 'upc_universe.parquet')
tot_upc, tot_spend = len(u), u['spend'].sum()
print(f"Universe: {tot_upc:,} UPCs, ${tot_spend/1e9:.1f}B real (2013$) spend, "
      f"{u['n_purch'].sum()/1e6:.1f}M purchases")

tiers = [('cal', 'Calories present\n(HI computable)'), ('macro', 'Complete\nmacro panel')]
stats = {}
for t, _ in tiers:
    for s in ['syn', 'usda']:
        f = u[f'{s}_{t}']
        stats[(s, t)] = (f.mean(), u.loc[f, 'spend'].sum() / tot_spend)
        print(f"  {s:5s} {t:5s}: UPC {f.mean():6.1%}   spend {u.loc[f,'spend'].sum()/tot_spend:6.1%}")

fig, axes = plt.subplots(1, 2, figsize=(13, 6), sharey=True)
x = np.arange(len(tiers)); w = 0.36
for ax, idx, ttl in [(axes[0], 0, 'Share of unique UPCs'), (axes[1], 1, 'Share of spending')]:
    ax.bar(x - w/2, [stats[('syn', t)][idx] for t, _ in tiers], w, label='Syndigo', color=SYN_C)
    ax.bar(x + w/2, [stats[('usda', t)][idx] for t, _ in tiers], w, label='USDA', color=USDA_C)
    for i, (t, _) in enumerate(tiers):
        for dx, s in [(-w/2, 'syn'), (w/2, 'usda')]:
            ax.text(i + dx, stats[(s, t)][idx] + 0.012, f"{stats[(s, t)][idx]:.0%}",
                    ha='center', fontsize=14, fontweight='bold')
    ax.set_xticks(x); ax.set_xticklabels([lab for _, lab in tiers], fontsize=15)
    ax.set_title(ttl)
axes[0].set_ylabel('Share of Nielsen purchase universe')
axes[0].legend(frameon=False, loc='upper right')
axes[0].set_ylim(0, max(v for k, v in stats.items() for v in [stats[k][1]]) + 0.1)
fig.tight_layout()
fig.savefig(FIG_DIR / 'coverage_bars.pdf')
print("Saved coverage_bars.pdf")

# overlap table on the complete-macro tier
both = u['syn_macro'] & u['usda_macro']
only_u = u['usda_macro'] & ~u['syn_macro']
only_s = u['syn_macro'] & ~u['usda_macro']
either = u['syn_macro'] | u['usda_macro']
print("\nOverlap (complete-macro tier):")
for name, m in [('Both', both), ('USDA only', only_u), ('Syndigo only', only_s),
                ('Either', either), ('Neither', ~either)]:
    print(f"  {name:12s}: {m.sum():>9,} UPCs  {m.mean():6.1%} of universe  "
          f"{u.loc[m,'spend'].sum()/tot_spend:6.1%} of spend")
