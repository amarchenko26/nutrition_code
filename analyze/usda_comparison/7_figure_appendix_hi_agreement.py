"""Publication (paper-appendix) figure: agreement between the Syndigo- and
USDA-based household Health Indices.

Unlike the diagnostic hexbins in the comparison note, this is a clean binned
scatter for a paper appendix: ventile means of the USDA index against the
Syndigo index (projection-weighted), a 45-degree reference, and the
correlation annotated. Full panel, 2004-2020.

Inputs:  interim/panel_dataset/panel_hh_year.parquet
         interim/panel_dataset/hi_usda_panel.parquet
Output:  Overleaf figs/appendix_hi_syndigo_usda_binscatter.pdf
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
FIG_DIR = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs')

plt.rcParams.update({
    'axes.labelsize': 16, 'xtick.labelsize': 13, 'ytick.labelsize': 13,
    'legend.fontsize': 13, 'figure.dpi': 200,
    'axes.spines.top': False, 'axes.spines.right': False,
})

syn = pd.read_parquet(BASE / 'interim/panel_dataset/panel_hh_year.parquet',
                      columns=['household_code', 'panel_year', 'hi', 'projection_factor'])
usd = pd.read_parquet(BASE / 'interim/panel_dataset/hi_usda_panel.parquet',
                      columns=['household_code', 'panel_year', 'hi_usda'])
df = syn.merge(usd, on=['household_code', 'panel_year'], how='inner').dropna()

pear = df['hi'].corr(df['hi_usda'])
spear = df['hi'].rank().corr(df['hi_usda'].rank())

# projection-weighted ventile means of hi_usda by hi
df = df.sort_values('hi')
cum = df['projection_factor'].cumsum() / df['projection_factor'].sum()
df['bin'] = np.minimum((cum * 20).astype(int), 19)
g = df.groupby('bin').apply(
    lambda d: pd.Series({
        'x': np.average(d['hi'], weights=d['projection_factor']),
        'y': np.average(d['hi_usda'], weights=d['projection_factor']),
    }), include_groups=False)

fig, ax = plt.subplots(figsize=(6.5, 5.2))
lim = [g[['x', 'y']].min().min() - 0.15, g[['x', 'y']].max().max() + 0.15]
ax.plot(lim, lim, color='0.65', linestyle='--', linewidth=1.4, zorder=1,
        label='45$^\\circ$ line')
ax.scatter(g['x'], g['y'], s=55, color='#2c5f8a', zorder=3,
           edgecolors='white', linewidth=0.6, label='Ventile means')
ax.set_xlabel('Health Index, Syndigo (std. dev.)')
ax.set_ylabel('Health Index, USDA (std. dev.)')
ax.set_xlim(lim); ax.set_ylim(lim)
ax.set_aspect('equal')
ax.annotate(f"$\\rho$ = {pear:.2f}   rank $\\rho$ = {spear:.2f}\n"
            f"N = {len(df):,} household-years",
            xy=(0.04, 0.92), xycoords='axes fraction', fontsize=13, va='top')
ax.legend(frameon=False, loc='lower right')
fig.tight_layout()
fig.savefig(FIG_DIR / 'appendix_hi_syndigo_usda_binscatter.pdf', bbox_inches='tight')
print(f"corr {pear:.3f} (rank {spear:.3f}), N={len(df):,}")
print("Saved appendix_hi_syndigo_usda_binscatter.pdf")
