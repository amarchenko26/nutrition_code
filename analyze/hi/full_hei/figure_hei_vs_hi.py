"""Figures for the simple-HI vs. full-HEI note (notes/hei_simple_vs_full.tex).

Outputs (Overleaf figs/hei_diagnostics/):
  hei_distribution.pdf  -- distribution of the full HEI (0-100) and of the
                           simple HI, both standardized, overlaid
  hei_vs_hi.pdf         -- ventile binscatter of the full HEI against the
                           simple HI (household-years, full panel)
Prints: correlations and summary stats quoted in the note.
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
FIG_DIR = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/hei_diagnostics')
FIG_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({
    'axes.labelsize': 17, 'axes.titlesize': 18, 'xtick.labelsize': 14,
    'ytick.labelsize': 14, 'legend.fontsize': 14, 'lines.linewidth': 3,
    'axes.spines.top': False, 'axes.spines.right': False, 'figure.dpi': 150,
})
C_SIMPLE, C_FULL = '#2c5f8a', '#a63d40'

hi = pd.read_parquet(BASE / 'interim/panel_dataset/panel_hh_year.parquet',
                     columns=['household_code', 'panel_year', 'hi', 'projection_factor'])
he = pd.read_parquet(BASE / 'interim/panel_dataset/hei_usda_panel.parquet',
                     columns=['household_code', 'panel_year', 'hei_usda', 'hei_usda_90'])
df = hi.merge(he, on=['household_code', 'panel_year'], how='inner').dropna(
    subset=['hi', 'hei_usda'])
print(f"Merged: {len(df):,} HH-years")
print(f"HEI (0-100): mean {df['hei_usda'].mean():.1f}, median {df['hei_usda'].median():.1f}, "
      f"p10 {df['hei_usda'].quantile(0.1):.1f}, p90 {df['hei_usda'].quantile(0.9):.1f}")
pear = df['hi'].corr(df['hei_usda'])
spear = df['hi'].rank().corr(df['hei_usda'].rank())
print(f"corr(simple HI, full HEI): Pearson {pear:.3f}, rank {spear:.3f}")
print(f"corr(simple HI, HEI without fatty-acid component): "
      f"{df['hi'].corr(df['hei_usda_90']):.3f}")

# ---- distributions, both standardized ----
fig, ax = plt.subplots(figsize=(9, 6))
for col, lab, c in [('hi', 'Simple index', C_SIMPLE), ('hei_usda', 'Full HEI', C_FULL)]:
    z = (df[col] - df[col].mean()) / df[col].std()
    z = z.clip(-4, 4)
    ax.hist(z, bins=90, density=True, histtype='step', label=lab, color=c)
ax.set_xlabel('Index (standardized)')
ax.set_ylabel('Density')
ax.legend(frameon=False)
fig.tight_layout()
fig.savefig(FIG_DIR / 'hei_distribution.pdf')
plt.close(fig)

# ---- binscatter: full HEI vs simple HI ----
df = df.sort_values('hi')
cum = df['projection_factor'].cumsum() / df['projection_factor'].sum()
df['bin'] = np.minimum((cum * 20).astype(int), 19)
g = df.groupby('bin').apply(
    lambda d: pd.Series({
        'x': np.average(d['hi'], weights=d['projection_factor']),
        'y': np.average(d['hei_usda'], weights=d['projection_factor'])}),
    include_groups=False)
fig, ax = plt.subplots(figsize=(8.5, 6))
ax.scatter(g['x'], g['y'], s=60, color=C_FULL, zorder=3, edgecolors='white', linewidth=0.6)
ax.set_xlabel('Simple index (std. dev.)')
ax.set_ylabel('Full HEI (0-100 points)')
fig.tight_layout()
fig.savefig(FIG_DIR / 'hei_vs_hi.pdf')
plt.close(fig)
print("Saved hei_distribution.pdf, hei_vs_hi.pdf")
