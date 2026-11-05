"""Per-100g nutrient agreement between Syndigo and USDA on both-matched universe UPCs.

Inputs:  interim/usda_comparison/upc_universe.parquet (universe UPC list)
         interim/syndigo_nielsen_merged/syndigo_wide.parquet
         interim/usda_nielsen_merged/usda_wide.parquet
Output:  Overleaf figs/usda_diagnostics/nutrient_agreement.pdf  (6 hexbin panels)
Prints:  the note's agreement table (tab:nut): n both, Pearson, rank rho,
         winsorized r, median |diff|, per-source medians.
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
    'axes.labelsize': 16, 'axes.titlesize': 17, 'xtick.labelsize': 13,
    'ytick.labelsize': 13, 'figure.dpi': 150,
})

u = pd.read_parquet(BASE / 'interim' / 'usda_comparison' / 'upc_universe.parquet',
                    columns=['upc_13'])
syn = pd.read_parquet(BASE / 'interim' / 'syndigo_nielsen_merged' / 'syndigo_wide.parquet'
                      ).rename(columns={'upc': 'upc_13'})
usd = pd.read_parquet(BASE / 'interim' / 'usda_nielsen_merged' / 'usda_wide.parquet'
                      ).rename(columns={'upc13': 'upc_13'})

NUTS = [('cal_per_100g', 'Calories (kcal)'), ('fiber_per_100g', 'Fiber (g)'),
        ('sugar_per_100g', 'Total sugars (g)'), ('satfat_per_100g', 'Sat. fat (g)'),
        ('sodium_per_100g', 'Sodium (g)'), ('chol_per_100g', 'Cholesterol (g)')]
cols = [c for c, _ in NUTS]
m = (u.merge(syn[['upc_13'] + cols], on='upc_13')
      .merge(usd[['upc_13'] + cols], on='upc_13', suffixes=('_syn', '_usda')))
both_cal = m['cal_per_100g_syn'].notna() & m['cal_per_100g_usda'].notna()
m = m[both_cal]
print(f"Universe UPCs matched by BOTH sources (calories tier): {len(m):,}")

fig, axes = plt.subplots(2, 3, figsize=(16, 10.5))
print(f"\n{'Nutrient':<18}{'n both':>9}{'Pearson':>9}{'Rank rho':>9}{'Winsor r':>9}"
      f"{'Med|d|':>9}{'Syn med':>9}{'USDA med':>9}")
for ax, (c, lab) in zip(axes.flat, NUTS):
    a, b = m[f'{c}_syn'], m[f'{c}_usda']
    ok = a.notna() & b.notna()
    a, b = a[ok], b[ok]
    pear = a.corr(b)
    rank = a.rank().corr(b.rank())
    aw = a.clip(a.quantile(0.01), a.quantile(0.99))
    bw = b.clip(b.quantile(0.01), b.quantile(0.99))
    winr = aw.corr(bw)
    medd = (a - b).abs().median()
    print(f"{lab:<18}{ok.sum():>9,}{pear:>9.2f}{rank:>9.2f}{winr:>9.2f}"
          f"{medd:>9.3f}{a.median():>9.3f}{b.median():>9.3f}")
    hi = max(aw.quantile(0.995), bw.quantile(0.995))
    hb = ax.hexbin(a.clip(0, hi), b.clip(0, hi), gridsize=55, bins='log',
                   cmap='viridis', mincnt=1)
    ax.plot([0, hi], [0, hi], 'k--', linewidth=2)
    ax.set_title(lab)
    ax.set_xlabel('Syndigo'); ax.set_ylabel('USDA')
fig.suptitle('Per-100g nutrient values on both-matched UPCs', fontsize=19, y=1.0)
fig.tight_layout()
fig.savefig(FIG_DIR / 'nutrient_agreement.pdf')
print("\nSaved nutrient_agreement.pdf")
