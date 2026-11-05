"""Full-panel comparison of the Syndigo- and USDA-based household Health Indices.

Replaces the note's original 5,000-HH-sample versions with the full panel
(all households, 2004-2020), using the PRODUCTION indices: `hi` from
panel_hh_year.parquet and `hi_usda` from hi_usda_panel.parquet.

Inputs:  interim/panel_dataset/panel_hh_year.parquet
         interim/panel_dataset/hi_usda_panel.parquet
         interim/usda_comparison/hh_year_bothmatched.parquet  (optional, for the
             identically-matched-rows correlation printed for the note text)
Outputs (Overleaf figs/usda_diagnostics/):
         hi_distribution.pdf   hi_by_year.pdf   hi_scatter.pdf
Prints:  every number quoted in the note's Section 4/5 text (N, correlations,
         medians, off-diagonal cluster shares).
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
PANEL   = BASE / 'interim' / 'panel_dataset' / 'panel_hh_year.parquet'
USDAP   = BASE / 'interim' / 'panel_dataset' / 'hi_usda_panel.parquet'
BOTHM   = BASE / 'interim' / 'usda_comparison' / 'hh_year_bothmatched.parquet'
FIG_DIR = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs/usda_diagnostics')
FIG_DIR.mkdir(parents=True, exist_ok=True)

# presentation-ready styling: big labels, fat lines
plt.rcParams.update({
    'axes.labelsize': 18, 'axes.titlesize': 19, 'xtick.labelsize': 15,
    'ytick.labelsize': 15, 'legend.fontsize': 15, 'lines.linewidth': 3,
    'axes.spines.top': False, 'axes.spines.right': False, 'figure.dpi': 150,
})
SYN_C, USDA_C = '#1f77b4', '#d62728'

syn = pd.read_parquet(PANEL, columns=['household_code', 'panel_year',
                                      'rHI_per_1000cal', 'hi', 'projection_factor'])
usd = pd.read_parquet(USDAP, columns=['household_code', 'panel_year', 'rHI_usda', 'hi_usda'])
df = syn.merge(usd, on=['household_code', 'panel_year'], how='inner')
n_hh = df['household_code'].nunique()
print(f"Merged full panel: {len(df):,} HH-years, {n_hh:,} households, "
      f"{df['panel_year'].min()}-{df['panel_year'].max()}")

# ================= hi_distribution.pdf =================
fig, ax = plt.subplots(figsize=(9, 6))
lo, hi_ = df[['rHI_per_1000cal', 'rHI_usda']].stack().quantile([0.005, 0.995])
bins = np.linspace(lo, hi_, 80)
for col, lab, c in [('rHI_per_1000cal', 'Syndigo', SYN_C), ('rHI_usda', 'USDA', USDA_C)]:
    ax.hist(df[col].clip(lo, hi_), bins=bins, density=True,
            histtype='step', label=lab, color=c)
    ax.axvline(df[col].median(), color=c, linestyle='--', linewidth=2.5)
ax.set_xlabel('Health Index per 1,000 kcal (raw)')
ax.set_ylabel('Density')
ax.set_title('Household HI distribution, full panel')
ax.legend(frameon=False)
fig.tight_layout()
fig.savefig(FIG_DIR / 'hi_distribution.pdf')
plt.close(fig)
print(f"medians: Syndigo {df['rHI_per_1000cal'].median():.3f}, USDA {df['rHI_usda'].median():.3f}")

# ================= hi_by_year.pdf =================
fig, ax = plt.subplots(figsize=(9, 6))
for col, lab, c in [('hi', 'Syndigo', SYN_C), ('hi_usda', 'USDA', USDA_C)]:
    g = df.groupby('panel_year')[col].quantile([0.25, 0.5, 0.75]).unstack()
    ax.plot(g.index, g[0.5], label=lab, color=c)
    ax.fill_between(g.index, g[0.25], g[0.75], color=c, alpha=0.15, linewidth=0)
ax.set_xlabel('Panel year')
ax.set_ylabel('Standardized HI (median, IQR band)')
ax.set_title('Household HI by year, full panel')
ax.legend(frameon=False)
fig.tight_layout()
fig.savefig(FIG_DIR / 'hi_by_year.pdf')
plt.close(fig)

# ================= hi_scatter.pdf =================
fig, ax = plt.subplots(figsize=(8, 7.2))
m = df[['hi', 'hi_usda']].clip(-4, 4)
hb = ax.hexbin(m['hi'], m['hi_usda'], gridsize=70, bins='log', cmap='viridis', mincnt=1)
ax.plot([-4, 4], [-4, 4], 'k--', linewidth=2.5)
ax.set_xlabel('Syndigo HI (standardized)')
ax.set_ylabel('USDA HI (standardized)')
ax.set_title('Household-year HI, full panel')
fig.colorbar(hb, ax=ax, label='log$_{10}$ count')
fig.tight_layout()
fig.savefig(FIG_DIR / 'hi_scatter.pdf')
plt.close(fig)

# ================= stats for the note text =================
pear = df['hi'].corr(df['hi_usda'])
spear = df['hi'].rank().corr(df['hi_usda'].rank())  # Spearman without scipy
w = df[['hi', 'hi_usda']].copy()
for c in w:
    w[c] = w[c].clip(w[c].quantile(0.01), w[c].quantile(0.99))
wr = w['hi'].corr(w['hi_usda'])
br = ((df['hi'] > 1) & (df['hi_usda'] < -1)).mean()   # healthy-Syndigo / unhealthy-USDA
tl = ((df['hi'] < -1) & (df['hi_usda'] > 1)).mean()   # the mirror clump
print(f"corr (realistic, full panel): Pearson {pear:.3f}, Spearman {spear:.3f}, winsorized {wr:.3f}")
print(f"off-diagonal clusters (>1 sd on one index, <-1 sd on the other): "
      f"bottom-right {br:.1%}, top-left {tl:.1%}")

if BOTHM.exists():
    bm = pd.read_parquet(BOTHM)
    bp = bm['rHI_syn_bm'].corr(bm['rHI_usda_bm'])
    bs = bm['rHI_syn_bm'].rank().corr(bm['rHI_usda_bm'].rank())  # Spearman without scipy
    print(f"identically-matched rows ({len(bm):,} HH-years): Pearson {bp:.3f}, Spearman {bs:.3f}")
else:
    print("(hh_year_bothmatched.parquet not built yet -- run 1_build_comparison_universe.py "
          "for the identically-matched-rows correlation)")
print("Saved: hi_distribution.pdf, hi_by_year.pdf, hi_scatter.pdf")
