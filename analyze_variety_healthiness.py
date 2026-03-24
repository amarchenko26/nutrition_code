"""
analyze_variety_healthiness.py

Correlates module-level healthiness (HI + nutrients) with product variety
(ssnp, ssep, n_upcs_new, etc.) across product modules.

Inputs:
  - module_healthiness.parquet   (one row per module)
  - rms_variety_module_fips_year.parquet  (module × fips × year)

Output: scatterplots saved to OUT_DIR
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

base    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
OUT_DIR = base / 'interim' / 'rms_variety'
FIG_DIR = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs')
FIG_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# LOAD
# ============================================================
health    = pd.read_parquet(OUT_DIR / 'module_healthiness.parquet')
claude_hi = pd.read_parquet(OUT_DIR / 'claude_hi_scores.parquet',
                            columns=['product_module_code', 'claude_hi', 'rationale'])

variety = pd.read_parquet(OUT_DIR / 'rms_variety_module_fips_year.parquet',
                          columns=['product_module_code', 'year',
                                   'total_spending', 'ssnp', 'ssep',
                                   'n_upcs', 'n_upcs_new', 'n_upcs_exit'])

# Drop 2006: all UPCs appear "new" that year due to left-censoring (no prior data)
variety = variety[variety['year'] != 2006]

# Collapse variety to module level (spending-weighted across fips, averaged over years)
variety['ssnp_wt']     = variety['ssnp']     * variety['total_spending']
variety['ssep_wt']     = variety['ssep']     * variety['total_spending']
variety['n_upcs_new_wt'] = variety['n_upcs_new'] * variety['total_spending']

mod_variety = (variety.groupby('product_module_code', as_index=False)
               .agg(total_spending = ('total_spending', 'sum'),
                    ssnp_num       = ('ssnp_wt',       'sum'),
                    ssep_num       = ('ssep_wt',       'sum'),
                    n_upcs         = ('n_upcs',        'mean'),
                    n_upcs_new     = ('n_upcs_new',    'mean')))
mod_variety['ssnp'] = mod_variety['ssnp_num'] / mod_variety['total_spending']
mod_variety['ssep'] = mod_variety['ssep_num'] / mod_variety['total_spending']
mod_variety['share_new_upcs'] = mod_variety['n_upcs_new'] / mod_variety['n_upcs']
mod_variety = mod_variety.drop(columns=['ssnp_num', 'ssep_num'])

# Merge module healthiness + variety
health_cols = ['hi_per_100g', 'fiber_per_100g', 'sugar_per_100g',
               'satfat_per_100g', 'sodium_per_100g']
df = health[['product_module_code', 'product_module_descr', 'product_group_code',
             'product_group_descr', 'pct_coverage'] + health_cols].merge(
    mod_variety, on='product_module_code', how='inner')

# Attach module-level Claude HI
df = df.merge(claude_hi, on='product_module_code', how='left')

print(f"Modules in analysis: {len(df):,}")
print(f"Dropping modules with <1% Syndigo coverage...")
df = df[df['pct_coverage'] >= 0.01].copy()
print(f"Modules after coverage filter: {len(df):,}")

# ============================================================
# STANDARDIZE: winsorize at 1st-99th pct, then z-score
# ============================================================
raw_health_vars = ['hi_per_100g', 'fiber_per_100g', 'sugar_per_100g',
                   'satfat_per_100g', 'sodium_per_100g']

for col in raw_health_vars:
    lo, hi_val = df[col].quantile([0.01, 0.99])
    w = df[col].clip(lo, hi_val)
    df[col + '_z'] = (w - w.mean()) / w.std()

# Claude HI: z-score on raw 0-10 scale (no product-group aggregation needed —
# scores are assigned directly at module level)
lo, hi_val = df['claude_hi'].quantile([0.01, 0.99])
w = df['claude_hi'].clip(lo, hi_val)
df['claude_hi_z'] = (w - w.mean()) / w.std()

health_vars  = [c + '_z' for c in raw_health_vars] + ['claude_hi_z']
variety_vars = ['ssnp', 'ssep', 'share_new_upcs']

# ============================================================
# CORRELATION TABLE
# ============================================================
print("\nCorrelations (Pearson r, winsorized z-scores):")
print(f"{'':25s}", end='')
for v in variety_vars:
    print(f"  {v:>15s}", end='')
print()
for h in health_vars:
    print(f"{h:25s}", end='')
    for v in variety_vars:
        mask = df[h].notna() & df[v].notna()
        r = np.corrcoef(df.loc[mask, h], df.loc[mask, v])[0, 1]
        print(f"  {r:>15.3f}", end='')
    print()

# ============================================================
# SCATTERPLOTS
# ============================================================
def scatter(x_col, y_col, xlabel, ylabel, fname, size_col='total_spending'):
    mask = df[x_col].notna() & df[y_col].notna()
    d = df[mask]
    sizes = np.sqrt(d[size_col]) / np.sqrt(d[size_col].max()) * 200
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(d[x_col], d[y_col], s=sizes, alpha=0.5, linewidths=0)
    m, b = np.polyfit(d[x_col], d[y_col], 1)
    xr = np.linspace(d[x_col].min(), d[x_col].max(), 100)
    ax.plot(xr, m * xr + b, color='black', linewidth=1)
    r = np.corrcoef(d[x_col], d[y_col])[0, 1]
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(f"r = {r:.3f}  (n = {mask.sum()})")
    ax.set_ylim(*d[y_col].quantile([0.01, 0.99]))
    plt.tight_layout()
    fig.savefig(FIG_DIR / fname, dpi=150)
    plt.close()
    print(f"Saved {fname}")

def quartile_dot(x_col, y_col, xlabel, ylabel, fname):
    """Dot-and-CI plot with connecting line, color gradient, and Q1-Q4 gap annotation."""
    mask = df[x_col].notna() & df[y_col].notna()
    d = df[mask].copy()
    d['quartile'] = pd.qcut(d[x_col], 4, labels=['Q1\n(least\nhealthy)', 'Q2', 'Q3', 'Q4\n(most\nhealthy)'])
    stats = d.groupby('quartile', observed=True)[y_col].agg(['mean', 'sem'])
    ci = 1.96 * stats['sem']
    means = stats['mean'].values
    xs = list(range(4))
    colors = ['#d73027', '#fc8d59', '#a1d99b', '#2ca25f']  # red → green gradient

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.grid(axis='y', color='#e5e5e5', linewidth=1.0, zorder=0)
    ax.set_axisbelow(True)
    for spine in ['top', 'right']:
        ax.spines[spine].set_visible(False)
    for spine in ['left', 'bottom']:
        ax.spines[spine].set_color('#cccccc')

    # 1. Connecting line
    ax.plot(xs, means, color='#999999', linewidth=1.2, zorder=1, linestyle='--')

    # Color-coded dots with CIs
    for i, (m, e, c) in enumerate(zip(means, ci.values, colors)):
        ax.errorbar(i, m, yerr=e, fmt='o', color=c,
                    markersize=10, capsize=5, linewidth=1.8, capthick=1.8,
                    markeredgewidth=0, zorder=2)

    ax.set_xticks(xs)
    ax.set_xticklabels(stats.index, fontsize=10)
    ax.set_xlabel(xlabel, fontsize=11, labelpad=8)
    ax.set_ylabel(ylabel, fontsize=11, labelpad=8)
    ax.tick_params(axis='both', length=0)
    pad = (means.max() - means.min()) * 0.8
    ax.set_ylim(means.min() - ci.max() - pad,
                means.max() + ci.max() + pad)
    plt.tight_layout()
    fig.savefig(FIG_DIR / fname, dpi=150)
    plt.close()
    print(f"Saved {fname}")

# ============================================================
# TOP MODULES BY HI
# ============================================================
print("\nTop 20 modules by Claude HI:")
top = df[['product_module_descr', 'claude_hi', 'ssnp', 'rationale']].nlargest(20, 'claude_hi')
print(top.to_string(index=False))

print("\nBottom 20 modules by Claude HI:")
bot = df[['product_module_descr', 'claude_hi', 'ssnp', 'rationale']].nsmallest(20, 'claude_hi')
print(bot.to_string(index=False))

# ============================================================
# EXPORT
# ============================================================
out_path = OUT_DIR / 'variety_healthiness_module.dta'
df.drop(columns=['rationale']).to_stata(str(out_path), write_index=False)
print(f"\nSaved {out_path}")

quartile_dot('claude_hi_z', 'share_new_upcs',
             r'$\bf{Nutrition}$ of product category',
             r'$\bf{Innovation}$ (share new products in category)',
             'variety_hi_share_new.png')
quartile_dot('claude_hi_z', 'ssnp',
             r'$\bf{Nutrition}$ of product category',
             r'$\bf{Expenditure\ share}$ on new products',
             'variety_hi_ssnp.png')
scatter('fiber_per_100g_z',  'ssnp', 'Module fiber (z-score)',                 'SSNP', 'variety_fiber_ssnp.png')
scatter('sugar_per_100g_z',  'ssnp', 'Module sugar (z-score)',                 'SSNP', 'variety_sugar_ssnp.png')
scatter('satfat_per_100g_z', 'ssnp', 'Module sat fat (z-score)',               'SSNP', 'variety_satfat_ssnp.png')
scatter('sodium_per_100g_z', 'ssnp', 'Module sodium (z-score)',                'SSNP', 'variety_sodium_ssnp.png')

print("\nDone.")
