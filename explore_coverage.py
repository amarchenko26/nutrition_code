"""
Explore nutrition data coverage patterns across product modules and departments.

Graphs:
  1. Coverage rate by department (% of spending with nutrition data)
  2. Coverage rate for top 30 product modules by spending
  4. Mean HI vs coverage rate across modules (selection bias check)
  5. Calorie distribution: matched vs imputed
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DATA_DIR = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
PANEL_PATH    = os.path.join(BASE_DATA_DIR, 'interim', 'hi_panel', 'purchases_with_nutrition.parquet')
OUTPUT_DIR    = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs'

HI_NUTRIENTS = ['fiber_per_100g', 'sugar_per_100g', 'satfat_per_100g',
                'sodium_per_100g', 'chol_per_100g']
HI_DIVISORS  = [29.5, 32.8, 17.2, 2.3, 0.3]
HI_SIGNS     = [1, -1, -1, -1, -1]  # fiber is "good", rest are "bad"


def fmt_dollars(v):
    """Format dollar amount as $X.XM or $XK."""
    if v >= 1e6:
        return f'${v/1e6:.1f}M'
    return f'${v/1e3:.0f}K'


def compute_hi(df):
    """Compute Health Index per row. Returns Series (NaN where any HI nutrient is missing)."""
    hi = pd.Series(0.0, index=df.index)
    for col, divisor, sign in zip(HI_NUTRIENTS, HI_DIVISORS, HI_SIGNS):
        hi += sign * df[col] / divisor
    return hi


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Loading panel...")
    df = pd.read_parquet(PANEL_PATH)
    print(f"  {len(df):,} rows, {df['upc'].nunique():,} UPCs")

    # remove department_desc of nan, of PRODUCE, and all product_module_normalized that have the words "REFERENCE CARD"
    df = df[~df['department_descr'].isna()]
    df = df[df['department_descr'] != 'PRODUCE']
    df = df[df['department_descr'] != 'nan']
    df = df[~df['product_module_normalized'].str.contains('REFERENCE CARD', case=False, na=False)]

    # Classify rows
    has_any_nut = df[HI_NUTRIENTS].notna().any(axis=1)
    is_imputed  = df['imputed'] == 1
    is_matched  = has_any_nut & ~is_imputed

    # Compute HI for all rows that have the 5 nutrients
    df['hi'] = compute_hi(df)
    df.loc[~df[HI_NUTRIENTS].notna().all(axis=1), 'hi'] = np.nan

    # ==================================================================
    # GRAPH 1: Spending share by department — all purchases vs matched
    # ==================================================================
    print("\nPlot 1: Spending shares by department...")
    dept = df.groupby('department_descr').agg(
        total_spending=('total_price_paid', 'sum'),
        matched_spending=('total_price_paid', lambda x: x[is_matched.loc[x.index]].sum()),
    ).sort_values('total_spending', ascending=False)

    grand_total = dept['total_spending'].sum()
    grand_matched = dept['matched_spending'].sum()
    dept['share_all'] = dept['total_spending'] / grand_total * 100
    dept['share_matched'] = dept['matched_spending'] / grand_matched * 100

    fig, ax = plt.subplots(figsize=(10, 6))
    y = np.arange(len(dept))
    bar_h = 0.35
    ax.barh(y - bar_h/2, dept['share_all'], bar_h, label='All Purchases', color='lightsteelblue')
    ax.barh(y + bar_h/2, dept['share_matched'], bar_h, label='Syndigo-Matched', color='steelblue')
    ax.set_yticks(y)
    ax.set_yticklabels(dept.index, fontsize=9)
    ax.set_xlabel('% of Total Spending')
    ax.set_title('Spending Share by Department: All Purchases vs Syndigo-Matched')
    ax.invert_yaxis()
    ax.legend(loc='lower right')
    plt.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, '1_spending_shares_by_department.png'), dpi=150)
    plt.close()
    print("  Saved 1_spending_shares_by_department.png")

    # ==================================================================
    # GRAPH 2: Spending share by top 30 modules — all vs matched
    # ==================================================================
    print("Plot 2: Spending shares for top 30 modules...")
    mod = df.groupby('product_module_normalized').agg(
        total_spending=('total_price_paid', 'sum'),
        matched_spending=('total_price_paid', lambda x: x[is_matched.loc[x.index]].sum()),
    ).sort_values('total_spending', ascending=False)
    top30 = mod.head(30)

    mod_grand_total = mod['total_spending'].sum()
    mod_grand_matched = mod['matched_spending'].sum()
    top30['share_all'] = top30['total_spending'] / mod_grand_total * 100
    top30['share_matched'] = top30['matched_spending'] / mod_grand_matched * 100

    fig, ax = plt.subplots(figsize=(10, 8))
    y = np.arange(len(top30))
    bar_h = 0.35
    ax.barh(y - bar_h/2, top30['share_all'], bar_h, label='All Purchases', color='lightsteelblue')
    ax.barh(y + bar_h/2, top30['share_matched'], bar_h, label='Syndigo-Matched', color='steelblue')
    ax.set_yticks(y)
    ax.set_yticklabels(top30.index, fontsize=8)
    ax.set_xlabel('% of Total Spending')
    ax.set_title('Spending Share by Product Module: All Purchases vs Syndigo-Matched (Top 30)')
    ax.invert_yaxis()
    ax.legend(loc='lower right')
    plt.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, '2_spending_shares_top30_modules.png'), dpi=150)
    plt.close()
    print("  Saved 2_spending_shares_top30_modules.png")

    # ==================================================================
    # GRAPH 4: Mean HI vs coverage rate across modules
    # ==================================================================
    print("Plot 4: HI vs coverage across modules...")
    # UPC-level stats per module
    upc_level = df.drop_duplicates(subset='upc')
    module_dept = upc_level.groupby('product_module_normalized')['department_descr'].agg(
        lambda s: s.mode().iloc[0] if not s.mode().empty else 'UNKNOWN')
    mod_hi = upc_level.groupby('product_module_normalized').agg(
        mean_hi=('hi', 'mean'),
        n_upcs=('upc', 'nunique'),
        n_with_hi=('hi', lambda x: x.notna().sum()),
    )
    mod_hi = mod_hi.join(module_dept.rename('department_descr'), how='left')
    mod_hi['coverage'] = mod_hi['n_with_hi'] / mod_hi['n_upcs'] * 100
    # Only plot modules with at least 10 UPCs and some HI data
    plot_mods = mod_hi[(mod_hi['n_upcs'] >= 10) & (mod_hi['n_with_hi'] >= 3)].copy()

    fig, ax = plt.subplots(figsize=(8, 6))
    # Color by department; keep legend readable by collapsing small groups into "Other".
    top_depts = plot_mods['department_descr'].value_counts().head(8).index
    plot_mods['department_plot'] = np.where(
        plot_mods['department_descr'].isin(top_depts),
        plot_mods['department_descr'],
        'Other'
    )

    dept_order = sorted(plot_mods['department_plot'].dropna().unique())
    palette = plt.cm.tab20(np.linspace(0, 1, len(dept_order)))
    dept_colors = dict(zip(dept_order, palette))

    for dept in dept_order:
        d = plot_mods[plot_mods['department_plot'] == dept]
        ax.scatter(
            d['coverage'], d['mean_hi'],
            s=np.sqrt(d['n_upcs']) * 3,
            alpha=0.55,
            color=dept_colors[dept],
            label=dept
        )

    # Label a small subset of outlier modules (extreme HI/coverage and high distance from center).
    valid_points = plot_mods[['coverage', 'mean_hi']].dropna().copy()
    if len(valid_points) > 0:
        cov_med = valid_points['coverage'].median()
        hi_med = valid_points['mean_hi'].median()
        cov_scale = (valid_points['coverage'] - cov_med).abs().median()
        hi_scale = (valid_points['mean_hi'] - hi_med).abs().median()
        if cov_scale == 0 or np.isnan(cov_scale):
            cov_scale = valid_points['coverage'].std() if valid_points['coverage'].std() > 0 else 1
        if hi_scale == 0 or np.isnan(hi_scale):
            hi_scale = valid_points['mean_hi'].std() if valid_points['mean_hi'].std() > 0 else 1

        valid_points['outlier_score'] = (
            (valid_points['coverage'] - cov_med).abs() / cov_scale
            + (valid_points['mean_hi'] - hi_med).abs() / hi_scale
        )

        candidate_idx = pd.Index([
            valid_points['mean_hi'].idxmin(),
            valid_points['mean_hi'].idxmax(),
            valid_points['coverage'].idxmin(),
            valid_points['coverage'].idxmax(),
        ]).append(valid_points['outlier_score'].nlargest(12).index).drop_duplicates()[:12]

        for i, mod_name in enumerate(candidate_idx):
            row = plot_mods.loc[mod_name]
            label = mod_name if len(mod_name) <= 30 else mod_name[:27] + '...'
            dx = 4 if i % 2 == 0 else -4
            dy = 4 if i % 3 == 0 else -5
            ax.annotate(
                label,
                (row['coverage'], row['mean_hi']),
                xytext=(dx, dy),
                textcoords='offset points',
                fontsize=7,
                alpha=0.9
            )

    ax.axhline(0, color='gray', linewidth=0.5, linestyle='--')
    ax.set_xlabel('Coverage Rate (% of UPCs with HI)')
    ax.set_ylabel('Mean Health Index')
    ax.set_title('Nutrition Coverage vs Mean HI Across Product Modules')
    ax.legend(title='Department', fontsize=7, title_fontsize=8,
              loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0)
    # Correlation
    valid = plot_mods[['coverage', 'mean_hi']].dropna()
    if len(valid) > 2:
        corr = valid['coverage'].corr(valid['mean_hi'])
        ax.text(0.05, 0.95, f'r = {corr:.3f} (n={len(valid)} modules)',
                transform=ax.transAxes, fontsize=10, va='top')
    plt.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, '4_hi_vs_coverage.png'), dpi=150)
    plt.close()
    print("  Saved 4_hi_vs_coverage.png")

    # ==================================================================
    # GRAPH 5: Calorie distribution — matched vs imputed
    #   (a) all UPCs, (b) top 50% by spending — spending-weighted KDEs
    # ==================================================================
    from scipy import stats as sp_stats

    print("Plot 5: Calorie distribution matched vs imputed...")

    # UPC-level: aggregate spending per UPC, keep calorie and imputed flag
    upc_spend = (df.groupby('upc')
                   .agg(spending=('total_price_paid', 'sum'),
                        cal=('cal_per_100g', 'first'),
                        imputed=('imputed', 'first'))
                   .dropna(subset=['cal']))
    upc_spend = upc_spend[upc_spend['cal'].between(0, 1500)]  # trim outliers

    matched = upc_spend[upc_spend['imputed'] == 0]
    imputed = upc_spend[upc_spend['imputed'] == 1]

    # Top 75% of UPCs by spending (within each group)
    matched_top = matched[matched['spending'] >= matched['spending'].quantile(0.5)]
    imputed_top = imputed[imputed['spending'] >= imputed['spending'].quantile(0.5)]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)

    for ax, m, imp, title_suffix in [
        (axes[0], matched, imputed, 'All UPCs'),
        (axes[1], matched_top, imputed_top, 'Top 50% UPCs by Spending'),
    ]:
        # Spending-weighted KDE via histogram with weights
        bins = np.linspace(0, 1500, 60)
        ax.hist(m['cal'], bins=bins, weights=m['spending'], density=True,
                alpha=0.5, color='steelblue', label=f'Matched (n={len(m):,})')
        ax.hist(imp['cal'], bins=bins, weights=imp['spending'], density=True,
                alpha=0.5, color='coral', label=f'Imputed (n={len(imp):,})')

        # Spending-weighted means
        m_wmean = np.average(m['cal'], weights=m['spending'])
        i_wmean = np.average(imp['cal'], weights=imp['spending'])

        # KS test (unweighted — tests distributional difference)
        ks_stat, ks_p = sp_stats.ks_2samp(m['cal'], imp['cal'])

        ax.set_xlabel('Calories per 100g')
        ax.set_title(f'{title_suffix} (spending-weighted)')
        ax.legend(fontsize=8, loc='upper right')
        ax.text(0.95, 0.55,
                f'Matched wt. mean: {m_wmean:.0f}  sd: {m["cal"].std():.0f}\n'
                f'Imputed wt. mean: {i_wmean:.0f}  sd: {imp["cal"].std():.0f}\n'
                f'Matched median: {m["cal"].median():.0f}\n'
                f'Imputed median: {imp["cal"].median():.0f}\n'
                f'KS stat: {ks_stat:.3f} (p={ks_p:.2e})',
                transform=ax.transAxes, fontsize=8, va='top', ha='right',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    axes[0].set_ylabel('Density (spending-weighted)')
    plt.tight_layout()
    fig.savefig(os.path.join(OUTPUT_DIR, '5_calories_matched_vs_imputed.png'), dpi=150)
    plt.close()
    print("  Saved 5_calories_matched_vs_imputed.png")

    print(f"\nAll figures saved to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
