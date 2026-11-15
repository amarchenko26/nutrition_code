"""
build_monthly_cycle_panel.py

Two within-HH analyses of income timing on food purchase quality:

  1. EITC annual cycle (within-HH):
     - Compare March purchases to January for EITC-eligible HHs.
     - Time series: daily spend and healthy/produce/unhealthy share from
       Feb 15 to Mar 15 (captures the refund spike).
     - DiD spec for Stata: Y_{hm} = β (month==March) + HH FE + year FE + ε

  2. SNAP within-month cycle (within-HH):
     - Benefits arrive days 1-15, staggered by case number/SSN across states.
       Most recipients get benefits in days 1-7 (flush window).
       Days 22-31 = depleted window.
     - Time series: average daily spend / healthy share by day-of-month.
     - DiD spec for Stata: Y_{hd} = β (week==4 vs week==1) + HH FE +
       year-month FE + ε

No external control group. Identification is entirely within-HH.

Memory strategy: aggregate to HH x date (day-level) inside the year loop
so we never hold all 600M+ raw rows in memory simultaneously.

Bug fixed vs. prior version: aggregating by *week* inflated week-4 totals
because week 4 covers 10 days (days 22-31) vs. 7 days for weeks 1-3.
Fix: aggregate to HH x day first, then derive week averages from per-day obs.

Outputs:
  - interim/panel_dataset/monthly_cycle_hh_day.parquet  (HH x date)
  - figs/eitc_daily_spend.png, eitc_daily_healthy.png
  - figs/snap_daily_spend.png, snap_daily_healthy.png
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ============================================================================
# PATHS
# ============================================================================

import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
BASE          = str(data)
PURCHASES_DIR = os.path.join(BASE, 'interim', 'purchases_food')
PANELISTS_PATH= os.path.join(BASE, 'interim', 'panelists', 'panelists_all_years.parquet')
CLAUDE_HI_PATH= os.path.join(BASE, 'interim', 'rms_variety', 'claude_hi_scores.parquet')
OUT_DIR       = os.path.join(BASE, 'interim', 'panel_dataset')
FIG_DIR       = str(fdir)

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(FIG_DIR, exist_ok=True)

HEALTHY_THRESHOLD   = 7
UNHEALTHY_THRESHOLD = 3
PRODUCE_GROUP_KEYWORDS = ['FRESH', 'PRODUCE', 'VEGETABLE', 'FRUIT']

# ============================================================================
# STEP 1: Load panelists and define eligibility
# ============================================================================

print("=" * 70)
print("STEP 1: Panelists and eligibility")
print("=" * 70)

panelists = pd.read_parquet(PANELISTS_PATH, columns=[
    'household_code', 'panel_year',
    'household_income_midpoint', 'household_size',
    'wic_indicator_current', 'age_and_presence_of_children',
    'projection_factor',
])
panelists['household_code']            = pd.to_numeric(panelists['household_code'], errors='coerce')
panelists['panel_year']                = pd.to_numeric(panelists['panel_year'], errors='coerce')
panelists['household_size']            = pd.to_numeric(panelists['household_size'], errors='coerce').clip(lower=1)
panelists['household_income_midpoint'] = pd.to_numeric(panelists['household_income_midpoint'], errors='coerce')
panelists['projection_factor']         = pd.to_numeric(panelists['projection_factor'], errors='coerce')

panelists['income_per_capita'] = panelists['household_income_midpoint'] / panelists['household_size']

# SNAP eligible: per-capita income <= 130% FPL (~$13k/person)
panelists['snap_eligible'] = (panelists['income_per_capita'] <= 13_000).astype('Int8')

# EITC eligible: has children + HH income <= $55k
panelists['has_children'] = (
    pd.to_numeric(panelists['age_and_presence_of_children'], errors='coerce') != 9
).astype('Int8')
panelists['eitc_eligible'] = (
    (panelists['has_children'] == 1) &
    (panelists['household_income_midpoint'] <= 55_000)
).astype('Int8')

# WIC
panelists['wic_current'] = (
    pd.to_numeric(panelists['wic_indicator_current'], errors='coerce') == 1
).astype('Int8')

print(f"Panelist-years: {len(panelists):,}")
print(f"  SNAP eligible: {panelists['snap_eligible'].sum():,}")
print(f"  EITC eligible: {panelists['eitc_eligible'].sum():,}")
print(f"  WIC current:   {panelists['wic_current'].sum():,}")

# Keep SNAP or EITC eligible only (no high-income control)
eligible = panelists[
    (panelists['snap_eligible'] == 1) | (panelists['eitc_eligible'] == 1)
].copy()

eligible['_key'] = eligible['household_code'].astype(str) + '_' + eligible['panel_year'].astype(str)
eligible_key_set = set(eligible['_key'])

panelists_slim = eligible[[
    'household_code', 'panel_year',
    'snap_eligible', 'eitc_eligible', 'wic_current', 'has_children',
    'household_income_midpoint', 'projection_factor',
]].copy()

print(f"\nEligible HH-years: {len(eligible):,}")

# ============================================================================
# STEP 2: Module healthiness scores
# ============================================================================

print("\n" + "=" * 70)
print("STEP 2: Module healthiness scores")
print("=" * 70)

hi_scores = pd.read_parquet(CLAUDE_HI_PATH, columns=[
    'product_module_code', 'claude_hi', 'product_group_descr',
])
hi_scores['product_module_code'] = pd.to_numeric(hi_scores['product_module_code'], errors='coerce')
hi_scores['is_healthy']   = (hi_scores['claude_hi'] >= HEALTHY_THRESHOLD).astype('Int8')
hi_scores['is_unhealthy'] = (hi_scores['claude_hi'] <= UNHEALTHY_THRESHOLD).astype('Int8')
hi_scores['is_produce']   = hi_scores['product_group_descr'].apply(
    lambda g: any(kw in str(g).upper() for kw in PRODUCE_GROUP_KEYWORDS)
).astype('Int8')
hi_scores = hi_scores.drop(columns=['product_group_descr'])
print(f"  {len(hi_scores)} modules loaded")

# ============================================================================
# STEP 3+4: Year loop — filter, merge healthiness, aggregate to HH x date
# ============================================================================

print("\n" + "=" * 70)
print("STEP 3+4: Load year-by-year, aggregate to HH x date")
print("=" * 70)

KEEP_COLS = [
    'household_code', 'purchase_date',
    'product_module_code',
    'total_price_paid_real_2013',
    'product_group',
]

years = sorted([
    int(d.replace('panel_year=', ''))
    for d in os.listdir(PURCHASES_DIR)
    if d.startswith('panel_year=')
])

agg_chunks = []

for year in years:
    path = os.path.join(PURCHASES_DIR, f'panel_year={year}')
    if not os.path.exists(path):
        continue

    df = pd.read_parquet(path, columns=[c for c in KEEP_COLS if c in
                         pd.read_parquet(path, columns=[]).columns or True])
    df['panel_year']     = year
    df['household_code'] = pd.to_numeric(df['household_code'], errors='coerce')

    # Filter to eligible HHs
    df['_key'] = df['household_code'].astype(str) + '_' + str(year)
    df = df[df['_key'].isin(eligible_key_set)].drop(columns=['_key'])
    if len(df) == 0:
        continue

    # Merge healthiness
    df['product_module_code'] = pd.to_numeric(df['product_module_code'], errors='coerce')
    df = df.merge(hi_scores, on='product_module_code', how='left')

    # Produce fallback from product_group
    if 'product_group' in df.columns:
        produce_fb = df['product_group'].apply(
            lambda g: any(kw in str(g).upper() for kw in PRODUCE_GROUP_KEYWORDS)
        )
        df['is_produce'] = (df['is_produce'].fillna(0).astype(int) | produce_fb.astype(int))

    df['is_healthy']   = df['is_healthy'].fillna(0).astype(int)
    df['is_unhealthy'] = df['is_unhealthy'].fillna(0).astype(int)
    df['is_produce']   = df['is_produce'].fillna(0).astype(int)

    spend = df['total_price_paid_real_2013'].fillna(0)
    df['spend_healthy']   = spend.where(df['is_healthy']   == 1, 0)
    df['spend_unhealthy'] = spend.where(df['is_unhealthy'] == 1, 0)
    df['spend_produce']   = spend.where(df['is_produce']   == 1, 0)

    # Aggregate to HH x purchase_date (daily)
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])
    agg = df.groupby(['household_code', 'panel_year', 'purchase_date']).agg(
        spend_total     = ('total_price_paid_real_2013', 'sum'),
        spend_healthy   = ('spend_healthy',   'sum'),
        spend_unhealthy = ('spend_unhealthy', 'sum'),
        spend_produce   = ('spend_produce',   'sum'),
        n_items         = ('total_price_paid_real_2013', 'count'),
    ).reset_index()

    agg_chunks.append(agg)
    print(f"  {year}: {len(df):,} rows -> {len(agg):,} HH-day obs")
    del df

print(f"\nConcatenating {len(agg_chunks)} year chunks...")
panel = pd.concat(agg_chunks, ignore_index=True)
print(f"Daily panel: {len(panel):,} HH-day obs")

# ============================================================================
# STEP 5: Merge eligibility flags + derive time variables
# ============================================================================

print("\n" + "=" * 70)
print("STEP 5: Merge flags and derive time variables")
print("=" * 70)

panel = panel.merge(panelists_slim, on=['household_code', 'panel_year'], how='inner')

panel['month']       = panel['purchase_date'].dt.month
panel['day_of_month']= panel['purchase_date'].dt.day
panel['year']        = panel['purchase_date'].dt.year

# Week of month — divides EQUALLY: wk1=days1-7, wk2=8-14, wk3=15-21, wk4=22+
panel['week_of_month'] = ((panel['day_of_month'] - 1) // 7 + 1).clip(upper=4)

# EITC season flag
panel['eitc_season'] = panel['month'].isin([2, 3]).astype(int)

# Share outcomes — compute at the HH-day level
panel['healthy_share']   = panel['spend_healthy']   / panel['spend_total'].replace(0, np.nan)
panel['unhealthy_share'] = panel['spend_unhealthy'] / panel['spend_total'].replace(0, np.nan)
panel['produce_share']   = panel['spend_produce']   / panel['spend_total'].replace(0, np.nan)

# Save full daily panel for Stata
out_path = os.path.join(OUT_DIR, 'monthly_cycle_hh_day.parquet')
panel.to_parquet(out_path, index=False)
print(f"Saved: {out_path}")

# ============================================================================
# STEP 6: EITC plots
#
# Key diagnostic question: when the refund hits, does healthy spending rise
# in ABSOLUTE DOLLARS even if the share falls? A falling share with a rising
# level means total spending spiked faster (splurge effect). A falling share
# with a flat or falling level is a different story.
#
# Two figures:
#   (a) Wide monthly view: Jan–Apr, all outcomes, to locate where the spike
#       actually sits (early filers get refunds in late January / early Feb).
#   (b) Narrow daily view: Jan 15 – Apr 15, showing BOTH absolute levels and
#       shares side by side so the share / level distinction is visible.
# ============================================================================

print("\n" + "=" * 70)
print("STEP 6: EITC plots")
print("=" * 70)

plt.rcParams.update({'font.size': 11, 'figure.dpi': 150})

eitc = panel[panel['eitc_eligible'] == 1].copy()

# ---- (a) Wide monthly bar chart: Jan–Apr -----------------------------------
eitc_monthly = (
    eitc[eitc['month'].isin([1, 2, 3, 4])]
    .groupby('month')
    .agg(
        spend_total     = ('spend_total',     'mean'),
        spend_healthy   = ('spend_healthy',   'mean'),
        spend_unhealthy = ('spend_unhealthy', 'mean'),
        spend_produce   = ('spend_produce',   'mean'),
        healthy_share   = ('healthy_share',   'mean'),
        unhealthy_share = ('unhealthy_share', 'mean'),
        produce_share   = ('produce_share',   'mean'),
    )
    .reset_index()
)

mlabels = ['Jan\n(baseline)', 'Feb\n(early refunds)', 'Mar\n(peak refunds)', 'Apr\n(post-refund)']
x = np.arange(4)
bar_colors = ['steelblue', 'gold', 'firebrick', 'steelblue']

fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))

# Panel 1: total spend
axes[0].bar(x, eitc_monthly['spend_total'], color=bar_colors, alpha=0.85, edgecolor='white')
axes[0].set_xticks(x); axes[0].set_xticklabels(mlabels, fontsize=8)
axes[0].set_ylabel('Mean daily real spending (2013$)')
axes[0].set_title('Total grocery spend')

# Panel 2: absolute $ on healthy / unhealthy / produce
axes[1].bar(x, eitc_monthly['spend_healthy'],   color='steelblue', alpha=0.8, label='Healthy ($)')
axes[1].bar(x, eitc_monthly['spend_unhealthy'], color='firebrick', alpha=0.6,
            bottom=eitc_monthly['spend_healthy'], label='Unhealthy ($)')
axes[1].bar(x - 0.3, eitc_monthly['spend_produce'], width=0.2, color='green',
            alpha=0.9, label='Produce ($)')
axes[1].set_xticks(x); axes[1].set_xticklabels(mlabels, fontsize=8)
axes[1].set_ylabel('Mean daily real spending (2013$)')
axes[1].set_title('Absolute spend: healthy vs unhealthy')
axes[1].legend(fontsize=8)

# Panel 3: shares
axes[2].plot(x, eitc_monthly['healthy_share'],   marker='o', color='steelblue', label='Healthy share')
axes[2].plot(x, eitc_monthly['unhealthy_share'], marker='s', color='firebrick', label='Unhealthy share')
axes[2].plot(x, eitc_monthly['produce_share'],   marker='^', color='green',    label='Produce share')
axes[2].set_xticks(x); axes[2].set_xticklabels(mlabels, fontsize=8)
axes[2].set_ylabel('Share of spending')
axes[2].set_title('Shares (healthy / unhealthy / produce)')
axes[2].legend(fontsize=8)

plt.suptitle('EITC-eligible HHs: Jan–Apr spending (within-HH, raw means)', fontsize=11)
plt.tight_layout()
path = os.path.join(FIG_DIR, 'eitc_monthly_comparison.png')
fig.savefig(path, dpi=200, bbox_inches='tight')
plt.close(fig)
print(f"  Saved: {path}")

# ---- (b) Daily time series: Jan 15 – Apr 15 --------------------------------
# Align all years to reference year 2000 for averaging.
# Wide window so we can see where the spike sits relative to Jan baseline.

eitc_wide = eitc[eitc['month'].isin([1, 2, 3, 4])].copy()
eitc_wide['ref_date'] = eitc_wide['purchase_date'].apply(lambda d: d.replace(year=2000))

jan15 = pd.Timestamp('2000-01-15')
apr15 = pd.Timestamp('2000-04-15')
eitc_wide = eitc_wide[(eitc_wide['ref_date'] >= jan15) & (eitc_wide['ref_date'] <= apr15)]

daily_eitc = (
    eitc_wide.groupby('ref_date')
    .agg(
        spend_total     = ('spend_total',     'mean'),
        spend_healthy   = ('spend_healthy',   'mean'),
        spend_unhealthy = ('spend_unhealthy', 'mean'),
        spend_produce   = ('spend_produce',   'mean'),
        healthy_share   = ('healthy_share',   'mean'),
        unhealthy_share = ('unhealthy_share', 'mean'),
        produce_share   = ('produce_share',   'mean'),
    )
    .reset_index()
    .sort_values('ref_date')
)

# 5-day rolling mean (wider window = smoother signal over noisy daily data)
for col in daily_eitc.columns[1:]:
    daily_eitc[col + '_s'] = daily_eitc[col].rolling(5, center=True, min_periods=2).mean()

def add_eitc_bands(ax, xlabel=False):
    ax.axvspan(pd.Timestamp('2000-02-01'), pd.Timestamp('2000-02-28'),
               alpha=0.10, color='gold')
    ax.axvspan(pd.Timestamp('2000-03-01'), pd.Timestamp('2000-03-31'),
               alpha=0.12, color='tomato')
    ax.axvline(pd.Timestamp('2000-03-03'), color='firebrick', linestyle='--', linewidth=1.1)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
    if xlabel:
        plt.setp(ax.get_xticklabels(), rotation=30, ha='right', fontsize=8)
    else:
        ax.set_xticklabels([])

# Figure: 7 subplots, each on its own y-axis (4 levels + 3 shares)
# Sharing x-axis so the date bands line up perfectly.
panels = [
    ('spend_total_s',     'Total spend ($)',     'black'),
    ('spend_healthy_s',   'Healthy spend ($)',   'steelblue'),
    ('spend_unhealthy_s', 'Unhealthy spend ($)', 'firebrick'),
    ('spend_produce_s',   'Produce spend ($)',   'green'),
    ('healthy_share_s',   'Healthy share',       'steelblue'),
    ('unhealthy_share_s', 'Unhealthy share',     'firebrick'),
    ('produce_share_s',   'Produce share',       'green'),
]

fig, axes = plt.subplots(len(panels), 1, figsize=(11, 2.2 * len(panels)), sharex=True)

for i, (col, ylabel, color) in enumerate(panels):
    ax = axes[i]
    is_last = (i == len(panels) - 1)
    ax.plot(daily_eitc['ref_date'], daily_eitc[col], color=color, linewidth=1.8)
    add_eitc_bands(ax, xlabel=is_last)
    ax.set_ylabel(ylabel, fontsize=9)
    # Tight y-limits so small variation is visible
    yvals = daily_eitc[col].dropna()
    pad = (yvals.max() - yvals.min()) * 0.4 or yvals.mean() * 0.02
    ax.set_ylim(yvals.min() - pad, yvals.max() + pad)
    ax.yaxis.set_major_locator(plt.MaxNLocator(4))
    ax.tick_params(axis='y', labelsize=8)
    # Divider between levels and shares
    if i == 3:
        ax.spines['bottom'].set_linewidth(2)

# Legend as a single annotation in the figure margin
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
legend_elements = [
    Patch(facecolor='gold',   alpha=0.5, label='Feb (early refunds)'),
    Patch(facecolor='tomato', alpha=0.5, label='Mar (peak refunds)'),
    Line2D([0], [0], color='firebrick', linestyle='--', label='IRS target (~Mar 3)'),
]
fig.legend(handles=legend_elements, loc='upper right', fontsize=8,
           bbox_to_anchor=(0.99, 0.99), framealpha=0.9)

fig.suptitle(
    'EITC-eligible HHs: daily spending Jan 15 – Apr 15  (5-day rolling mean)\n'
    'Top 4 panels = absolute $; bottom 3 = shares — each on its own y-axis',
    fontsize=10, y=1.01
)
plt.tight_layout()
path = os.path.join(FIG_DIR, 'eitc_daily_spend.png')
fig.savefig(path, dpi=200, bbox_inches='tight')
plt.close(fig)
print(f"  Saved: {path}")

# ============================================================================
# STEP 7: SNAP within-month plots
#   - Average daily spend and healthy share by day-of-month
#   - Normalize: use mean spend per shopping day (not total), so day count
#     differences across weeks don't inflate week 4.
# ============================================================================

print("\n" + "=" * 70)
print("STEP 7: SNAP within-month plots")
print("=" * 70)

snap = panel[panel['snap_eligible'] == 1].copy()

# Average across HHs and year-months, by day-of-month
# This is already per-HH-per-day, so averaging gives mean daily spend
snap_daily = snap.groupby('day_of_month').agg(
    spend_total     = ('spend_total',     'mean'),
    healthy_share   = ('healthy_share',   'mean'),
    unhealthy_share = ('unhealthy_share', 'mean'),
    produce_share   = ('produce_share',   'mean'),
    n_obs           = ('spend_total',     'count'),
).reset_index().sort_values('day_of_month')

# Smooth with 3-day rolling mean
for col in ['spend_total', 'healthy_share', 'unhealthy_share', 'produce_share']:
    snap_daily[col + '_smooth'] = snap_daily[col].rolling(3, center=True, min_periods=1).mean()

# Plot 1: daily spend by day-of-month
fig, ax = plt.subplots(figsize=(9, 4))
ax.plot(snap_daily['day_of_month'], snap_daily['spend_total_smooth'],
        color='firebrick', linewidth=2)
ax.axvspan(1, 7,   alpha=0.12, color='green', label='Flush window (days 1-7)')
ax.axvspan(22, 31, alpha=0.10, color='red',   label='Depletion window (days 22-31)')
ax.axvline(10, color='gray', linestyle=':', linewidth=1,
           label='Day 10 (most states done issuing)')
ax.set_xlabel('Day of month')
ax.set_ylabel('Mean real spending per shopping day (2013$)')
ax.set_title('SNAP-eligible HHs: grocery spending by day of month')
ax.set_xticks([1, 7, 10, 15, 22, 31])
ax.legend(fontsize=9)
plt.tight_layout()
path = os.path.join(FIG_DIR, 'snap_daily_spend.png')
fig.savefig(path, dpi=200, bbox_inches='tight')
plt.close(fig)
print(f"  Saved: {path}")

# Plot 2: healthy / produce / unhealthy share by day-of-month
fig, axes = plt.subplots(1, 3, figsize=(13, 4))
for ax, col, color, title in zip(
    axes,
    ['healthy_share_smooth', 'produce_share_smooth', 'unhealthy_share_smooth'],
    ['steelblue', 'green', 'firebrick'],
    ['Healthy share', 'Produce share', 'Unhealthy share'],
):
    ax.plot(snap_daily['day_of_month'], snap_daily[col], color=color, linewidth=2)
    ax.axvspan(1, 7,   alpha=0.12, color='green')
    ax.axvspan(22, 31, alpha=0.10, color='red')
    ax.axvline(10, color='gray', linestyle=':', linewidth=1)
    ax.set_xlabel('Day of month')
    ax.set_title(title)
    ax.set_xticks([1, 7, 10, 15, 22, 31])

axes[0].set_ylabel('Share of spending')
plt.suptitle('SNAP-eligible HHs: purchase quality by day of month', fontsize=11)
plt.tight_layout()
path = os.path.join(FIG_DIR, 'snap_daily_healthy.png')
fig.savefig(path, dpi=200, bbox_inches='tight')
plt.close(fig)
print(f"  Saved: {path}")

# ============================================================================
# STEP 8: Raw DiD summary stats (descriptive — no FE)
# ============================================================================

print("\n" + "=" * 70)
print("STEP 8: Raw DiD statistics")
print("=" * 70)

print("\n--- SNAP: week 1 vs week 4 ---")
for outcome in ['spend_total', 'healthy_share', 'produce_share']:
    w1 = snap[snap['week_of_month'] == 1][outcome].mean()
    w4 = snap[snap['week_of_month'] == 4][outcome].mean()
    print(f"  {outcome}: week1={w1:.4f}, week4={w4:.4f}, diff={w1-w4:.4f}")

print("\n--- EITC: March vs January ---")
for outcome in ['spend_total', 'healthy_share', 'produce_share']:
    jan = eitc[eitc['month'] == 1][outcome].mean()
    mar = eitc[eitc['month'] == 3][outcome].mean()
    print(f"  {outcome}: jan={jan:.4f}, mar={mar:.4f}, diff={mar-jan:.4f}")

print("\nDone. Run stata/build_monthly_cycle_regs.do for within-HH regressions.")
