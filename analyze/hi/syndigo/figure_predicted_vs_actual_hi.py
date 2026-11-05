"""
3-panel figure: Predicted vs. Actual Change Over Time
for whole grain share, produce share, and sodium per 1000 cal.

Cross-sectional slope β estimated from HMS panel (WLS, controlling for age bins,
year FE, household size). Predicted trajectory uses Census CPS/ASEC real median
household income (not HMS panel income) to avoid panel composition bias.

    outcome_predicted_t = outcome_2004 + β × (log(census_income_t) - log(census_income_2004))
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
FIG_DIR = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf/figs')

BASELINE_YEAR = 2004
YEARS         = range(2004, 2021)

PANELS = [
    # (column, y-axis label, title, higher_is_better)
    ('whole',              'Share bread calories from whole grains',   'Whole Grain Adoption',    True),
    ('produce',            'Calorie share from fruit & vegetables',    'Produce Consumption',     True),
    ('sodium_per_1000cal', 'Sodium (g per 1,000 Cal)',                 'Sodium Consumption',      False),
]

# ============================================================
# CENSUS CPS/ASEC REAL MEDIAN HOUSEHOLD INCOME
# Source: Census Bureau Historical Income Table H-6
# Units: thousands of 2020 CPI-U-RS adjusted dollars
# ============================================================
CENSUS_INCOME_K = {
    2004: 61.1, 2005: 62.2, 2006: 63.3, 2007: 64.5,
    2008: 61.5, 2009: 58.6, 2010: 57.7, 2011: 56.5,
    2012: 56.8, 2013: 57.5, 2014: 57.7, 2015: 60.0,
    2016: 62.9, 2017: 64.0, 2018: 65.7, 2019: 68.7,
    2020: 67.5,
}
census = pd.DataFrame({'year': list(CENSUS_INCOME_K.keys()),
                       'census_income_k': list(CENSUS_INCOME_K.values())})
census['log_census'] = np.log(census['census_income_k'])

# ============================================================
# LOAD & FILTER HMS PANEL
# ============================================================
log("Loading dataset...")
hhy = pd.read_parquet(DATASET)
hhy = hhy[hhy['panel_year'].isin(YEARS)].copy()
log(f"  {len(hhy):,} HH-year obs")

p1, p99 = hhy['hh_real_income_avg'].quantile(0.01), hhy['hh_real_income_avg'].quantile(0.99)
hhy = hhy[(hhy['hh_real_income_avg'] >= p1) & (hhy['hh_real_income_avg'] <= p99)].copy()
hhy = hhy[hhy['hh_real_income_avg'] > 0].copy()
hhy['log_income'] = np.log(hhy['hh_real_income_avg'])

# Controls: age bins, year FE, household size
hhy['age_bin'] = pd.cut(hhy['avg_age_hh_head'], bins=[0, 35, 45, 55, 65, 100], labels=False)
age_dum = pd.get_dummies(hhy['age_bin'],    prefix='a', drop_first=True, dtype=float)
yr_dum  = pd.get_dummies(hhy['panel_year'], prefix='y', drop_first=True, dtype=float)
hhy = pd.concat([hhy.reset_index(drop=True),
                 age_dum.reset_index(drop=True),
                 yr_dum.reset_index(drop=True)], axis=1)
ctl_cols = list(age_dum.columns) + list(yr_dum.columns) + ['household_size']

# ============================================================
# HELPER: cross-sectional slope (WLS, Frisch-Waugh)
# ============================================================
def estimate_beta(df, yvar):
    keep = [yvar, 'log_income', 'projection_factor'] + ctl_cols
    d  = df[keep].dropna().copy()
    y  = d[yvar].values
    x  = d['log_income'].values
    w  = d['projection_factor'].values
    C  = d[ctl_cols].values
    sw = np.sqrt(w)
    def wls_resid(dep, exog, sw):
        X = np.column_stack([np.ones(len(dep)), exog])
        b, _, _, _ = np.linalg.lstsq(X * sw[:, None], dep * sw, rcond=None)
        return dep - X @ b
    yr = wls_resid(y, C, sw)
    xr = wls_resid(x, C, sw)
    return np.average(xr * yr, weights=w) / np.average(xr ** 2, weights=w)

# ============================================================
# BUILD TIME SERIES FOR EACH PANEL
# ============================================================
def build_ts(df, yvar):
    rows = []
    for year in sorted(df['panel_year'].unique()):
        sub = df[df['panel_year'] == year].dropna(subset=[yvar, 'projection_factor'])
        if len(sub) < 50:
            continue
        rows.append({'year': year,
                     'actual': np.average(sub[yvar], weights=sub['projection_factor'])})
    ts = pd.DataFrame(rows).sort_values('year').reset_index(drop=True)
    ts = ts.merge(census, on='year', how='left')

    base_actual     = ts.loc[ts['year'] == BASELINE_YEAR, 'actual'].values[0]
    base_log_census = ts.loc[ts['year'] == BASELINE_YEAR, 'log_census'].values[0]
    beta            = estimate_beta(df, yvar)

    ts['delta_log_census'] = ts['log_census'] - base_log_census
    ts['predicted']        = base_actual + beta * ts['delta_log_census']
    ts['cum_pct_income']   = (ts['delta_log_census'] * 100).round(1)
    return ts, beta, base_actual

# ============================================================
# FIGURE: 3 panels side by side
# ============================================================
log("Building figure...")
fig, axes = plt.subplots(1, 3, figsize=(16, 5.5), sharey=False)

for ax, (yvar, ylabel, title, higher_is_better) in zip(axes, PANELS):
    ts, beta, base_actual = build_ts(hhy, yvar)
    log(f"  {yvar}: β={beta:.4f}, actual Δ={ts['actual'].iloc[-1]-ts['actual'].iloc[0]:+.4f}, "
        f"predicted Δ={ts['predicted'].iloc[-1]-ts['predicted'].iloc[0]:+.4f}")

    # Lines
    ax.plot(ts['year'], ts['predicted'], color='#e34a33', linewidth=2,
            linestyle='--', marker='o', markersize=4,
            label=f'Predicted (β={beta:.3f})')
    ax.plot(ts['year'], ts['actual'], color='#2c5f8a', linewidth=2.2,
            linestyle='-', marker='s', markersize=4,
            label='Actual')

    # Shade gap: green where actual beats prediction, orange where it falls short
    if higher_is_better:
        ax.fill_between(ts['year'], ts['predicted'], ts['actual'],
                        where=(ts['actual'] >= ts['predicted']),
                        color='#74add1', alpha=0.18)
        ax.fill_between(ts['year'], ts['predicted'], ts['actual'],
                        where=(ts['actual'] < ts['predicted']),
                        color='#fc8d59', alpha=0.18)
    else:
        # Lower is better (sodium): shade where actual < predicted as improvement
        ax.fill_between(ts['year'], ts['predicted'], ts['actual'],
                        where=(ts['actual'] <= ts['predicted']),
                        color='#74add1', alpha=0.18)
        ax.fill_between(ts['year'], ts['predicted'], ts['actual'],
                        where=(ts['actual'] > ts['predicted']),
                        color='#fc8d59', alpha=0.18)

    ax.axvline(BASELINE_YEAR, color='gray', linewidth=0.7, linestyle=':', alpha=0.6)

    ax.set_xlabel('Year', fontsize=11)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.set_title(title, fontsize=13, fontweight='bold')
    ax.legend(fontsize=9, framealpha=0.9, loc='upper left')
    ax.grid(True, alpha=0.25, axis='y')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.set_xticks([2004, 2008, 2012, 2016, 2020])
    ax.tick_params(axis='x', labelsize=9)

    # Bottom annotation
    last = ts.iloc[-1]
    da   = last['actual'] - ts.iloc[0]['actual']
    dp   = last['predicted'] - ts.iloc[0]['predicted']
    ax.text(0.03, 0.03,
            f"Actual Δ: {da:+.4f}\nPredicted Δ: {dp:+.4f}",
            transform=ax.transAxes, fontsize=8, color='gray', va='bottom',
            bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7, edgecolor='none'))

fig.suptitle(
    'Actual vs. Income-Predicted Nutrition Change  (Census CPS/ASEC income, 2004 baseline)',
    fontsize=13, fontweight='bold', y=1.01
)
plt.tight_layout()
out = FIG_DIR / 'nutrition_predicted_vs_actual_3panel.png'
plt.savefig(out, bbox_inches='tight', dpi=150)
plt.close()
log(f"Saved: {out}")
log("Done!")
