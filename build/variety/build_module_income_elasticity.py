"""
build_module_income_elasticity.py  --  run LOCALLY

Estimates the income elasticity of demand η_m for each product module
from the HMS household panel.

Specification (per module m):
    Δlog(spend_hmt) = α_t + η_m × Δlog(income_ht) + ε_hmt

Estimated by IV:
  - Endogenous:  Δlog(real_income_ht)
  - Instrument:  Δlog(iv_income_fips_ht)  [leave-one-county-out predicted income]
  - Year FE:     within-year demeaning (Frisch-Waugh)
  - Weight:      projection_factor

First-differencing removes household FE.
Conditioning on positive spending in consecutive years → intensive margin.

Output: interim/panel_dataset/module_income_elasticity.dta
  product_module_code  eta_iv  eta_ols  n_obs  first_stage_f
"""

import pandas as pd
import numpy as np
from pathlib import Path

import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
BASE  = data
HHY   = BASE / 'interim/panel_dataset/panel_hh_year.parquet'
IV    = BASE / 'interim/panel_dataset/iv_income.parquet'
PAN   = BASE / 'interim/panelists/panelists_all_years.parquet'
PURCH = BASE / 'interim/purchases_food'
OUT   = BASE / 'interim/panel_dataset/module_income_elasticity.dta'

YEARS   = list(range(2004, 2021))
MIN_OBS = 200   # minimum obs per module for a reliable estimate

def log(msg): print(msg, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1  Household-year income panel with IV
# ─────────────────────────────────────────────────────────────────────────────
log("=" * 65)
log("STEP 1: Household-year income + IV (first differences)")
log("=" * 65)

hhy = pd.read_parquet(HHY, columns=['household_code', 'panel_year', 'real_income',
                                     'projection_factor'])
hhy = hhy[hhy['real_income'] > 0]

iv  = pd.read_parquet(IV, columns=['household_code', 'panel_year', 'iv_income_fips'])
iv  = iv[iv['iv_income_fips'] > 0]

income = hhy.merge(iv, on=['household_code', 'panel_year'], how='inner')
income['weight'] = pd.to_numeric(income['projection_factor'], errors='coerce').fillna(1.0)
log(f"  Merged: {len(income):,} HH-year obs")

# Log levels → first differences within household
income = income.sort_values(['household_code', 'panel_year'])
income['log_income'] = np.log(income['real_income'])
income['log_iv']     = np.log(income['iv_income_fips'])

income['d_log_income'] = income.groupby('household_code')['log_income'].diff()
income['d_log_iv']     = income.groupby('household_code')['log_iv'].diff()

income = income.dropna(subset=['d_log_income', 'd_log_iv'])

# Sanity check: first stage correlation should be positive
r = income['d_log_income'].corr(income['d_log_iv'])
log(f"  First-stage corr(Δlog_income, Δlog_iv): {r:.3f}")
log(f"  After first-differencing: {len(income):,} HH-year obs")

income = income[['household_code', 'panel_year',
                 'd_log_income', 'd_log_iv', 'weight']].copy()


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2  Household × module × year spending panel
# ─────────────────────────────────────────────────────────────────────────────
log("")
log("=" * 65)
log("STEP 2: Household × module × year spending (first differences)")
log("=" * 65)

frames = []
for yr in YEARS:
    pdir = PURCH / f'panel_year={yr}'
    if not pdir.exists():
        continue
    yr_df = pd.concat([
        pd.read_parquet(fp, columns=['household_code', 'product_module_code',
                                     'total_price_paid'])
        for fp in pdir.glob('*.parquet')
    ])
    agg = (yr_df.groupby(['household_code', 'product_module_code'])
           ['total_price_paid'].sum().reset_index())
    agg['panel_year'] = yr
    frames.append(agg)

hm = pd.concat(frames, ignore_index=True)
del frames
log(f"  Total HH-module-year obs: {len(hm):,}")

# Keep positive spending, compute log and first difference within (hh, module)
hm = hm[hm['total_price_paid'] > 0].copy()
hm['log_spend'] = np.log(hm['total_price_paid'])
hm = hm.sort_values(['household_code', 'product_module_code', 'panel_year'])
hm['d_log_spend'] = (hm.groupby(['household_code', 'product_module_code'])
                       ['log_spend'].diff())
hm = hm.dropna(subset=['d_log_spend'])
log(f"  After first-differencing (consecutive buys only): {len(hm):,}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3  Merge and estimate η_m per module
# ─────────────────────────────────────────────────────────────────────────────
log("")
log("=" * 65)
log("STEP 3: Estimating η_m per module")
log("=" * 65)

data = hm.merge(income, on=['household_code', 'panel_year'], how='inner')
del hm
log(f"  Analysis sample: {len(data):,} obs, "
    f"{data['product_module_code'].nunique():,} modules")


def estimate_module(grp):
    """
    IV estimate of income elasticity for one module, with year FE.

    Returns: eta_iv, eta_ols, first_stage_f, n_obs
    """
    n = len(grp)
    w  = grp['weight'].values
    y  = grp['d_log_spend'].values
    x  = grp['d_log_income'].values
    z  = grp['d_log_iv'].values
    yr = grp['panel_year'].values

    # Within-year weighted demeaning to partial out year FE (Frisch-Waugh)
    y_dm = y.copy().astype(float)
    x_dm = x.copy().astype(float)
    z_dm = z.copy().astype(float)

    for t in np.unique(yr):
        mask = (yr == t)
        wt   = w[mask]
        wsum = wt.sum()
        if wsum > 0:
            y_dm[mask] -= np.average(y[mask], weights=wt)
            x_dm[mask] -= np.average(x[mask], weights=wt)
            z_dm[mask] -= np.average(z[mask], weights=wt)

    # Weighted moments
    cov_zx = np.average(z_dm * x_dm, weights=w)
    cov_zy = np.average(z_dm * y_dm, weights=w)
    cov_xx = np.average(x_dm * x_dm, weights=w)
    cov_xy = np.average(x_dm * y_dm, weights=w)
    var_z  = np.average(z_dm * z_dm, weights=w)

    eta_iv  = cov_zy / cov_zx if abs(cov_zx) > 1e-12 else np.nan
    eta_ols = cov_xy / cov_xx if abs(cov_xx) > 1e-12 else np.nan

    # First-stage F (approximate, unweighted SE for simplicity)
    if var_z > 1e-12 and abs(cov_zx) > 1e-12:
        pi_hat   = cov_zx / var_z
        resid_x  = x_dm - pi_hat * z_dm
        mse_x    = np.average(resid_x ** 2, weights=w)
        # F = (pi_hat^2 * var_z) / (mse_x / (n - 2))
        fs_f = (pi_hat ** 2 * var_z) / (mse_x / max(n - 2, 1))
    else:
        fs_f = np.nan

    return pd.Series({
        'eta_iv':        eta_iv,
        'eta_ols':       eta_ols,
        'first_stage_f': fs_f,
        'n_obs':         n,
    })


# Filter to modules with enough obs, then groupby apply
data_filt = data[data.groupby('product_module_code')
                      ['product_module_code'].transform('count') >= MIN_OBS]
log(f"  Modules with ≥{MIN_OBS} obs: {data_filt['product_module_code'].nunique():,}")

results = (data_filt.groupby('product_module_code')
           .apply(estimate_module)
           .reset_index())

# Winsorize extreme outliers (keep 1-99th percentile)
for col in ['eta_iv', 'eta_ols']:
    p1, p99 = results[col].quantile([0.01, 0.99])
    results[col + '_raw'] = results[col]          # keep unwinsorized copy
    results[col] = results[col].clip(p1, p99)

log(f"\n  Results: {len(results):,} modules")
log(f"  η_iv:  mean={results['eta_iv'].mean():.3f}  "
    f"sd={results['eta_iv'].std():.3f}  "
    f"p10={results['eta_iv'].quantile(.10):.3f}  "
    f"p90={results['eta_iv'].quantile(.90):.3f}")
log(f"  η_ols: mean={results['eta_ols'].mean():.3f}  "
    f"sd={results['eta_ols'].std():.3f}")
log(f"  First-stage F: median={results['first_stage_f'].median():.1f}  "
    f"share > 10: {(results['first_stage_f'] > 10).mean():.1%}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4  Save
# ─────────────────────────────────────────────────────────────────────────────
log("")
log("=" * 65)
log(f"STEP 4: Saving → {OUT}")
log("=" * 65)

out_cols = ['product_module_code', 'eta_iv', 'eta_ols',
            'eta_iv_raw', 'eta_ols_raw', 'n_obs', 'first_stage_f']
results[out_cols].to_stata(str(OUT), write_index=False)
log(f"  Saved {len(results):,} module rows.")
log("""
  Stata regression check:
  ─────────────────────────────────────────────────────────
  use "$data/interim/panel_dataset/module_income_elasticity.dta", clear
  rename product_module_code module_code

  merge 1:1 module_code using "$data/interim/panel_dataset/innovation_reg_data.dta" ...
  collapse (mean) ssnp spending, by(module_code)   // collapse to cross-section

  merge m:1 module_code using "$data/interim/rms_variety/variety_healthiness_module.dta" ...
  xtile hi_q = claude_hi_z, n(4)

  * Does income elasticity predict innovation?
  reg ssnp eta_iv [aw=sqrt(spending)], robust

  * Heterogeneity: do income-elastic modules get more innovation,
  * and is this effect stronger for healthy vs unhealthy modules?
  reg ssnp ib4.hi_q c.eta_iv##ib4.hi_q [aw=sqrt(spending)], robust
  ─────────────────────────────────────────────────────────
""")
