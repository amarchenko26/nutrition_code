"""
build_bartik_bls_module_iv.py  --  run LOCALLY

Aggregates the county-level BLS Bartik IV to the module x year level:

    Z_{mt}^BLS = Σ_c  share_{mc,0}  ×  bartik_bls_{ct}

where:
  share_{mc,0}    = county c's baseline (2008-2010) share of module m's national spending
                    [from RMS rms_variety_module_fips_year.parquet]
  bartik_bls_{ct} = county-level BLS Bartik IV (from build_bartik_bls_iv.py)

Intuition: if 60% of national vegan cheese sales are in SF County, then exogenous
income shocks to SF (measured via BLS industry composition) drive national vegan
cheese demand by a share of 0.6. This is identical to iv_bartik in
innovation_reg_data.dta except the county-level shock is external BLS wage shifts
rather than within-Nielsen leave-one-out income changes.

Output:
  interim/panel_dataset/bartik_bls_module_iv.dta
    module_code  year  iv_bartik_bls  share_covered  n_counties
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
BASE    = data
RMS_VAR = BASE / 'interim' / 'rms_variety'
BLS_CTY = BASE / 'interim' / 'panel_dataset' / 'bartik_bls_iv.parquet'
OUT_DTA = BASE / 'interim' / 'panel_dataset' / 'bartik_bls_module_iv.dta'
OUT_PQT = BASE / 'interim' / 'panel_dataset' / 'bartik_bls_module_iv.parquet'

BASELINE_YEARS = [2008, 2009, 2010]   # match innovation_reg_data.py
STUDY_YEARS    = list(range(2008, 2021))

def log(msg): print(msg, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Baseline module geographic spending shares  share_{mc,0}
# ─────────────────────────────────────────────────────────────────────────────
log("=" * 65)
log("STEP 1: Baseline module geographic shares  share_{mc,0}  (2008-2010)")
log("=" * 65)

var = pd.read_parquet(
    RMS_VAR / 'rms_variety_module_fips_year.parquet',
    columns=['product_module_code', 'fips', 'year', 'total_spending'],
)
var = var[var['year'].isin(BASELINE_YEARS) & (var['total_spending'] > 0)].copy()
log(f"  Baseline rows loaded: {len(var):,}")

# Average spending over baseline years per (module, county)
base_shares = (
    var.groupby(['product_module_code', 'fips'])['total_spending']
    .mean()
    .reset_index(name='base_spend')
)

# Total baseline spending per module (denominator)
mod_tot = (
    base_shares.groupby('product_module_code')['base_spend']
    .sum()
    .rename('mod_base_tot')
    .reset_index()
)

base_shares = base_shares.merge(mod_tot, on='product_module_code')
base_shares['share_mc0'] = base_shares['base_spend'] / base_shares['mod_base_tot']

log(f"  Module-county baseline pairs: {len(base_shares):,}")
log(f"  Modules: {base_shares['product_module_code'].nunique():,}")
log(f"  Counties: {base_shares['fips'].nunique():,}")
log(f"  Avg share coverage per module (should be ~1): "
    f"{base_shares.groupby('product_module_code')['share_mc0'].sum().mean():.3f}")

del var


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Load county-level BLS Bartik
# ─────────────────────────────────────────────────────────────────────────────
log("\n" + "=" * 65)
log("STEP 2: Load county-level BLS Bartik IV")
log("=" * 65)

bls = pd.read_parquet(BLS_CTY, columns=['fips', 'year', 'bartik_bls'])
bls = bls[bls['year'].isin(STUDY_YEARS)].copy()

log(f"  County-year obs: {len(bls):,}")
log(f"  Counties: {bls['fips'].nunique():,}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Z_{mt}^BLS = Σ_c  share_{mc,0}  ×  bartik_bls_{ct}
# ─────────────────────────────────────────────────────────────────────────────
log("\n" + "=" * 65)
log("STEP 3: Z_mt = Σ_c  share_{mc,0}  ×  bartik_bls_{ct}")
log("=" * 65)

# Expand baseline shares across study years, merge BLS county shocks
shares_panel = pd.concat(
    [base_shares.assign(year=yr) for yr in STUDY_YEARS],
    ignore_index=True,
)

iv_long = shares_panel.merge(bls, on=['fips', 'year'], how='inner')
iv_long['contribution'] = iv_long['share_mc0'] * iv_long['bartik_bls']

iv_module = (
    iv_long
    .groupby(['product_module_code', 'year'])
    .agg(
        iv_bartik_bls = ('contribution',  'sum'),
        share_covered = ('share_mc0',     'sum'),
        n_counties    = ('fips',          'nunique'),
    )
    .reset_index()
    .rename(columns={'product_module_code': 'module_code'})
)

iv_module['year'] = iv_module['year'].astype(int)

log(f"  Module-year observations: {len(iv_module):,}")
log(f"  Modules: {iv_module['module_code'].nunique():,}")
log(f"  Year range: {iv_module['year'].min()} – {iv_module['year'].max()}")
log(f"\n  iv_bartik_bls summary:")
log(f"    mean  = {iv_module['iv_bartik_bls'].mean():.6f}")
log(f"    sd    = {iv_module['iv_bartik_bls'].std():.6f}")
log(f"    p5    = {iv_module['iv_bartik_bls'].quantile(0.05):.6f}")
log(f"    p95   = {iv_module['iv_bartik_bls'].quantile(0.95):.6f}")
log(f"  Avg share_covered: {iv_module['share_covered'].mean():.3f}")
log(f"  Avg n_counties: {iv_module['n_counties'].mean():.0f}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Save
# ─────────────────────────────────────────────────────────────────────────────
log("\n" + "=" * 65)
log("STEP 4: Save")
log("=" * 65)

iv_module.to_parquet(OUT_PQT, index=False)
iv_module.to_stata(str(OUT_DTA), write_index=False)
log(f"  Saved parquet: {OUT_PQT}")
log(f"  Saved Stata:   {OUT_DTA}")
log("""
  Quick Stata check:
    use "innovation_reg_data.dta", clear
    merge 1:1 module_code year using "bartik_bls_module_iv.dta", nogen

    * How correlated is the BLS instrument with the Nielsen-internal one?
    corr iv_bartik_bls iv_bartik

    * First stage
    reghdfejl d_spending iv_bartik_bls [aw=spending], absorb(module_code year) cluster(module_code)

    * 2SLS
    reghdfejl ssnp (d_spending = iv_bartik_bls) [aw=spending], absorb(module_code year) cluster(module_code)
""")
log("Done.")
