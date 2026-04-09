"""
build_innovation_reg_data.py  --  run on OSCAR

Builds two regression datasets:

  (A) innovation_reg_data.dta  — module x year panel
        v_mt = ssnp, ssep, n_new_upcs, share_new_upcs  (nationally new UPCs)
        d_mt = real expenditure growth in module m
        iv   = Bartik IV = Σ_c share_mct0 * d_hat_ct

  (B) innovation_reg_data_county.dta  — module x county x year panel
        v_mct = ssnp, ssep, n_new_upcs_county, share_new_upcs_county  (locally new UPCs)
        d_mct = real expenditure growth in module m, county c
        iv    = d_log_income_hat_ct  (county income shock directly — no Bartik needed)

Requires upc_first_year_county.parquet (built by build_upc_first_year_county.py).
"""

import pandas as pd
import numpy as np
from pathlib import Path
import gc

RMS_VAR  = Path('/users/amarche4/data/rms_variety')
YEARS    = list(range(2007, 2021))
BASE_YRS = [2008, 2009, 2010]    # baseline window for Bartik shares

def log(msg):
    print(msg, flush=True)

# ============================================================
# 1. LOAD VARIETY DATA: innovation outcomes + spending
# ============================================================
log("Loading rms_variety_module_fips_year...")
var = pd.read_parquet(RMS_VAR / 'rms_variety_module_fips_year.parquet',
                      columns=['product_module_code', 'fips', 'year',
                               'ssnp', 'ssep', 'total_spending'])
var = var[(var['year'] >= 2007) & (var['year'] <= 2020)]
log(f"  {len(var):,} module-fips-year rows")

# ============================================================
# 2. COUNT NEW UPCs PER MODULE x YEAR (national) AND MODULE x COUNTY x YEAR (local)
# ============================================================
log("\nCounting new UPCs per module x year...")

# Build global first-appearance year for each (upc, module)
first_seen = {}    # (upc, module) -> first year

for yr in YEARS:
    path = RMS_VAR / f'rms_upc_fips_spending_{yr}.parquet'
    if not path.exists():
        continue
    df = pd.read_parquet(path, columns=['upc', 'product_module_code'])
    df = df.drop_duplicates(['upc', 'product_module_code'])
    for row in df.itertuples(index=False):
        key = (row.upc, row.product_module_code)
        if key not in first_seen:
            first_seen[key] = yr
    log(f"  {yr}: {len(first_seen):,} unique upc-module pairs seen so far")

first_seen_df = pd.DataFrame(
    [(upc, mod, yr) for (upc, mod), yr in first_seen.items()],
    columns=['upc', 'product_module_code', 'first_year']
)
del first_seen; gc.collect()

# Count total and new UPCs per module x year (national) AND per module x county x year (local)
upc_counts = []
county_upc_counts = []

for yr in YEARS:
    path = RMS_VAR / f'rms_upc_fips_spending_{yr}.parquet'
    if not path.exists():
        continue
    df = pd.read_parquet(path, columns=['upc', 'product_module_code', 'fips'])
    df = df.drop_duplicates(['upc', 'product_module_code', 'fips'])

    # National: deduplicate to upc-module before counting
    df_nat = df[['upc', 'product_module_code']].drop_duplicates()
    df_nat = df_nat.merge(first_seen_df, on=['upc', 'product_module_code'], how='left')
    agg = df_nat.groupby('product_module_code').agg(
        n_upcs     = ('upc', 'count'),
        n_new_upcs = ('first_year', lambda x: (x == yr).sum())
    ).reset_index()
    agg['year'] = yr
    upc_counts.append(agg)

    # County: total UPCs per (module, fips, year) — new counts come from upc_first_year_county later
    agg_cty = (df.groupby(['product_module_code', 'fips'])
               .agg(n_upcs_county=('upc', 'count')).reset_index())
    agg_cty['year'] = yr
    county_upc_counts.append(agg_cty)

upc_yr = pd.concat(upc_counts, ignore_index=True)
upc_yr['share_new_upcs'] = upc_yr['n_new_upcs'] / upc_yr['n_upcs']
log(f"  UPC counts done: {len(upc_yr):,} module-year rows")

upc_cty_yr = pd.concat(county_upc_counts, ignore_index=True)
log(f"  County UPC counts: {len(upc_cty_yr):,} module-county-year rows")
del upc_counts, county_upc_counts; gc.collect()

# ============================================================
# 3. COLLAPSE VARIETY DATA TO MODULE x YEAR
#    spending-weighted across FIPS
# ============================================================
log("\nCollapsing to module x year...")

var_valid = var.dropna(subset=['ssnp', 'ssep'])
var_valid['ssnp_wt'] = var_valid['ssnp'] * var_valid['total_spending']
var_valid['ssep_wt'] = var_valid['ssep'] * var_valid['total_spending']

mod_yr = (var_valid.groupby(['product_module_code', 'year'])
          .agg(ssnp_num    = ('ssnp_wt',       'sum'),
               ssep_num    = ('ssep_wt',       'sum'),
               total_spending = ('total_spending', 'sum'))
          .reset_index())
mod_yr['ssnp'] = mod_yr['ssnp_num'] / mod_yr['total_spending']
mod_yr['ssep'] = mod_yr['ssep_num'] / mod_yr['total_spending']
mod_yr = mod_yr.drop(columns=['ssnp_num', 'ssep_num'])

mod_yr = mod_yr.merge(upc_yr[['product_module_code', 'year', 'n_upcs',
                                'n_new_upcs', 'share_new_upcs']],
                      on=['product_module_code', 'year'], how='left')
log(f"  Module-year panel: {len(mod_yr):,} rows")

# ============================================================
# 4. REAL EXPENDITURE GROWTH d_mt
# ============================================================
log("\nComputing real expenditure growth...")

pi = pd.read_parquet(RMS_VAR / 'price_index_module_year.parquet',
                     columns=['product_module_code', 'year', 'level_ces'])

mod_yr = mod_yr.merge(pi, on=['product_module_code', 'year'], how='left')
mod_yr = mod_yr.sort_values(['product_module_code', 'year'])

mod_yr['log_real_spending'] = (np.log(mod_yr['total_spending']) -
                                np.log(mod_yr['level_ces']))
mod_yr['d_log_real_spending'] = (mod_yr.groupby('product_module_code')
                                       ['log_real_spending'].diff())

# ============================================================
# 5. BARTIK IV: w_mt = Σ_c share_mct0 * d_hat_ct
# ============================================================
log("\nBuilding Bartik IV...")

shocks = pd.read_parquet(RMS_VAR / 'county_income_shocks.parquet',
                         columns=['fips', 'year', 'd_log_income_hat'])
log(f"  Income shocks: {len(shocks):,} county-year obs, "
    f"{shocks['fips'].nunique()} counties")

base = var[var['year'].isin(BASE_YRS)].copy()
base_shares = (base.groupby(['product_module_code', 'fips'])['total_spending']
               .mean().reset_index().rename(columns={'total_spending': 'base_spend'}))
mod_base_tot = (base_shares.groupby('product_module_code')['base_spend']
                .sum().rename('mod_base_tot').reset_index())
base_shares = base_shares.merge(mod_base_tot, on='product_module_code')
base_shares['share_mct0'] = base_shares['base_spend'] / base_shares['mod_base_tot']
log(f"  Baseline shares: {len(base_shares):,} module-fips pairs")

bartik_long = base_shares.merge(shocks, on='fips', how='inner')
bartik_long['contribution'] = bartik_long['share_mct0'] * bartik_long['d_log_income_hat']

bartik = (bartik_long.groupby(['product_module_code', 'year'])
          .agg(w_bartik      = ('contribution', 'sum'),
               share_covered = ('share_mct0',   'sum'))
          .reset_index())
log(f"  Bartik IV: {len(bartik):,} module-year obs")
log(f"  Avg share of module spending covered by counties with income data: "
    f"{bartik['share_covered'].mean():.2%}")

# ============================================================
# 6. ASSEMBLE MODULE x YEAR DATASET (A)
# ============================================================
log("\nAssembling module x year dataset...")

out = (mod_yr.merge(bartik[['product_module_code', 'year',
                              'w_bartik', 'share_covered']],
                    on=['product_module_code', 'year'], how='left'))

out = out[(out['year'] >= 2008) & (out['year'] <= 2020)]
out = out.dropna(subset=['d_log_real_spending', 'w_bartik'])

out = out.rename(columns={
    'product_module_code': 'module_code',
    'd_log_real_spending': 'd_spending',
    'w_bartik':            'iv_bartik',
    'total_spending':      'spending',
    'level_ces':           'price_level_ces',
})

log(f"\nFinal dataset (A): {len(out):,} module-year obs, "
    f"{out['module_code'].nunique()} modules, "
    f"years {out['year'].min()}-{out['year'].max()}")
log("Means of key variables:")
for v in ['ssnp', 'ssep', 'share_new_upcs', 'd_spending', 'iv_bartik']:
    log(f"  {v:20s}: {out[v].mean():.4f}  (sd={out[v].std():.4f})")

out_path = RMS_VAR / 'innovation_reg_data.dta'
out.to_stata(out_path, write_index=False, version=118)
log(f"Saved: {out_path}")

del out; gc.collect()

# ============================================================
# 7. COUNTY-LEVEL NEW UPC COUNTS
#    Requires upc_first_year_county.parquet (from build_upc_first_year_county.py)
# ============================================================
log("\nLoading upc_first_year_county...")
first_cty = pd.read_parquet(RMS_VAR / 'upc_first_year_county.parquet',
                             columns=['product_module_code', 'fips', 'first_year_county'])

# Count new UPCs per (module, fips, year) = rows where first_year_county == year
new_cty = (first_cty.groupby(['product_module_code', 'fips', 'first_year_county'])
           .size().rename('n_new_upcs_county').reset_index()
           .rename(columns={'first_year_county': 'year'}))
del first_cty; gc.collect()

upc_cty_yr = upc_cty_yr.merge(new_cty, on=['product_module_code', 'fips', 'year'], how='left')
upc_cty_yr['n_new_upcs_county'] = upc_cty_yr['n_new_upcs_county'].fillna(0).astype(int)
upc_cty_yr['share_new_upcs_county'] = upc_cty_yr['n_new_upcs_county'] / upc_cty_yr['n_upcs_county']
del new_cty; gc.collect()
log(f"  County UPC counts with new-UPC flags: {len(upc_cty_yr):,} rows")

# ============================================================
# 8. ASSEMBLE MODULE x COUNTY x YEAR DATASET (B)
# ============================================================
log("\nAssembling module x county x year dataset...")

# Start from var (has ssnp, ssep, total_spending at module x fips x year)
cty = var.merge(upc_cty_yr, on=['product_module_code', 'fips', 'year'], how='left')

# Real expenditure growth per module x county
# Use national module-level CES price index (best available deflator at module level)
cty = cty.merge(pi, on=['product_module_code', 'year'], how='left')
cty = cty[(cty['total_spending'] > 0) & (cty['level_ces'] > 0)]
cty = cty.sort_values(['product_module_code', 'fips', 'year'])

cty['log_real_spending_cty'] = np.log(cty['total_spending']) - np.log(cty['level_ces'])
cty['d_spending_cty'] = (cty.groupby(['product_module_code', 'fips'])
                            ['log_real_spending_cty'].diff())
cty = cty.drop(columns=['log_real_spending_cty'])

# IV: county income shock directly (no Bartik needed at county level)
cty = cty.merge(shocks, on=['fips', 'year'], how='left')

# Final filters
cty_out = cty[(cty['year'] >= 2008) & (cty['year'] <= 2020)].copy()
cty_out = cty_out.dropna(subset=['d_spending_cty', 'd_log_income_hat'])

cty_out = cty_out.rename(columns={
    'product_module_code':   'module_code',
    'total_spending':        'spending',
    'level_ces':             'price_level_ces',
    'd_spending_cty':        'd_spending',
    'd_log_income_hat':      'iv_income',
    'n_upcs_county':         'n_upcs',
    'n_new_upcs_county':     'n_new_upcs',
    'share_new_upcs_county': 'share_new_upcs',
})

log(f"\nFinal dataset (B): {len(cty_out):,} module-county-year obs, "
    f"{cty_out['module_code'].nunique()} modules, "
    f"{cty_out['fips'].nunique()} counties, "
    f"years {cty_out['year'].min()}-{cty_out['year'].max()}")
log("Means of key variables:")
for v in ['ssnp', 'ssep', 'share_new_upcs', 'd_spending', 'iv_income']:
    if v in cty_out.columns:
        log(f"  {v:20s}: {cty_out[v].mean():.4f}  (sd={cty_out[v].std():.4f})")

cty_path = RMS_VAR / 'innovation_reg_data_county.dta'
cty_out.to_stata(cty_path, write_index=False, version=118)
log(f"Saved: {cty_path}")

log("\nDone. Transfer with:")
log("  scp amarche4@oscar.ccv.brown.edu:/users/amarche4/data/rms_variety/innovation_reg_data.dta .")
log("  scp amarche4@oscar.ccv.brown.edu:/users/amarche4/data/rms_variety/innovation_reg_data_county.dta .")
