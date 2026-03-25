"""
build_price_index.py  —  run on OSCAR

Computes the Jaravel (2019) variety-adjusted price index from Nielsen RMS data.

For each consecutive year pair (t, t+1):
  1. Load UPC x FIPS spending + units for years t and t+1
  2. Compute unit values: p_{k,c,t} = total_spending / total_units
  3. Identify continuing UPCs: present in both t and t+1 (within module x FIPS)
  4. Compute Sato-Vartia CES price index for continuing UPCs (module x FIPS)
  5. Compute Feenstra lambda correction from SSNP / SSEP
  6. Combine: pi_VA = pi_CES * lambda_correction

Output (saved to OUT_DIR):
  price_index_module_fips_year.parquet  — module x fips x year with all components
  price_index_module_year.parquet       — spending-weighted collapse to module x year
"""

import pandas as pd
import numpy as np
from pathlib import Path
import gc

# ============================================================
# PATHS
# ============================================================
RMS_VAR = Path('/users/amarche4/data/rms_variety')
OUT_DIR = RMS_VAR   # save alongside the variety parquets
VARIETY = RMS_VAR / 'rms_variety_module_fips_year.parquet'

YEARS = list(range(2007, 2021))   # drop 2006 (left-censored)
SIGMA = 5                          # elasticity of substitution for Feenstra

# Price winsorization: drop unit values outside [p1, p99] within module x year
PRICE_WINSOR = (0.01, 0.99)

def log(msg):
    print(msg, flush=True)


# ============================================================
# HELPERS
# ============================================================
def load_upc_year(year):
    """Load UPC x FIPS spending/units for one year. Returns DataFrame."""
    path = RMS_VAR / f'rms_upc_fips_spending_{year}.parquet'
    df = pd.read_parquet(path, columns=[
        'upc', 'product_module_code', 'fips', 'total_spending', 'total_units'
    ])
    df = df[df['total_units'] > 0].copy()
    df['price'] = df['total_spending'] / df['total_units']
    df = df[df['price'] > 0]
    return df


def winsorize_prices(df):
    """Winsorize unit values within each product_module_code."""
    lo = df.groupby('product_module_code')['price'].transform(
        lambda x: x.quantile(PRICE_WINSOR[0]))
    hi = df.groupby('product_module_code')['price'].transform(
        lambda x: x.quantile(PRICE_WINSOR[1]))
    df['price'] = df['price'].clip(lo, hi)
    return df


def sato_vartia_weight(s_t, s_t1):
    """
    Sato-Vartia log-mean weight.
    L(a, b) = (a - b) / (ln a - ln b)  for a != b
            = a                          for a == b
    Returns unnormalized weights (caller normalizes to sum=1).
    """
    same = np.isclose(s_t, s_t1)
    with np.errstate(divide='ignore', invalid='ignore'):
        lm = np.where(same, s_t,
                      (s_t1 - s_t) / (np.log(s_t1) - np.log(s_t)))
    return lm


def compute_ces_one_pair(df_t, df_t1):
    """
    Given UPC-level data for year t and t+1, compute the Sato-Vartia CES
    price index per module x fips for continuing UPCs.

    Returns DataFrame: product_module_code, fips, log_pi_ces, spend_cont_t, n_cont
    """
    KEY = ['product_module_code', 'fips', 'upc']

    # Keep only continuing UPCs (present in both years, same module x fips)
    merged = df_t[KEY + ['price', 'total_spending']].merge(
        df_t1[KEY + ['price', 'total_spending']],
        on=KEY, suffixes=('_t', '_t1'), how='inner'
    )
    if merged.empty:
        return pd.DataFrame()

    # Log price ratio
    merged['log_price_ratio'] = np.log(merged['price_t1'] / merged['price_t'])

    # Spending shares within module x fips (over continuing UPCs only)
    grp_cols = ['product_module_code', 'fips']
    merged['spend_cont_t']  = merged.groupby(grp_cols)['total_spending_t'].transform('sum')
    merged['spend_cont_t1'] = merged.groupby(grp_cols)['total_spending_t1'].transform('sum')
    merged['s_t']  = merged['total_spending_t']  / merged['spend_cont_t']
    merged['s_t1'] = merged['total_spending_t1'] / merged['spend_cont_t1']

    # Sato-Vartia weights (unnormalized)
    merged['sv_raw'] = sato_vartia_weight(merged['s_t'].values, merged['s_t1'].values)

    # Normalize within module x fips
    merged['sv_sum'] = merged.groupby(grp_cols)['sv_raw'].transform('sum')
    merged['omega']  = merged['sv_raw'] / merged['sv_sum']

    # CES log price index = sum(omega * log_price_ratio) per module x fips
    merged['weighted_log_pr'] = merged['omega'] * merged['log_price_ratio']

    result = (merged.groupby(grp_cols, as_index=False)
              .agg(
                  log_pi_ces    = ('weighted_log_pr', 'sum'),
                  spend_cont_t  = ('spend_cont_t',    'first'),
                  n_cont        = ('upc',              'count'),
              ))
    return result


# ============================================================
# MAIN
# ============================================================
def main():
    log("Loading variety data (SSNP / SSEP)...")
    variety = pd.read_parquet(VARIETY, columns=[
        'product_module_code', 'fips', 'year', 'ssnp', 'ssep', 'total_spending'
    ])
    variety = variety[(variety['year'] >= 2007) & (variety['year'] <= 2020)]
    variety = variety.dropna(subset=['ssnp', 'ssep'])
    log(f"  {len(variety):,} module-fips-year rows")

    results = []

    for t in YEARS[:-1]:       # t = 2007..2019, pair is (t, t+1)
        t1 = t + 1
        log(f"\nProcessing {t} → {t1}...")

        # --- Load and winsorize unit values ---
        df_t  = load_upc_year(t)
        df_t1 = load_upc_year(t1)
        log(f"  Loaded: {len(df_t):,} UPC-FIPS obs in {t}, {len(df_t1):,} in {t1}")

        df_t  = winsorize_prices(df_t)
        df_t1 = winsorize_prices(df_t1)

        # --- CES price index for continuing products ---
        ces = compute_ces_one_pair(df_t, df_t1)
        log(f"  CES computed: {len(ces):,} module-FIPS cells")

        # --- Feenstra lambda correction ---
        # lambda_t   = 1 - SSNP_t     (share of t spending on continuing products)
        # lambda_t-1 = 1 - SSEP_{t-1} (share of t-1 spending on continuing products)
        # We use ssep from year t (= products available in t but not after t),
        # which equals the exit share going from t to t+1.
        v_t  = variety[variety['year'] == t][
            ['product_module_code', 'fips', 'ssnp', 'ssep', 'total_spending']
        ].copy()
        v_t1 = variety[variety['year'] == t1][
            ['product_module_code', 'fips', 'ssnp']
        ].rename(columns={'ssnp': 'ssnp_t1'})

        feenstra = v_t.merge(v_t1, on=['product_module_code', 'fips'], how='inner')
        feenstra['lambda_t']   = (1 - feenstra['ssnp_t1']).clip(0.01, 1)
        feenstra['lambda_tm1'] = (1 - feenstra['ssep']).clip(0.01, 1)
        feenstra['log_feenstra'] = (
            np.log(feenstra['lambda_t'] / feenstra['lambda_tm1'])
            / (SIGMA - 1)
        )

        # --- Combine CES + Feenstra ---
        grp = ces.merge(
            feenstra[['product_module_code', 'fips', 'log_feenstra',
                       'total_spending', 'lambda_t', 'lambda_tm1']],
            on=['product_module_code', 'fips'], how='inner'
        )
        grp['log_pi_va']    = grp['log_pi_ces'] + grp['log_feenstra']
        grp['pi_ces']       = np.exp(grp['log_pi_ces'])
        grp['pi_feenstra']  = np.exp(grp['log_feenstra'])
        grp['pi_va']        = np.exp(grp['log_pi_va'])
        grp['year']         = t1   # price change FROM t TO t+1, indexed at t+1

        results.append(grp[['product_module_code', 'fips', 'year',
                             'pi_ces', 'pi_feenstra', 'pi_va',
                             'log_pi_ces', 'log_feenstra', 'log_pi_va',
                             'total_spending', 'spend_cont_t', 'n_cont']])

        del df_t, df_t1, ces, feenstra, grp
        gc.collect()

    # ============================================================
    # ASSEMBLE AND CUMULATE
    # ============================================================
    log("\nAssembling results...")
    out = pd.concat(results, ignore_index=True)

    # Cumulative price levels (base = 1 at first year in data = 2008)
    out = out.sort_values(['product_module_code', 'fips', 'year'])
    for col, cum_col in [('log_pi_ces', 'level_ces'),
                          ('log_pi_va',  'level_va')]:
        out[cum_col] = out.groupby(['product_module_code', 'fips'])[col].cumsum()
        out[cum_col] = np.exp(out[cum_col])

    # Save module x fips x year
    out_path = OUT_DIR / 'price_index_module_fips_year.parquet'
    out.to_parquet(out_path, index=False)
    log(f"Saved: {out_path}  ({len(out):,} rows)")

    # ============================================================
    # COLLAPSE TO MODULE x YEAR (spending-weighted)
    # ============================================================
    log("Collapsing to module x year...")

    def wavg(df, val_col, wt_col):
        return np.average(df[val_col], weights=df[wt_col])

    rows = []
    for (mod, yr), g in out.groupby(['product_module_code', 'year']):
        rows.append({
            'product_module_code': mod,
            'year': yr,
            'pi_ces':      wavg(g, 'pi_ces',    'total_spending'),
            'pi_feenstra': wavg(g, 'pi_feenstra','total_spending'),
            'pi_va':       wavg(g, 'pi_va',      'total_spending'),
            'level_ces':   wavg(g, 'level_ces',  'total_spending'),
            'level_va':    wavg(g, 'level_va',   'total_spending'),
            'total_spending': g['total_spending'].sum(),
        })
    mod_yr = pd.DataFrame(rows)

    out_path2 = OUT_DIR / 'price_index_module_year.parquet'
    mod_yr.to_parquet(out_path2, index=False)
    log(f"Saved: {out_path2}  ({len(mod_yr):,} rows)")

    # ============================================================
    # QUICK SUMMARY
    # ============================================================
    log("\nMean annual pi_ces by year:")
    log(out.groupby('year')[['pi_ces','pi_feenstra','pi_va']].mean().to_string())
    log("\nDone.")


if __name__ == '__main__':
    main()
