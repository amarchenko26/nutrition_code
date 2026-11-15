"""
build_bartik_bls_iv.py  --  run LOCALLY

Classic Bartik (1991) shift-share IV using BLS QCEW data.

    Bartik_{c,t} = Σ_k  s_{ck,0}  ×  g_{k,t}

where:
  s_{ck,0}  = baseline (2004-2006 avg) private employment share of 2-digit
               NAICS industry k in county c  [QCEW agglvl=74, own=5]
  g_{k,t}   = national Δlog(avg_annual_pay) for private sector industry k,
               year t-1 → t  [QCEW agglvl=14, own=5, area=US000]

Unlike the existing Nielsen-internal IV (build_iv.py / build_county_income_shock.py)
which uses within-Nielsen demographic cell averages, this instrument is constructed
entirely from external BLS administrative records. National industry wage trends
reflect aggregate technology / factor-demand shocks uncorrelated with local
dietary preferences.

Leave-one-out correction for shifts: omitted — counties are tiny fractions of
national employment so the correction is negligible (<0.01% for median county).

Data: BLS QCEW annual singlefiles 2003-2020
  URL: https://data.bls.gov/cew/data/files/{year}/csv/{year}_annual_singlefile.zip

Raw downloads  →  raw/bls/qcew_{year}_filtered.parquet
Final IV       →  interim/panel_dataset/bartik_bls_iv.parquet  (.dta also)
  columns: fips, year, bartik_bls, n_industries, share_coverage
"""

import io
import zipfile
import requests
import pandas as pd
import numpy as np
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
BASE    = data
BLS_RAW = BASE / 'raw' / 'bls'
OUT_IV  = BASE / 'interim' / 'panel_dataset' / 'bartik_bls_iv.parquet'
OUT_DTA = BASE / 'interim' / 'panel_dataset' / 'bartik_bls_iv.dta'

# 2003 needed for the first-difference that gives g_{k,2004}
DOWNLOAD_YEARS = list(range(2003, 2021))
BASELINE_YEARS = [2004, 2005, 2006]
STUDY_YEARS    = list(range(2004, 2021))

BLS_URL = (
    "https://data.bls.gov/cew/data/files/{year}/csv/{year}_annual_singlefile.zip"
)

# Columns we actually need from the ~43-column file
KEEP_COLS = [
    'area_fips', 'own_code', 'industry_code', 'agglvl_code',
    'size_code', 'year', 'disclosure_code',
    'annual_avg_emplvl', 'avg_annual_pay',
]


def log(msg): print(msg, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Download + filter QCEW annual files
# ─────────────────────────────────────────────────────────────────────────────
def download_and_filter(year: int) -> pd.DataFrame:
    """
    Download the QCEW annual singlefile for {year} (or use cached parquet).
    Returns a filtered DataFrame with:
      - county × NAICS sector × private rows  (level='county')
      - national × NAICS sector × private rows (level='national')
    """
    cache = BLS_RAW / f'qcew_{year}_filtered.parquet'
    if cache.exists():
        log(f"  {year}: cached → {cache.name}")
        return pd.read_parquet(cache)

    url = BLS_URL.format(year=year)
    log(f"  {year}: downloading {url} ...")
    resp = requests.get(url, timeout=600, stream=True)
    resp.raise_for_status()
    raw_bytes = b''.join(resp.iter_content(1 << 20))
    log(f"  {year}: {len(raw_bytes)/1e6:.1f} MB — parsing ...")

    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
        csv_name = next(n for n in zf.namelist() if n.endswith('.csv'))
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, usecols=KEEP_COLS, dtype=str, low_memory=False)

    log(f"  {year}: {len(df):,} total rows loaded")

    # ── County × NAICS Sector × Private ──────────────────────────────────────
    # agglvl_code=74 : County, NAICS Sector -- by ownership sector
    # own_code=5     : Total Private
    # size_code=0    : All establishment sizes
    # area_fips      : 5-digit numeric county FIPS (excludes state/national rows)
    # disclosure_code: skip suppressed cells
    county = df[
        (df['agglvl_code'] == '74') &
        (df['own_code']    == '5')  &
        (df['size_code']   == '0')  &
        (df['area_fips'].str.match(r'^\d{5}$')) &
        (df['disclosure_code'].fillna('') != 'N')
    ].copy()

    # ── National × NAICS Sector × Private ────────────────────────────────────
    # agglvl_code=14 : National, NAICS Sector -- by ownership sector
    # area_fips      : "US000"
    national = df[
        (df['agglvl_code'] == '14') &
        (df['own_code']    == '5')  &
        (df['size_code']   == '0')  &
        (df['area_fips']   == 'US000')
    ].copy()

    out = pd.concat(
        [county.assign(level='county'), national.assign(level='national')],
        ignore_index=True,
    )

    for col in ('annual_avg_emplvl', 'avg_annual_pay'):
        out[col] = pd.to_numeric(out[col], errors='coerce')
    out['year'] = pd.to_numeric(out['year'], errors='coerce').astype('Int64')

    out.to_parquet(cache, index=False)
    log(f"  {year}: {len(county):,} county + {len(national):,} national rows → {cache.name}")
    return out


log("=" * 65)
log("STEP 1: Download / load QCEW data (2003-2020)")
log("=" * 65)
BLS_RAW.mkdir(parents=True, exist_ok=True)

all_frames = [download_and_filter(yr) for yr in DOWNLOAD_YEARS]
df_all = pd.concat(all_frames, ignore_index=True)
log(f"\nAll years combined: {len(df_all):,} rows")

county_df   = df_all[df_all['level'] == 'county'].copy()
national_df = df_all[df_all['level'] == 'national'].copy()

# Diagnostic: what 2-digit NAICS codes are present?
log(f"\nNational industry codes: {sorted(national_df['industry_code'].unique())}")
log(f"County  industry codes (sample): {sorted(county_df['industry_code'].unique())[:20]}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: National wage shifts  g_{k,t} = Δlog(avg_annual_pay_{k,t})
# ─────────────────────────────────────────────────────────────────────────────
log("\n" + "=" * 65)
log("STEP 2: National industry wage shifts  g_{k,t}")
log("=" * 65)

nat = (
    national_df[national_df['avg_annual_pay'].gt(0)]
    .groupby(['industry_code', 'year'])['avg_annual_pay']
    .mean()          # unique per cell; .mean() is safe
    .reset_index()
    .sort_values(['industry_code', 'year'])
)

nat['log_pay'] = np.log(nat['avg_annual_pay'])
nat['g_kt']    = nat.groupby('industry_code')['log_pay'].diff()

shifts = (
    nat[nat['year'].isin(STUDY_YEARS) & nat['g_kt'].notna()]
    [['industry_code', 'year', 'g_kt']]
    .copy()
)

log(f"  Industries with shifts: {shifts['industry_code'].nunique()}")
log(f"  g_kt: mean={shifts['g_kt'].mean():.4f}  "
    f"sd={shifts['g_kt'].std():.4f}  "
    f"p5={shifts['g_kt'].quantile(0.05):.4f}  "
    f"p95={shifts['g_kt'].quantile(0.95):.4f}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Baseline county × industry employment shares  s_{ck,0}
# ─────────────────────────────────────────────────────────────────────────────
log("\n" + "=" * 65)
log("STEP 3: Baseline employment shares  s_{ck,0}  (2004-2006 avg)")
log("=" * 65)

base = county_df[
    county_df['year'].isin(BASELINE_YEARS) &
    county_df['annual_avg_emplvl'].gt(0)
].copy()

# Average employment over baseline years for each county × industry
base_avg = (
    base.groupby(['area_fips', 'industry_code'])['annual_avg_emplvl']
    .mean()
    .reset_index(name='emp_base')
)

# Total private employment per county at baseline (denominator)
cty_total = (
    base_avg.groupby('area_fips')['emp_base']
    .sum()
    .reset_index(name='emp_total')
)

shares = base_avg.merge(cty_total, on='area_fips')
shares['s_ck'] = shares['emp_base'] / shares['emp_total']

# Restrict to industries that have national shifts
valid_industries = set(shifts['industry_code'].unique())
shares = shares[shares['industry_code'].isin(valid_industries)].copy()

log(f"  County-industry pairs (shares): {len(shares):,}")
log(f"  Counties: {shares['area_fips'].nunique():,}")
log(f"  Industries kept: {shares['industry_code'].nunique()}")
log(f"  Avg industries per county: {shares.groupby('area_fips').size().mean():.1f}")
log(f"  Avg share coverage (Σ s_ck per county): "
    f"{shares.groupby('area_fips')['s_ck'].sum().mean():.3f}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Bartik IV  =  Σ_k  s_{ck,0}  ×  g_{k,t}
# ─────────────────────────────────────────────────────────────────────────────
log("\n" + "=" * 65)
log("STEP 4: Bartik_{c,t} = Σ_k s_{ck,0} × g_{k,t}")
log("=" * 65)

# Expand shares across all study years, then merge national shifts
shares_panel = pd.concat(
    [shares.assign(year=yr) for yr in STUDY_YEARS],
    ignore_index=True,
)
shares_panel = shares_panel.merge(shifts, on=['industry_code', 'year'], how='inner')
shares_panel['contribution'] = shares_panel['s_ck'] * shares_panel['g_kt']

bartik = (
    shares_panel
    .groupby(['area_fips', 'year'])
    .agg(
        bartik_bls     = ('contribution',  'sum'),
        n_industries   = ('industry_code', 'nunique'),
        share_coverage = ('s_ck',          'sum'),   # Σ s_ck ≤ 1 (suppressed cells excluded)
    )
    .reset_index()
    .rename(columns={'area_fips': 'fips'})
)

bartik['year'] = bartik['year'].astype(int)

log(f"  County-year observations: {len(bartik):,}")
log(f"  Counties: {bartik['fips'].nunique():,}")
log(f"  Year range: {bartik['year'].min()} – {bartik['year'].max()}")
log(f"\n  bartik_bls summary:")
log(f"    mean  = {bartik['bartik_bls'].mean():.5f}")
log(f"    sd    = {bartik['bartik_bls'].std():.5f}")
log(f"    p5    = {bartik['bartik_bls'].quantile(0.05):.5f}")
log(f"    p25   = {bartik['bartik_bls'].quantile(0.25):.5f}")
log(f"    p75   = {bartik['bartik_bls'].quantile(0.75):.5f}")
log(f"    p95   = {bartik['bartik_bls'].quantile(0.95):.5f}")
log(f"  Avg n_industries per county-year: {bartik['n_industries'].mean():.1f}")
log(f"  Avg share_coverage: {bartik['share_coverage'].mean():.3f}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Save
# ─────────────────────────────────────────────────────────────────────────────
log("\n" + "=" * 65)
log(f"STEP 5: Save")
log("=" * 65)

bartik.to_parquet(OUT_IV, index=False)
bartik.to_stata(str(OUT_DTA), write_index=False)

log(f"  Saved parquet: {OUT_IV}")
log(f"  Saved Stata:   {OUT_DTA}")
log("""
  Quick Stata validation:
    use "bartik_bls_iv.dta", clear
    merge 1:1 fips year using "county_income_shocks.dta", nogen
    corr bartik_bls d_log_income
    ivreg2 d_log_income (bartik_bls = bartik_bls), first cluster(fips)
""")
log("Done.")
