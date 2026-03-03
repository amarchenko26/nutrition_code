"""
Map of average Health Index by household zip code.
Loads the pre-built panel dataset from build_hi_panel.py.
Requires: pgeocode (pip install pgeocode)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def log(msg):
    print(msg, flush=True)

BASE    = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
DATASET = BASE / 'interim' / 'panel_dataset' / 'panel_hh_year.parquet'
FIG_DIR = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs')
FIG_DIR.mkdir(parents=True, exist_ok=True)

MIN_OBS = 10  # minimum HH-year observations per zip to include

# ============================================================
# LOAD & AGGREGATE BY ZIP
# ============================================================
log("Loading dataset...")
hhy = pd.read_parquet(DATASET)
log(f"  {len(hhy):,} HH-year obs, {hhy['household_code'].nunique():,} HHs")

if 'zip_code' not in hhy.columns:
    raise ValueError("No zip_code column in dataset. Check build_hi_panel.py settings.")

log("Aggregating HI by zip code...")
hhy['zip_code'] = hhy['zip_code'].astype(str).str.zfill(5)
zip_agg = (
    hhy.dropna(subset=['zip_code', 'HI', 'projection_factor'])
    .groupby('zip_code')
    .apply(lambda g: pd.Series({
        'HI':    np.average(g['HI'], weights=g['projection_factor']),
        'n_obs': len(g),
    }))
    .reset_index()
)
zip_agg = zip_agg[zip_agg['n_obs'] >= MIN_OBS].copy()
log(f"  {len(zip_agg):,} zip codes with >= {MIN_OBS} obs")

# ============================================================
# GEOCODE
# ============================================================
log("Geocoding zip codes...")
try:
    import pgeocode
except ImportError:
    raise ImportError("Run: pip install pgeocode")

nomi = pgeocode.Nominatim('us')
geo  = nomi.query_postal_code(zip_agg['zip_code'].tolist())
zip_agg['lat'] = geo['latitude'].values
zip_agg['lon'] = geo['longitude'].values

zip_agg = zip_agg.dropna(subset=['lat', 'lon'])
# Contiguous US only
zip_agg = zip_agg[zip_agg['lat'].between(24, 50) & zip_agg['lon'].between(-125, -66)]
log(f"  {len(zip_agg):,} zip codes in contiguous US")

# ============================================================
# FIGURE
# ============================================================
log("Creating map...")

# Clip color scale at 2nd/98th percentile to avoid outliers dominating
vmin = zip_agg['HI'].quantile(0.02)
vmax = zip_agg['HI'].quantile(0.98)

fig, ax = plt.subplots(figsize=(14, 8))

sc = ax.scatter(
    zip_agg['lon'], zip_agg['lat'],
    c=zip_agg['HI'],
    cmap='RdYlGn',
    vmin=vmin, vmax=vmax,
    s=6, alpha=0.75, linewidths=0,
    rasterized=True,
)

cbar = plt.colorbar(sc, ax=ax, label='Health Index (std. dev.)', shrink=0.55, pad=0.02)
cbar.ax.tick_params(labelsize=10)

ax.set_xlim(-125, -66)
ax.set_ylim(24, 50)
ax.set_aspect(1.3)  # approximate equal-area stretch for continental US latitude
ax.set_title('Average Health Index by Zip Code', fontsize=15, fontweight='bold', pad=12)
ax.axis('off')

# Simple annotation
ax.text(0.01, 0.02,
        f"n = {len(zip_agg):,} zip codes  |  pooled across all years  |  weighted by projection factor",
        transform=ax.transAxes, fontsize=8, color='gray', va='bottom')

plt.tight_layout()
plt.savefig(FIG_DIR / 'hi_by_zipcode.png', bbox_inches='tight', dpi=150)
plt.close()
log(f"Saved: {FIG_DIR / 'hi_by_zipcode.png'}")
log("Done!")
