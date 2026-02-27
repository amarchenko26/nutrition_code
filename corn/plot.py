

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Choropleths of county-level geographic variation in government payments (real $) over time.

Requires: pandas, geopandas, matplotlib, requests, pyproj, shapely, mapclassify
pip install pandas geopandas matplotlib requests mapclassify

This script will:
1) Download county boundaries to the configured folder (if missing).
2) Merge your deflated census data by county FIPS.
3) Produce a per-year choropleth shaded by the configured value column.
4) Optionally create a small-multiples panel for all years on one figure.
"""

# -----------------------------
# Configuration
# -----------------------------
DATA_FILE_PATH = "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim/census_merged_1992_2022_deflated.tsv"
OUTPUT_DIR     = "/Users/anyamarchenko/Documents/GitHub/corn/output"
FIGS_DIR       = "figs"
TABS_DIR       = "tabs"

COUNTY_SAVE_DIR = "/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/counties"

# Column names in your merged dataframe
YEAR_COL  = "year"
LEVEL_COL = "level"
FIPS_COL  = "fips"                
VALUE_COL = "gov_pay_total_real"  
CORN_FLAG = True #True means we filter for counties that have >0 acres of corn 
CORN_ACRE_CUTOFF = 300

# Map options
CONUS_ONLY        = True                   
CMAP              = "viridis"
MISSING_COLOR     = "#f0f0f0"
LINE_COLOR        = "#ffffff"
LINE_WIDTH        = 0.1
FIGSIZE_SINGLE    = (10, 6)
FIGSIZE_PANEL     = (16, 10)
NORMALIZE_GLOBAL  = True                   # True: same color scale for all years
CLIP_QUANTILES    = (0.02, 0.98)           # clip extremes when computing global vmin/vmax
SAVE_PANEL_FIG    = True

# County boundary source (Cartographic 1:5m)
# See: https://www2.census.gov/geo/tiger/GENZ2022/shp/
COUNTY_ZIP_URL    = "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_county_5m.zip"
COUNTY_SHP_STEM   = "cb_2022_us_county_5m"      # shapefile stem inside the zip

# -----------------------------
# Script
# -----------------------------
import os
from pathlib import Path
import io
import zipfile
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

plt.style.use('seaborn-v0_8')

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def download_counties_if_needed(save_dir: Path, url: str, stem: str) -> Path:
    """
    Download and extract county shapefile if missing. Returns path to .shp.
    """
    ensure_dir(save_dir)
    shp_path = save_dir / f"{stem}.shp"
    if shp_path.exists():
        return shp_path

    print(f"Downloading county boundaries from:\n  {url}")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(save_dir)
    print(f"Extracted to: {save_dir}")
    if not shp_path.exists():
        raise FileNotFoundError(f"Expected shapefile {shp_path} not found after extraction.")
    return shp_path

def load_counties(shp_path: Path, conus_only: bool = True) -> gpd.GeoDataFrame:
    """
    Load counties, optionally filter to CONUS. Keeps GEOID (5-digit) for merge.
    """
    gdf = gpd.read_file(shp_path)
    # Keep needed columns
    keep = ['STATEFP', 'COUNTYFP', 'GEOID', 'NAME', 'STATE_NAME', 'geometry']
    keep = [c for c in keep if c in gdf.columns] or list(gdf.columns)
    gdf = gdf[keep].copy()

    if conus_only:
        # Exclude AK (02), HI (15), PR (72), VI (78), AS (60), GU (66), MP (69)
        excl = {'02','15','72','78','60','66','69'}
        gdf = gdf[~gdf['STATEFP'].isin(excl)].copy()

    # Project to Albers Equal Area for nicer rendering
    try:
        gdf = gdf.to_crs("EPSG:5070")
    except Exception:
        # if proj errors, keep in original
        pass
    # Standardize a 5-digit FIPS string for merge
    gdf['fips5'] = gdf['GEOID'].astype(str).str.zfill(5)
    return gdf

def load_value_data(data_path: Path,
                    year_col: str,
                    level_col: str,
                    fips_col: str,
                    value_col: str) -> pd.DataFrame:
    """
    Read TSV, filter to county rows (level==1), build fips5, keep year/value.
    If the requested value_col is missing, try a couple of common fallbacks.
    """
    df = pd.read_csv(data_path, sep="\t", low_memory=False)
    # Filter to counties
    if level_col in df.columns:
        df = df[df[level_col] == 1].copy()
    # Build fips5
    if fips_col not in df.columns:
        raise KeyError(f"'{fips_col}' not found in data.")
    df['fips5'] = pd.to_numeric(df[fips_col], errors='coerce') \
                    .astype('Int64') \
                    .astype(str) \
                    .str.replace('<NA>', '', regex=False) \
                    .str.zfill(5)

    if CORN_FLAG == True:
        df['corn_for_grain_acres'] = pd.to_numeric(
            df['corn_for_grain_acres'].astype(str).str.replace(r'[^\d\.\-]', '', regex=True),
            errors='coerce'
        )
        df = df[df['corn_for_grain_acres'].notna() & (df['corn_for_grain_acres'] > CORN_ACRE_CUTOFF)].copy()

    # Choose value column
    candidates = [value_col, 'gov_pay_total_real', 'gov_payments_total_real']
    chosen = None
    for c in candidates:
        if c in df.columns:
            chosen = c
            break
    if chosen is None:
        raise KeyError(f"None of the expected value columns found: {candidates}")

    # Keep tidy set
    sub = df[[year_col, 'fips5', chosen]].copy()
    sub = sub.rename(columns={chosen: 'value'})
    # Numeric
    sub['value'] = pd.to_numeric(sub['value'], errors='coerce')
    # Aggregate in case of duplicates (rare): mean per county-year
    sub = sub.groupby([year_col, 'fips5'], as_index=False)['value'].mean()
    return sub



def compute_global_scale(values: pd.Series, clip_q=(0.02, 0.98)):
    """
    Compute global vmin/vmax with quantile clipping to reduce the effect of outliers.
    """
    v = values.replace([np.inf, -np.inf], np.nan).dropna()
    if v.empty:
        return None, None
    lo = v.quantile(clip_q[0])
    hi = v.quantile(clip_q[1])
    # Avoid degenerate scales
    if not np.isfinite(lo) or not np.isfinite(hi) or lo == hi:
        lo, hi = v.min(), v.max()
    return float(lo), float(hi)



def plot_year_map(gdf_base: gpd.GeoDataFrame,
                  df_year: pd.DataFrame,
                  year: int,
                  vmin=None,
                  vmax=None,
                  out_dir: Path = None):
    """
    Plot one year's county choropleth, saving to PNG.
    """
    g = gdf_base.merge(df_year, on='fips5', how='left')
    fig, ax = plt.subplots(1, 1, figsize=FIGSIZE_SINGLE)
    g.plot(column='value',
           ax=ax,
           cmap=CMAP,
           linewidth=LINE_WIDTH,
           edgecolor=LINE_COLOR,
           missing_kwds={"color": MISSING_COLOR, "label": "No data"},
           vmin=vmin, vmax=vmax)
    ax.set_axis_off()
    if CORN_FLAG == True:
        ttl = f"Government Payments per Farm by County (2017 $)\nYear: {year}, Corn Growing Counties Only"
    else:
        ttl = f"Government Payments per Farm by County (2017 $)\nYear: {year}"
    ax.set_title(ttl, fontsize=14, fontweight='bold')
    # Colorbar
    sm = plt.cm.ScalarMappable(cmap=CMAP,
                               norm=plt.Normalize(vmin=vmin, vmax=vmax) if (vmin is not None and vmax is not None) else None)
    sm._A = []
    cbar = fig.colorbar(sm, ax=ax, fraction=0.030, pad=0.02)
    cbar.ax.set_ylabel("Dollars per Farm (2017 $)", rotation=90)
    plt.tight_layout()

    if out_dir is not None:
        ensure_dir(out_dir)
        out_path = out_dir / f"county_choropleth_{year}.png"
        plt.savefig(out_path, dpi=300, bbox_inches='tight')
        print(f"Saved: {out_path}")
    plt.close(fig)



def plot_panel(gdf_base: gpd.GeoDataFrame,
               df_all: pd.DataFrame,
               years: list,
               vmin=None, vmax=None,
               out_dir: Path = None):
    """
    Small-multiples panel for all years.
    """
    n = len(years)
    if n == 0:
        return
    # Grid size
    ncols = 3 if n >= 6 else 2
    nrows = int(np.ceil(n / ncols))

    fig, axes = plt.subplots(nrows, ncols, figsize=FIGSIZE_PANEL, subplot_kw={'aspect': 'equal'})
    axes = np.array(axes).reshape(-1)
    for i, yr in enumerate(years):
        ax = axes[i]
        gy = gdf_base.merge(df_all[df_all[YEAR_COL] == yr], on='fips5', how='left')
        gy.plot(column='value',
                ax=ax, cmap=CMAP, linewidth=LINE_WIDTH, edgecolor=LINE_COLOR,
                missing_kwds={"color": MISSING_COLOR, "label": "No data"},
                vmin=vmin, vmax=vmax)
        ax.set_title(str(yr), fontsize=11, fontweight='bold')
        ax.set_axis_off()
    # Hide unused axes
    for j in range(i+1, len(axes)):
        axes[j].set_axis_off()

    # Colorbar
    sm = plt.cm.ScalarMappable(cmap=CMAP,
                               norm=plt.Normalize(vmin=vmin, vmax=vmax) if (vmin is not None and vmax is not None) else None)
    sm._A = []
    cbar = fig.colorbar(sm, ax=axes.tolist(), fraction=0.015, pad=0.01)
    cbar.ax.set_ylabel("Dollars per Farm (2017 $)", rotation=90)
    
    if CORN_FLAG == True:
        ttl = f"Government Payments per Farm by County (2017 $)\nAll Census Years, Only Corn Growing Counties"
    else:
        ttl = f"Government Payments per Farm by County (2017 $)\nAll Census Years"
    
    fig.suptitle(ttl, fontsize=16, fontweight='bold', y=0.98)
    plt.tight_layout()

    if out_dir is not None and SAVE_PANEL_FIG:
        ensure_dir(out_dir)
        out_path = out_dir / "county_choropleth_all_years_panel.png"
        plt.savefig(out_path, dpi=300, bbox_inches='tight')
        print(f"Saved: {out_path}")
    plt.close(fig)

def main():
    # Paths
    county_dir = Path(COUNTY_SAVE_DIR)
    figs_dir = Path(OUTPUT_DIR) / FIGS_DIR
    data_path = Path(DATA_FILE_PATH)

    # 1) Download county boundaries
    shp_path = download_counties_if_needed(county_dir, COUNTY_ZIP_URL, COUNTY_SHP_STEM)

    # 2) Load counties & data
    gdf_counties = load_counties(shp_path, conus_only=CONUS_ONLY)
    df_values = load_value_data(data_path, YEAR_COL, LEVEL_COL, FIPS_COL, VALUE_COL)

    # 3) Global scale (optional)
    if NORMALIZE_GLOBAL:
        vmin, vmax = compute_global_scale(df_values['value'], clip_q=CLIP_QUANTILES)
    else:
        vmin = vmax = None

    # 4) Plot per-year maps
    years = sorted(df_values[YEAR_COL].dropna().astype(int).unique().tolist())
    for yr in years:
        df_y = df_values[df_values[YEAR_COL] == yr]
        plot_year_map(gdf_counties, df_y, yr, vmin=vmin, vmax=vmax, out_dir=figs_dir)

    # 5) Small-multiples panel
    plot_panel(gdf_counties, df_values, years, vmin=vmin, vmax=vmax, out_dir=figs_dir)

if __name__ == "__main__":
    main()
