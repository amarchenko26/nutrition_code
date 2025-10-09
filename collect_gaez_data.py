from pathlib import Path
import os, tempfile
import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterstats import zonal_stats

# -------------------------------
# CONFIG — edit paths if needed
# -------------------------------
COUNTIES_SHP = Path("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/counties/cb_2022_us_county_5m.shp")
RASTER_DIR   = Path("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/fao_gaez_v4")
OUT_DTA      = Path("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim/gaez/gaez_by_county.dta")
OUT_DTA.parent.mkdir(parents=True, exist_ok=True)

# discover your rasters (GAEZ file pattern)
RASTER_FILES = sorted(RASTER_DIR.glob("sxHr0_*.tif"))

# crop code -> readable name
CROP_NAME = {
    "brl": "barley",
    "chk": "chickpea",
    "cot": "cotton",
    "grd": "peanut",
    "mze": "corn",
    "oat": "oats",
    "pea": "pea",
    "rcw": "rice",
    "soy": "soybean",
    "srg": "sorghum",
    "whe": "wheat",
}

EA_CRS = "EPSG:5070"      # NAD83 / Conus Albers (equal-area)
PIX_M  = (10000, 10000)   # ~10 km pixels

# -------------------------------
# 1) Counties → CONUS only, project to equal-area
# -------------------------------
print("Reading counties…")
gdf = gpd.read_file(COUNTIES_SHP)

# keep 48 states + DC (drop AK=02, HI=15, PR=72 + territories: 60,66,69,78)
drop_states = {"02","15","72","60","66","69","78"}
gdf = gdf[~gdf["STATEFP"].isin(drop_states)].copy()

# keep core fields
cols = [c for c in ["STATEFP","COUNTYFP","GEOID","NAME"] if c in gdf.columns]
gdf = gdf[cols + ["geometry"]].copy()

STATE_FIPS_TO_NAME = {
    "01":"Alabama","04":"Arizona","05":"Arkansas","06":"California","08":"Colorado","09":"Connecticut",
    "10":"Delaware","11":"District of Columbia","12":"Florida","13":"Georgia","16":"Idaho","17":"Illinois",
    "18":"Indiana","19":"Iowa","20":"Kansas","21":"Kentucky","22":"Louisiana","23":"Maine","24":"Maryland",
    "25":"Massachusetts","26":"Michigan","27":"Minnesota","28":"Mississippi","29":"Missouri","30":"Montana",
    "31":"Nebraska","32":"Nevada","33":"New Hampshire","34":"New Jersey","35":"New Mexico","36":"New York",
    "37":"North Carolina","38":"North Dakota","39":"Ohio","40":"Oklahoma","41":"Oregon","42":"Pennsylvania",
    "44":"Rhode Island","45":"South Carolina","46":"South Dakota","47":"Tennessee","48":"Texas","49":"Utah",
    "50":"Vermont","51":"Virginia","53":"Washington","54":"West Virginia","55":"Wisconsin","56":"Wyoming"
}
gdf["STATE_NAME"] = gdf["STATEFP"].map(STATE_FIPS_TO_NAME).fillna("")
gdf = gdf.to_crs(EA_CRS)

# -------------------------------
# helpers
# -------------------------------
def warp_to_equal_area(in_path: Path) -> Path:
    """Reproject a raster to EPSG:5070 (~10km). Returns temp path."""
    with rasterio.open(in_path) as src:
        dst_crs = EA_CRS
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds, resolution=PIX_M
        )
        meta = src.meta.copy()
        meta.update({
            "crs": dst_crs, "transform": transform,
            "width": width, "height": height,
            "compress": "lzw", "tiled": True,
            "nodata": src.nodata if src.nodata is not None else -32768
        })
        tmp = Path(tempfile.mkstemp(suffix=".tif")[1])
        with rasterio.open(tmp, "w", **meta) as dst:
            reproject(
                source=rasterio.band(src, 1),
                destination=rasterio.band(dst, 1),
                src_transform=src.transform, src_crs=src.crs,
                dst_transform=transform,   dst_crs=dst_crs,
                resampling=Resampling.average, num_threads=2
            )
    return tmp

def zonal_mean(raster_path: Path, geodf: gpd.GeoDataFrame, all_touched=False) -> np.ndarray:
    """Mean of raster values within each polygon (equal-area)."""
    with rasterio.open(raster_path) as src:
        nd = src.nodata
    zs = zonal_stats(geodf, raster_path, stats=["mean"], nodata=nd,
                     all_touched=all_touched, geojson_out=False)
    return pd.Series([z["mean"] if z["mean"] is not None else np.nan for z in zs]).to_numpy(dtype="float64")

def make_binary_copy(src_path: Path, threshold: float) -> Path:
    """Return temp path to a 0/1 raster where value >= threshold."""
    with rasterio.open(src_path) as src:
        arr = src.read(1)
        nodata = src.nodata if src.nodata is not None else -32768
        arr = np.where(arr <= nodata, np.nan, arr)  # mask nodata
        bin_arr = np.where(arr >= threshold, 1.0, 0.0).astype("float32")
        tmp = Path(tempfile.mkstemp(suffix=".tif")[1])
        meta = src.meta.copy()
        meta.update(dtype="float32", nodata=np.nan)
        with rasterio.open(tmp, "w", **meta) as dst:
            dst.write(bin_arr, 1)
    return tmp

# -------------------------------
# 2) Mean SI for all crops + corn shares at 8500/5500
# -------------------------------
# base table
out = pd.DataFrame({
    "state_name": gdf["STATE_NAME"].astype(str).values,
    "statefip":   gdf["STATEFP"].astype(int).values,
    "countfip":   gdf["COUNTYFP"].astype(int).values,
    "fips":       gdf["GEOID"].astype(int).values,
})

corn_warped = None  # we’ll reuse this for shares

for tif in RASTER_FILES:
    code = tif.stem.split("_")[-1].lower()  # sxHr0_brl -> brl
    crop = CROP_NAME.get(code, code)
    print(f"Processing {tif.name} → {crop}")

    warped = warp_to_equal_area(tif)

    # 2a) county mean SI (0–10,000 scale)
    means = zonal_mean(warped, gdf, all_touched=False)
    out[f"si_{crop}"] = means

    # 2b) if this is corn, compute shares for ≥8500 and ≥5500
    if code == "mze" or crop == "corn":
        corn_warped = warped  # keep it for thresholds
    else:
        # cleanup non-corn warped rasters right away
        try: os.remove(warped)
        except Exception: pass

# If we found corn, compute shares
if corn_warped is not None:
    print("Computing corn suitability shares (≥8500, ≥5500)…")
    bin8500 = make_binary_copy(corn_warped, 8500.0)
    bin5500 = make_binary_copy(corn_warped, 5500.0)

    # mean of binary = share of county area (equal-area pixels)
    out["corn_share_8500"] = zonal_mean(bin8500, gdf, all_touched=True)
    out["corn_share_5500"] = zonal_mean(bin5500, gdf, all_touched=True)

    # cleanup
    for p in [corn_warped, bin8500, bin5500]:
        try: os.remove(p)
        except Exception: pass

# -------------------------------
# 3) Save
# -------------------------------
out.to_stata(OUT_DTA, write_index=False, version=118)
