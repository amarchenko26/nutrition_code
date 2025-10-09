# pip install geopandas rasterio rasterstats pyreadstat shapely pandas numpy
import os
from pathlib import Path
import tempfile
import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterstats import zonal_stats

# -------------------------------
# CONFIG — EDIT PATHS
# -------------------------------
COUNTIES_SHP = Path("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/counties/cb_2022_us_county_5m.shp")
RASTER_DIR   = Path("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/raw/fao_gaez_v4")
OUT_DTA      = Path("/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/corn/interim/gaez/gaez_by_county.dta")
OUT_DTA.parent.mkdir(parents=True, exist_ok=True)

# crop code -> readable name (based on your files)
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

# discover your 11 rasters
RASTER_FILES = sorted(RASTER_DIR.glob("sxHr0_*.tif"))

# -------------------------------
# 1) Load counties, keep CONUS only, project to equal-area
# -------------------------------
print("Reading counties…")
gdf = gpd.read_file(COUNTIES_SHP)

# keep 48 states + DC (drop AK=02, HI=15, PR=72 + territories: 60,66,69,78)
drop_states = {"02","15","72","60","66","69","78"}
gdf = gdf[~gdf["STATEFP"].isin(drop_states)].copy()

# keep core fields
cols = [c for c in ["STATEFP","COUNTYFP","GEOID","NAME"] if c in gdf.columns]
gdf = gdf[cols + ["geometry"]].copy()

# state name lookup
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

# equal-area CRS
EA_CRS = "EPSG:5070"
gdf = gdf.to_crs(EA_CRS)

# -------------------------------
# helper: warp raster to 5070 once
# -------------------------------
def warp_to_equal_area(in_path: Path) -> Path:
    with rasterio.open(in_path) as src:
        dst_crs = EA_CRS
        dst_res = (10000, 10000)  # ~10 km
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds, resolution=dst_res
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

# -------------------------------
# 2) Zonal mean per county, per crop
# -------------------------------
frames = []

for tif in RASTER_FILES:
    code = tif.stem.split("_")[-1].lower()  # sxHr0_brl -> brl
    crop = CROP_NAME.get(code, code)
    print(f"Processing {tif.name} → {crop}")

    warped = warp_to_equal_area(tif)
    with rasterio.open(warped) as src:
        nodata = src.nodata

    zs = zonal_stats(
        gdf, warped, stats=["mean"], nodata=nodata,
        all_touched=False, geojson_out=False
    )
    mean_vals = pd.DataFrame(zs)["mean"].astype("float64").to_numpy()

    dfc = pd.DataFrame({
        "STATEFP": gdf["STATEFP"].astype(str).values,
        "COUNTYFP": gdf["COUNTYFP"].astype(str).values,
        "GEOID": gdf["GEOID"].astype(str).values,
        "STATE_NAME": gdf["STATE_NAME"].astype(str).values,
        f"si_{crop}": mean_vals  # still 0–10,000 scale
    })
    frames.append(dfc)

    try: os.remove(warped)
    except Exception: pass

# -------------------------------
# 3) Merge all crops into one table
# -------------------------------
out = frames[0]
for df in frames[1:]:
    out = out.merge(df[["GEOID"] + [c for c in df.columns if c.startswith("si_")]],
                    on="GEOID", how="left")

out["statefip"]  = out["STATEFP"].astype(int)
out["countfip"] = out["COUNTYFP"].astype(int)
out["fips"]        = out["GEOID"].astype(int)
out['state_name'] = out['STATE_NAME'].astype(str)

# order: ids then 11 crop columns
meta_cols = ["state_name","statefip","countfip", "fips"]
crop_cols = sorted([c for c in out.columns if c.startswith("si_")])
out = out[meta_cols + crop_cols]

# -------------------------------
# 4) Save
# -------------------------------
out.to_stata(OUT_DTA, write_index=False, version=118)
