"""
03a_total_snowlines.py
-----------------------
Compute the 90th-percentile snowline elevation and total bare-ice extent
for the entire Greenland ice sheet (PROMICE mask), without per-basin
disaggregation.

Inputs
------
- Geoid-corrected ArcticDEM mosaic (500 m, EPSG:3413)
    ../data/supporting/arcticdem_v4.1_500m_geoid_corrected.tif
- Max annual ice extent rasters
    ../data/<year>/max_ice_extent_<year>.tif
- PROMICE 2022 ice mask (vector)

Outputs
-------
- ../data/total/all_snowlines.csv  (year, percentile_90)
- ../data/total/all_extents.csv    (year, extent)

Edit the paths in the CONFIG section below, then run:
    python 03a_total_snowlines.py
"""

import glob
import os
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray as rxr
from tqdm import tqdm

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path(__file__).parent.parent.resolve()

FPATH_DEM = OUTPUT_DIR / "data" / "supporting" / "dem" / "arcticdem_v4.1_500m_geoid_corrected.tif"
FPATH_PROMICE_MASK = (
    "/Users/ju24811/Library/CloudStorage/OneDrive-UniversityofBristol/"
    "Data/promice_mask/06-PROMICE-2022-IceMask-Nunatak-polygon.gpkg"
)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # Validate inputs
    for p in [FPATH_DEM, FPATH_PROMICE_MASK]:
        if not os.path.exists(p):
            print(f"[error] Required file not found: {p}", file=sys.stderr)
            sys.exit(1)

    # Discover annual ice extent rasters
    ice_extent_fpaths = sorted(glob.glob(str(OUTPUT_DIR / "data" / "rasters" / "*" / "max_ice_extent_*.tif")))
    if not ice_extent_fpaths:
        print("[error] No max_ice_extent_*.tif files found under data/rasters/*/", file=sys.stderr)
        sys.exit(1)
    print(f"Found {len(ice_extent_fpaths)} annual ice extent raster(s).")

    # Load mask and DEM
    mask_gdf = gpd.read_file(FPATH_PROMICE_MASK).to_crs(3413)
    dem = rxr.open_rasterio(str(FPATH_DEM), masked=True).squeeze().rio.write_crs(3413)
    dem_masked = dem.rio.clip(mask_gdf.geometry, mask_gdf.crs, drop=True)

    res_x, res_y = dem.rio.resolution()
    pixel_area = abs(res_x) * abs(res_y)  # m²

    # Output directory
    out_dir = OUTPUT_DIR / "data" / "total"
    out_dir.mkdir(parents=True, exist_ok=True)

    years = []
    percentiles_90 = []
    extents = []

    for f in tqdm(ice_extent_fpaths, desc="Years"):
        year = int(os.path.basename(f).split("_")[-1].split(".")[0])

        ice_extent = (
            rxr.open_rasterio(f, masked=True)
            .squeeze()
            .rio.write_crs(3413)
            .rio.clip(mask_gdf.geometry, mask_gdf.crs, drop=True)
        )
        dem_ice = dem_masked.where(ice_extent == 1)

        vals = dem_ice.values.ravel()
        vals = vals[~np.isnan(vals)]

        if len(vals) == 0:
            print(f"  [warn] {year} - no valid pixels, recording NaN")
            percentile_90 = np.nan
            extent = 0.0
        else:
            percentile_90 = np.percentile(vals, 90)
            extent = len(vals) * pixel_area  # m²

        years.append(year)
        percentiles_90.append(percentile_90)
        extents.append(extent)

    all_total_snowlines_df = pd.DataFrame({"year": years, "percentile_90": percentiles_90})
    all_total_snowlines_df.to_csv(
        out_dir / "_all_total_snowlines.csv", index=False
    )
    all_total_extents_df = pd.DataFrame({"year": years, "extent": extents})
    all_total_extents_df.to_csv(
        out_dir / "_all_total_extents.csv", index=False
    )

    total_df = all_total_snowlines_df.merge(all_total_extents_df, on="year")
    total_df.to_csv(out_dir / "GREENLAND.csv", index=False)

    print(f"\n[save] {out_dir / 'all_snowlines.csv'}")
    print(f"[save] {out_dir / 'all_extents.csv'}")
    print("\nDone.")


if __name__ == "__main__":
    main()
