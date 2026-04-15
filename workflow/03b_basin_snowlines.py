"""
03_basin_snowlines.py
----------------------
For each Mouginot basin, compute the 90th-percentile elevation of the
maximum annual ice extent and save the time series as a per-basin CSV.

Inputs
------
- Geoid-corrected ArcticDEM mosaic (500 m, EPSG:3413)
    ../data/supporting/arcticdem_v4.1_500m_geoid_corrected.tif
- Max annual ice extent rasters
    ../data/<year>/max_ice_extent_<year>.tif
- PROMICE 2022 ice mask (vector)
- Mouginot basin shapefile

Outputs
-------
- Per-basin CSV : ../data/basins/<BASIN>_percentiles.csv
- Combined CSV  : ../data/basins/_all_basins_percentiles.csv

Edit the paths in the CONFIG section below, then run:
    python 03_basin_snowlines.py
"""

import glob
import os
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray as rxr
from rioxarray.exceptions import NoDataInBounds
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
FPATH_BASINS = (
    "/Users/ju24811/Library/CloudStorage/OneDrive-UniversityofBristol/"
    "Data/basins/doi_10.7280_D1WT11__v1 2/Greenland_Basins_PS_v1.4.2.shp"
)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # Validate inputs
    for p in [FPATH_DEM, FPATH_PROMICE_MASK, FPATH_BASINS]:
        if not os.path.exists(p):
            print(f"[error] Required file not found: {p}", file=sys.stderr)
            sys.exit(1)

    # Discover annual ice extent rasters
    ice_extent_fpaths = sorted(glob.glob(str(OUTPUT_DIR / "data" / "rasters" / "*" / "max_ice_extent_*.tif")))
    if not ice_extent_fpaths:
        print("[error] No max_ice_extent_*.tif files found under data/rasters/*/", file=sys.stderr)
        sys.exit(1)
    print(f"Found {len(ice_extent_fpaths)} annual ice extent raster(s).")

    # Load vector data
    mask_gdf = gpd.read_file(FPATH_PROMICE_MASK).to_crs(3413)
    basins_gdf = gpd.read_file(FPATH_BASINS).to_crs(3413)
    basins_gdf = basins_gdf.clip(mask_gdf)

    # Load DEM
    dem = rxr.open_rasterio(str(FPATH_DEM), masked=True).squeeze().rio.write_crs(3413)
    res_x, res_y = dem.rio.resolution()
    pixel_area = abs(res_x) * abs(res_y)  # m²

    # Output directory
    out_dir = OUTPUT_DIR / "data" / "basins"
    out_dir.mkdir(parents=True, exist_ok=True)

    basin_percentiles_list = []
    basin_extents_list = []
    basin_names = basins_gdf.NAME.values
    n_total = len(basin_names)

    for i, basin_name in enumerate(tqdm(basin_names, desc="Basins"), 1):

        out_fpath = out_dir / f"{basin_name}.csv"

        if out_fpath.exists():
            print(f"  [skip] {i:03d}/{n_total:03d} {basin_name} - already exists")
            basin_df = pd.read_csv(out_fpath)
            basin_percentiles_list.append(
                {basin_name: basin_df["percentile_90"].values, "year": basin_df["year"].values}
            )
            basin_extents_list.append(
                {basin_name: basin_df["extent"].values, "year": basin_df["year"].values}
            )
            continue

        print(f"  [proc] {i:03d}/{n_total:03d} {basin_name}")

        basin_gdf = basins_gdf.loc[basins_gdf["NAME"] == basin_name]
        try:
            dem_basin = dem.rio.clip(basin_gdf.geometry, basin_gdf.crs, drop=True)
        except NoDataInBounds:
            print(f"  [warn] {i:03d}/{n_total:03d} {basin_name} - no DEM data in bounds, skipping")
            continue

        years = []
        percentiles_90 = []
        extents = []

        for f in tqdm(ice_extent_fpaths, desc=f"  Years ({basin_name})", leave=False):
            year = int(os.path.basename(f).split("_")[-1].split(".")[0])

            ice_extent = (
                rxr.open_rasterio(f, masked=True)
                .squeeze()
                .rio.write_crs(3413)
                .rio.clip(basin_gdf.geometry, basin_gdf.crs, drop=True)
            )
            dem_basin_ice = dem_basin.where(ice_extent == 1)

            vals = dem_basin_ice.values.ravel()
            vals = vals[~np.isnan(vals)]

            if len(vals) == 0:
                percentile_90 = np.nan
                extent = 0.0
            else:
                percentile_90 = np.percentile(vals, 90)
                extent = len(vals) * pixel_area  # m²

            years.append(year)
            percentiles_90.append(percentile_90)
            extents.append(extent)

        basin_df = pd.DataFrame({"year": years, "percentile_90": percentiles_90, "extent": extents})
        basin_df.to_csv(out_fpath, index=False)
        basin_percentiles_list.append({basin_name: percentiles_90, "year": years})
        basin_extents_list.append({basin_name: extents, "year": years})

    # Combine all basins into two wide DataFrames (snowlines and extents) indexed by year
    def _combine(records):
        combined = {}
        index_years = None
        for entry in records:
            entry = dict(entry)  # copy so pop doesn't mutate original
            year_vals = entry.pop("year")
            if index_years is None:
                index_years = year_vals
            name, vals = next(iter(entry.items()))
            combined[name] = vals
        return combined, index_years

    snowlines, index_years = _combine(basin_percentiles_list)
    extents, _ = _combine(basin_extents_list)

    if snowlines:
        pd.DataFrame(snowlines, index=index_years).to_csv(out_dir / "_all_basins_snowlines.csv")
        print(f"\n[save] {out_dir / '_all_basins_snowlines.csv'}")
    if extents:
        pd.DataFrame(extents, index=index_years).to_csv(out_dir / "_all_basins_extents.csv")
        print(f"[save] {out_dir / '_all_basins_extents.csv'}")

    print("\nDone.")


if __name__ == "__main__":
    main()
