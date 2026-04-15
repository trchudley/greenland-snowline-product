"""
03c_region_snowlines.py
------------------------
For each Mouginot region, compute the 90th-percentile snowline elevation
and total bare-ice extent from the maximum annual ice extent rasters.

Regions are derived by dissolving the Mouginot basin shapefile on its
REGION column, so all basins within a region are treated as one polygon.

Inputs
------
- Geoid-corrected ArcticDEM mosaic (500 m, EPSG:3413)
    ../data/supporting/arcticdem_v4.1_500m_geoid_corrected.tif
- Max annual ice extent rasters
    ../data/<year>/max_ice_extent_<year>.tif
- PROMICE 2022 ice mask (vector)
- Mouginot basin shapefile (REGION column used for grouping)

Outputs
-------
- Per-region CSV : ../data/regions/<REGION>.csv
- Combined CSVs  : ../data/regions/_all_regions_snowlines.csv
                   ../data/regions/_all_regions_extents.csv

Edit the paths in the CONFIG section below, then run:
    python 03c_region_snowlines.py
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

    # Load vector data and dissolve basins into regions
    mask_gdf = gpd.read_file(FPATH_PROMICE_MASK).to_crs(3413)
    basins_gdf = gpd.read_file(FPATH_BASINS).to_crs(3413)
    basins_gdf = basins_gdf.clip(mask_gdf)
    regions_gdf = basins_gdf.dissolve(by="SUBREGION1").reset_index()[["SUBREGION1", "geometry"]]

    # Load DEM
    dem = rxr.open_rasterio(str(FPATH_DEM), masked=True).squeeze().rio.write_crs(3413)
    res_x, res_y = dem.rio.resolution()
    pixel_area = abs(res_x) * abs(res_y)  # m²

    # Output directory
    out_dir = OUTPUT_DIR / "data" / "regions"
    out_dir.mkdir(parents=True, exist_ok=True)

    region_percentiles_list = []
    region_extents_list = []
    region_names = regions_gdf.SUBREGION1.values
    n_total = len(region_names)

    for i, region_name in enumerate(tqdm(region_names, desc="Regions"), 1):

        out_fpath = out_dir / f"{region_name}.csv"

        if out_fpath.exists():
            print(f"  [skip] {i:03d}/{n_total:03d} {region_name} - already exists")
            region_df = pd.read_csv(out_fpath)
            region_percentiles_list.append(
                {region_name: region_df["percentile_90"].values, "year": region_df["year"].values}
            )
            region_extents_list.append(
                {region_name: region_df["extent"].values, "year": region_df["year"].values}
            )
            continue

        print(f"  [proc] {i:03d}/{n_total:03d} {region_name}")

        region_gdf = regions_gdf.loc[regions_gdf["SUBREGION1"] == region_name]
        try:
            dem_region = dem.rio.clip(region_gdf.geometry, region_gdf.crs, drop=True)
        except NoDataInBounds:
            print(f"  [warn] {i:03d}/{n_total:03d} {region_name} - no DEM data in bounds, skipping")
            continue

        years = []
        percentiles_90 = []
        extents = []

        for f in tqdm(ice_extent_fpaths, desc=f"  Years ({region_name})", leave=False):
            year = int(os.path.basename(f).split("_")[-1].split(".")[0])

            ice_extent = (
                rxr.open_rasterio(f, masked=True)
                .squeeze()
                .rio.write_crs(3413)
                .rio.clip(region_gdf.geometry, region_gdf.crs, drop=True)
            )
            dem_region_ice = dem_region.where(ice_extent == 1)

            vals = dem_region_ice.values.ravel()
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

        region_df = pd.DataFrame({"year": years, "percentile_90": percentiles_90, "extent": extents})
        region_df.to_csv(out_fpath, index=False)
        region_percentiles_list.append({region_name: percentiles_90, "year": years})
        region_extents_list.append({region_name: extents, "year": years})

    # Combine all regions into two wide DataFrames indexed by year
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

    snowlines, index_years = _combine(region_percentiles_list)
    extents, _ = _combine(region_extents_list)

    if snowlines:
        pd.DataFrame(snowlines, index=index_years).to_csv(out_dir / "_all_regions_snowlines.csv")
        print(f"\n[save] {out_dir / '_all_regions_snowlines.csv'}")
    if extents:
        pd.DataFrame(extents, index=index_years).to_csv(out_dir / "_all_regions_extents.csv")
        print(f"[save] {out_dir / '_all_regions_extents.csv'}")

    print("\nDone.")


if __name__ == "__main__":
    main()
