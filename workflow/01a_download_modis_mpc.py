"""
01a_download_modis_mpc.py
-----------------
Download MOD10A1.061 snow albedo tiles from the Microsoft Planetary Computer
for a configurable range of years and day-of-year dates.

Output structure:
    <output_dir>/MOD10A1.061/<year>/YYYYMMDD.tif

Edit the constants in the CONFIG section below to adjust the date range and
output directory, then run:  python 01a_download_modis_mpc.py

NB: MPC appears to have stopped updating the MOD10A1.061 collection after 2025, 
so an alternative approach may be needed for more recent data.
"""

import datetime
import os
import sys
import time
from pathlib import Path

import planetary_computer
import pystac_client
import stackstac
import rioxarray  # noqa: F401
import geopandas as gpd
from rasterio.enums import Resampling

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
COLLECTION = "modis-10A1-061"
ASSETS = ["Snow_Albedo_Daily_Tile", "NDSI_Snow_Cover_Basic_QA"]

# Path to promice-2022 ice mask file to help constrain download bounds.
MASK_VECTOR_FPATH = (
    "/Users/ju24811/Library/CloudStorage/OneDrive-UniversityofBristol/"
    "Data/promice_mask/06-PROMICE-2022-IceMask-Nunatak-polygon.gpkg"
)

TARGET_CRS_EPSG = 3413
TARGET_RES = 500  # metres

# ---------------------------------------------------------------------------
# CONFIG – edit these to change the download range
# ---------------------------------------------------------------------------

# OUTPUT_DIR is parent of this directory (i.e. repo root)
OUTPUT_DIR = Path(__file__).parent.parent.resolve()

YEAR_START = 2001
# YEAR_START = 2025
YEAR_END = 2023
DOY_START = "05-01"  # MM-DD, inclusive
DOY_END = "09-30"    # MM-DD, inclusive

# Refresh the STAC client after this many seconds (Planetary Computer tokens
# expire after ~45 min; refresh well before that).
CLIENT_TTL_SECONDS = 30 * 60  # 30 minutes

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_client() -> pystac_client.Client:
    """Open a fresh, signed Planetary Computer STAC client."""
    return pystac_client.Client.open(
        STAC_URL,
        modifier=planetary_computer.sign_inplace,
    )


def iter_days(year_start: int, year_end: int, doy_start: str, doy_end: str):
    """
    Yield datetime.date objects for every day in [year_start, year_end]
    whose month-day falls within [doy_start, doy_end] (both inclusive).

    doy_start / doy_end are 'MM-DD' strings (e.g. '05-01').
    """
    start_md = tuple(int(x) for x in doy_start.split("-"))
    end_md = tuple(int(x) for x in doy_end.split("-"))

    for year in range(year_start, year_end + 1):
        start_date = datetime.date(year, *start_md)
        end_date = datetime.date(year, *end_md)
        current = start_date
        while current <= end_date:
            yield current
            current += datetime.timedelta(days=1)


def download_day(
    client: pystac_client.Client,
    date: datetime.date,
    bounds_3413: tuple,
    bounds_4326: tuple,
    output_dir: Path,
) -> bool:
    """
    Download, mosaic, filter, and save the MOD10A1.061 scene for *date*.

    Returns True on success, False if no STAC items were found.
    """
    date_str = date.strftime("%Y-%m-%d")
    out_path = output_dir / "MOD10A1.061" / str(date.year) / f"{date.strftime('%Y%m%d')}.tif"

    if os.path.exists(out_path):
        print(f"  [skip] {date_str} - already exists")
        return True

    # Search
    search = client.search(
        max_items=25,
        collections=COLLECTION,
        bbox=list(bounds_4326),
        datetime=f"{date_str}/{date_str}",
        # query={"platform": {"eq": "terra"}},  # filtering based on ID happens below instead
    )
    items = list(search.items())

    if not items:
        print(f"  [warn] {date_str} - no STAC items found, skipping")
        return False
    
    # filter items to only where id begins with "MOD", not "MYD" (terra vs aqua)
    items = [item for item in items if item.id.startswith("MOD")]

    print(f"  [info] {date_str} - {len(items)} item(s) found, stacking …")

    # Stack → mosaic
    ds = stackstac.stack(
        items,
        assets=ASSETS,
        epsg=TARGET_CRS_EPSG,
        bounds=(bounds_3413[0], bounds_3413[1], bounds_3413[2], bounds_3413[3]),
        resolution=TARGET_RES,
        resampling=Resampling.nearest,
        chunksize="auto",
        properties=False,
    )
    ds = stackstac.mosaic(ds, dim="time")
    ds = ds.compute()

    # Filter: keep albedo where it is valid (0–100) and QA < 3; else 255
    ds_flt = (
        ds.sel(band=["Snow_Albedo_Daily_Tile"])
        .where(
            (ds.sel(band="Snow_Albedo_Daily_Tile") <= 100)
            & (ds.sel(band="NDSI_Snow_Cover_Basic_QA") < 3),
            other=255,
        )
        .squeeze()
        .astype("uint8")
    )

    # Save
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ds_flt.rio.to_raster(str(out_path), compress="deflate")
    print(f"  [save] {out_path}")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    output_dir = OUTPUT_DIR

    print(f"Output directory : {output_dir}")
    print(f"Year range       : {YEAR_START} - {YEAR_END}")
    print(f"Day range        : {DOY_START} - {DOY_END} (each year)")

    # Load mask geometry once
    gdf_3413 = gpd.read_file(MASK_VECTOR_FPATH)
    gdf_4326 = gdf_3413.to_crs("EPSG:4326")
    bounds_3413 = tuple(gdf_3413.total_bounds)
    bounds_4326 = tuple(gdf_4326.total_bounds)

    # Build day list
    days = list(iter_days(YEAR_START, YEAR_END, DOY_START, DOY_END))
    total = len(days)
    print(f"Total days to process: {total}\n")

    # Init STAC client
    client = make_client()
    client_born = time.monotonic()

    for i, day in enumerate(days, 1):
        # Refresh client every CLIENT_TTL_SECONDS
        if time.monotonic() - client_born >= CLIENT_TTL_SECONDS:
            print("[info] Refreshing STAC client …")
            client = make_client()
            client_born = time.monotonic()

        print(f"[{i}/{total}] {day.isoformat()}")
        try:
            download_day(client, day, bounds_3413, bounds_4326, output_dir)
        except Exception as exc:  # noqa: BLE001
            print(f"  [error] {day.isoformat()} – {exc}", file=sys.stderr)

    print("\nDone.")


if __name__ == "__main__":
    main()
