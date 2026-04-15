"""
01b_process_modis_artdata_download.py
--------------------------------------
Post-process locally-downloaded MOD10A1.061 HDF files for a configurable
range of years and day-of-year dates. This script is intended as an
alternative to 01a_download_modis_mpc.py for cases where the Planetary
Computer collection is unavailable (e.g. 2024 onwards).

Expected raw data layout:
    <repo_root>/MOD10A1.061_raw/<year>/MOD10A1.A<YYYYDDD>.<tile>.061.*.hdf

Output structure (same as 01a):
    <repo_root>/MOD10A1.061/<year>/YYYYMMDD.tif

A spatial reference raster is required at:
    <repo_root>/MOD10A1.061/resample_ref.tif
This is used to define the target CRS, extent, and resolution via
rioxarray's reproject_match().

Edit the constants in the CONFIG section below, then run:
    python 01b_process_modis_artdata_download.py
"""

import datetime
import glob
import os
import sys
from pathlib import Path

import xarray as xr
import rioxarray as rxr
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BANDS = ["Snow_Albedo_Daily_Tile", "NDSI_Snow_Cover_Basic_QA"]

# ---------------------------------------------------------------------------
# CONFIG – edit these to change the processing range
# ---------------------------------------------------------------------------

# OUTPUT_DIR is the parent of this script's directory (i.e. repo root)
OUTPUT_DIR = Path(__file__).parent.parent.resolve()

RAW_DIR = OUTPUT_DIR / "MOD10A1.061_raw"
REF_RASTER = OUTPUT_DIR / "MOD10A1.061" / "resample_ref.tif"

YEAR_START = 2024
YEAR_END = 2025
DOY_START = "05-01"  # MM-DD, inclusive
DOY_END = "09-30"    # MM-DD, inclusive

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def to_julian(date: datetime.date) -> str:
    """Return YYYYDDD string for use in MODIS HDF file name matching."""
    return f"{date.year}{date.timetuple().tm_yday:03d}"


def process_day(
    date: datetime.date,
    raw_dir: Path,
    reproj_ref: xr.DataArray,
    output_dir: Path,
) -> bool:
    """
    Load, merge, reproject, filter, and save all HDF tiles for *date*.

    Returns True on success, False if no HDF files were found.
    """
    out_path = (
        output_dir
        / "MOD10A1.061"
        / str(date.year)
        / f"{date.strftime('%Y%m%d')}.tif"
    )

    if out_path.exists():
        print(f"  [skip] {date} - already exists")
        return True

    # Find all HDF tiles for this date
    pattern = str(raw_dir / str(date.year) / f"MOD10A1.A{to_julian(date)}.*.hdf")
    fpaths = sorted(glob.glob(pattern))

    if not fpaths:
        print(f"  [warn] {date} - no HDF files found, skipping")
        return False

    print(f"  [info] {date} - {len(fpaths)} tile(s) found, processing …")

    # Load each tile as a Dataset and merge into a single mosaic
    ds_list = []
    for fpath in fpaths:
        ds = rxr.open_rasterio(fpath, variable=BANDS).squeeze()
        ds_list.append(ds)

    ds_merged = xr.merge(ds_list)

    # Reproject to match the reference raster (CRS, extent, resolution)
    ds_reproj = ds_merged.rio.reproject_match(reproj_ref)

    # Filter: keep albedo where valid (0–100) and QA < 3; set everything
    # else to 255 to match the output convention used by 01a.
    ds_flt = (
        ds_reproj.Snow_Albedo_Daily_Tile
        .where(
            (ds_reproj.Snow_Albedo_Daily_Tile <= 100)
            & (ds_reproj.NDSI_Snow_Cover_Basic_QA < 3),
            other=255,
        )
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
    if not REF_RASTER.exists():
        print(
            f"[error] Reference raster not found: {REF_RASTER}\n"
            "        Please provide MOD10A1.061/resample_ref.tif "
            "or update REF_RASTER in the CONFIG section.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not RAW_DIR.exists():
        print(
            f"[error] Raw HDF directory not found: {RAW_DIR}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Output directory : {OUTPUT_DIR}")
    print(f"Raw HDF directory: {RAW_DIR}")
    print(f"Year range       : {YEAR_START} - {YEAR_END}")
    print(f"Day range        : {DOY_START} - {DOY_END} (each year)")

    # Load spatial reference raster once
    reproj_ref = rxr.open_rasterio(str(REF_RASTER)).rio.write_crs(3413)

    days = list(iter_days(YEAR_START, YEAR_END, DOY_START, DOY_END))
    total = len(days)
    print(f"Total days to process: {total}\n")

    for i, day in enumerate(tqdm(days, desc="Processing days"), 1):
        print(f"[{i}/{total}] {day.isoformat()}")
        try:
            process_day(day, RAW_DIR, reproj_ref, OUTPUT_DIR)
        except Exception as exc:  # noqa: BLE001
            print(f"  [error] {day.isoformat()} – {exc}", file=sys.stderr)

    print("\nDone.")


if __name__ == "__main__":
    main()
