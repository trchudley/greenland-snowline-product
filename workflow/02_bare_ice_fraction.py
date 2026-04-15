"""
02_bare_ice_fraction.py

For each year in YEARS, compute:
  - bare ice fraction (bare_ice_fraction_YYYY.tif)
  - maximum ice extent mask (max_ice_extent_YYYY.tif)

following the methodology of Ryan et al. 2019:
  - JJA (June 1 - August 31) daily MODIS albedo (MOD10A1)
  - Temporal gap-filling: fill NaN pixels from ±1, ±2, ±3 calendar days
  - Bare ice threshold: albedo <= 55 (scaled units, i.e. 0.55)
  - Bare ice presence fraction = bare ice days / valid observation days
  - Maximum ice extent = pixels with presence fraction >= 0.1

Outputs are written to data/YYYY/.
Years whose output files already exist are skipped.
"""

import glob
import os

import numpy as np
import pandas as pd
import xarray as xr
import rioxarray  # noqa: F401 — registers the .rio accessor

YEARS = range(2001, 2026)
MODIS_DIR = "../MOD10A1.061"
OUTPUT_DIR = "../data/rasters"


def output_paths(year: int) -> tuple[str, str]:
    return (
        os.path.join(OUTPUT_DIR, str(year), f"bare_ice_fraction_{year}.tif"),
        os.path.join(OUTPUT_DIR, str(year), f"max_ice_extent_{year}.tif"),
    )


def process_year(year: int) -> None:
    print(f"[{year}] Processing...")

    # --- locate input files ---------------------------------------------------
    fdir = os.path.join(MODIS_DIR, str(year))
    fpaths = glob.glob(os.path.join(fdir, "*.tif"))
    fpaths = [
        f for f in fpaths
        if int(f"{year}0601") <= int(os.path.basename(f)[:8]) <= int(f"{year}0831")
    ]
    fpaths = sorted(fpaths)

    if not fpaths:
        print(f"[{year}] No input files found, skipping.")
        return

    print(f"[{year}] Found {len(fpaths)} input files (JJA).")

    # --- load and stack -------------------------------------------------------
    print(f"[{year}] Loading and stacking files...")
    times = [pd.to_datetime(os.path.basename(f)[:8], format="%Y%m%d") for f in fpaths]
    arrays = [
        xr.open_dataarray(f, engine="rasterio", chunks="auto").squeeze("band", drop=True)
        for f in fpaths
    ]
    ds = xr.concat(arrays, dim=xr.Variable("time", times))
    ds = ds.rio.write_crs(arrays[0].rio.crs)
    ds = ds.rio.write_transform(arrays[0].rio.transform())
    ds = ds.where(ds != 255)  # no-data → NaN
    print(f"[{year}] Computing into memory...")
    ds = ds.compute()
    print(f"[{year}] Loaded. Array shape: {ds.shape}")

    # --- temporal gap filling -------------------------------------------------
    print(f"[{year}] Applying temporal gap filling (±1, ±2, ±3 days)...")
    ds_filled = ds.copy()
    for offset in [1, -1, 2, -2, 3, -3]:
        print(f"[{year}]   Gap filling with offset {offset:+d} day(s)...")
        shifted_times = ds_filled.time + pd.Timedelta(days=offset)
        shifted = (
            ds
            .reindex(time=shifted_times.values)
            .assign_coords(time=ds_filled.time.values)
        )
        ds_filled = ds_filled.fillna(shifted)
    print(f"[{year}] Gap filling complete.")

    # --- bare ice presence fraction ------------------------------------------
    print(f"[{year}] Calculating bare ice presence fraction...")
    bare_ice_presence = xr.where(
        ds_filled > 55, 0,
        xr.where(ds_filled <= 55, 1, np.nan)
    )
    bare_ice_fraction = (
        bare_ice_presence.sum(dim="time")
        / bare_ice_presence.notnull().sum(dim="time")
    ).astype("float32")

    # --- maximum ice extent mask ---------------------------------------------
    print(f"[{year}] Calculating maximum ice extent mask...")
    max_ice_extent = xr.where(
        bare_ice_fraction >= 0.1, 1,
        xr.where(bare_ice_fraction < 0.1, 0, 255)
    )

    # --- write outputs --------------------------------------------------------
    out_dir = os.path.join(OUTPUT_DIR, str(year))
    os.makedirs(out_dir, exist_ok=True)

    bif_path, mie_path = output_paths(year)
    print(f"[{year}] Writing bare ice fraction to {bif_path}...")
    bare_ice_fraction.astype("float32").rio.to_raster(bif_path, compress="deflate", driver="COG")
    print(f"[{year}] Writing maximum ice extent to {mie_path}...")
    max_ice_extent.astype("uint8").rio.to_raster(mie_path, compress="deflate", driver="COG")
    print(f"[{year}] Done.")


def main() -> None:
    years = list(YEARS)
    print(f"Processing {len(years)} year(s): {years[0]}–{years[-1]}")
    for year in years:
        bif_path, mie_path = output_paths(year)
        if os.path.exists(bif_path) and os.path.exists(mie_path):
            print(f"[{year}] Output files already exist, skipping.")
            continue
        process_year(year)


if __name__ == "__main__":
    main()
