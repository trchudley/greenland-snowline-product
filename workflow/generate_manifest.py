"""
generate_manifest.py
---------------------
Scans ../data/rasters/ for year subdirectories and writes
../data/rasters/manifest.json listing the available years.

Run this after adding new raster outputs so the website
picks up the new years automatically.

    python generate_manifest.py
"""

import json
import os
from pathlib import Path

RASTERS_DIR = Path(__file__).parent.parent / "data" / "rasters"


def main() -> None:
    if not RASTERS_DIR.exists():
        print(f"[error] Rasters directory not found: {RASTERS_DIR}")
        return

    years = sorted(
        int(d.name)
        for d in RASTERS_DIR.iterdir()
        if d.is_dir() and d.name.isdigit()
    )

    manifest = {"years": years}
    out_path = RASTERS_DIR / "manifest.json"
    with open(out_path, "w") as f:
        json.dump(manifest, f)

    print(f"Written {out_path} with {len(years)} year(s): {years[0]}–{years[-1]}")


if __name__ == "__main__":
    main()
