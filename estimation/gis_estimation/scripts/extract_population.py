#!/usr/bin/env python3
"""
Nigeria Population Extractor
Uses rasterstats to sum GRID3 population raster values
within LGA and/or Federal Constituency boundary polygons.

Requirements (install via Terminal):
    pip install rasterstats geopandas pandas fiona
"""

import sys
import json
from pathlib import Path

# ── Dependency check ──────────────────────────────────────────────────────────
try:
    import geopandas as gpd
    import pandas as pd
    from rasterstats import zonal_stats
except ImportError as e:
    print(f"\nMissing library: {e}")
    print("Run this in Terminal first:")
    print("  pip install rasterstats geopandas pandas fiona\n")
    sys.exit(1)


# ── CONFIGURATION — edit these paths ─────────────────────────────────────────

# Path to your GRID3 population raster (.tif)
# Download from: https://data.grid3.org/maps/6966d625aea0488496d01debd3bb80f9/about
POPULATION_RASTER = "nga_population.tif"

# Path to your LGA boundary file (.shp or .geojson)
# Download from: https://data.grid3.org/datasets/GRID3::grid3-nga-operational-lga-boundaries/about
LGA_BOUNDARIES = "grid3_nga_boundary_vacclgas.shp"

# Path to federal constituency boundary file (.shp or .geojson) — optional
# Download from: https://opendata.sambusgeospatial.com (search "Nigeria Federal Constituencies")
CONSTITUENCY_BOUNDARIES = "nga_federal_constituencies.shp"

# Output folder (will be created if it doesn't exist)
OUTPUT_DIR = "nigeria_population_output"

# Column name in your boundary file that holds the area name
# Common values: "admin2Name", "LGA_NAME", "lganame", "NAME_2"
# Run the script once and it will print available columns if this is wrong
LGA_NAME_COLUMN = "admin2Name"
CONSTITUENCY_NAME_COLUMN = "NAME"  # adjust if needed


# ── MAIN PROCESSING ───────────────────────────────────────────────────────────

def extract_population(boundary_file: str,
                        raster_file: str,
                        name_column: str,
                        label: str) -> pd.DataFrame:
    """
    For each polygon in boundary_file, sum all raster pixel values
    that fall within it. Returns a DataFrame with names and population totals.
    """
    print(f"\n{'='*60}")
    print(f"Processing: {label}")
    print(f"  Boundaries : {boundary_file}")
    print(f"  Raster     : {raster_file}")

    # Load boundary polygons
    print("  Loading boundary shapefile...")
    gdf = gpd.read_file(boundary_file)

    # Show available columns so you can confirm the right name column
    print(f"  Available columns: {list(gdf.columns)}")

    if name_column not in gdf.columns:
        print(f"\n  WARNING: Column '{name_column}' not found.")
        print(f"  Available columns: {list(gdf.columns)}")
        print(f"  Edit the script and set the correct column name above.\n")
        # Fall back to first string column
        str_cols = [c for c in gdf.columns if gdf[c].dtype == object]
        name_column = str_cols[0] if str_cols else gdf.columns[0]
        print(f"  Using '{name_column}' as name column instead.")

    # Reproject to WGS84 if needed (GRID3 rasters use EPSG:4326)
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        print(f"  Reprojecting from {gdf.crs} to EPSG:4326...")
        gdf = gdf.to_crs(epsg=4326)

    total = len(gdf)
    print(f"  Found {total} polygons. Running zonal statistics (this may take a few minutes)...")

    # Core calculation: sum all population pixels within each polygon
    stats = zonal_stats(
        gdf,
        raster_file,
        stats=["sum", "count", "nodata"],
        nodata=-9999,          # typical GRID3 nodata value; change if needed
        all_touched=False,     # True = include pixels that merely touch boundary
        geojson_out=False
    )

    # Build results DataFrame
    results = []
    for i, (row, stat) in enumerate(zip(gdf.itertuples(), stats)):
        pop = stat.get("sum") or 0
        results.append({
            "name":            getattr(row, name_column, f"Area_{i}"),
            "population":      round(pop),
            "pixels_counted":  stat.get("count", 0),
            "pixels_nodata":   stat.get("nodata", 0),
        })

    df = pd.DataFrame(results)
    df = df.sort_values("population", ascending=False).reset_index(drop=True)

    # Summary
    total_pop = df["population"].sum()
    print(f"  Done. Total population across all {label}: {total_pop:,.0f}")
    print(f"  Areas processed: {len(df)}")
    print(f"\n  Top 5 by population:")
    print(df.head(5).to_string(index=False))

    return df


def save_outputs(df: pd.DataFrame, label: str, output_dir: Path):
    """Save results as CSV and GeoJSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_label = label.lower().replace(" ", "_")

    # CSV
    csv_path = output_dir / f"{safe_label}_population.csv"
    df.to_csv(csv_path, index=False)
    print(f"\n  Saved CSV : {csv_path}")

    # JSON (useful for web/dashboard use)
    json_path = output_dir / f"{safe_label}_population.json"
    df.to_json(json_path, orient="records", indent=2)
    print(f"  Saved JSON: {json_path}")


def main():
    output_dir = Path(OUTPUT_DIR)

    # ── LGA extraction ────────────────────────────────────────────────────────
    if Path(LGA_BOUNDARIES).exists() and Path(POPULATION_RASTER).exists():
        lga_df = extract_population(
            boundary_file=LGA_BOUNDARIES,
            raster_file=POPULATION_RASTER,
            name_column=LGA_NAME_COLUMN,
            label="LGAs"
        )
        save_outputs(lga_df, "lga", output_dir)
    else:
        missing = []
        if not Path(LGA_BOUNDARIES).exists():
            missing.append(f"  LGA boundary file not found: {LGA_BOUNDARIES}")
        if not Path(POPULATION_RASTER).exists():
            missing.append(f"  Raster file not found: {POPULATION_RASTER}")
        print("\nSkipping LGA extraction — missing files:")
        print("\n".join(missing))

    # ── Federal Constituency extraction ───────────────────────────────────────
    if Path(CONSTITUENCY_BOUNDARIES).exists() and Path(POPULATION_RASTER).exists():
        const_df = extract_population(
            boundary_file=CONSTITUENCY_BOUNDARIES,
            raster_file=POPULATION_RASTER,
            name_column=CONSTITUENCY_NAME_COLUMN,
            label="Federal Constituencies"
        )
        save_outputs(const_df, "federal_constituency", output_dir)
    else:
        if not Path(CONSTITUENCY_BOUNDARIES).exists():
            print(f"\nSkipping constituency extraction — file not found: {CONSTITUENCY_BOUNDARIES}")
            print("Download from: https://opendata.sambusgeospatial.com")

    print(f"\n{'='*60}")
    print(f"All outputs saved to: {Path(OUTPUT_DIR).resolve()}")
    print("Open the CSV files in Excel or Numbers.")


if __name__ == "__main__":
    main()