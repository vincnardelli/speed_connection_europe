#!/usr/bin/env python3
"""
Build a grid_cell -> H3 weight matrix by geometric intersection.
For Eurostat Census Grid cells (1km x 1km in EPSG:3035).
"""

from pathlib import Path
import os
import time
from concurrent.futures import ProcessPoolExecutor

import h3
import polars as pl
import geopandas as gpd
from shapely.geometry import Polygon
from pyproj import Transformer

# Configuration
REPO_ROOT = Path(__file__).resolve().parent.parent
POPULATION_DIR = REPO_ROOT / "data" / "population"
OUTPUT_MATRIX = REPO_ROOT / "matrix" / "outputs" / "matrix_grid_h3_weights.parquet"

GPKG_FILE = POPULATION_DIR / "ESTAT_Census_2021_V2.gpkg"

H3_RES = 8
K_RING_SIZE = 1
MAX_WORKERS = max(1, (os.cpu_count() or 1) - 1)
BATCH_SIZE = 500

transformer = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)


def process_grid_batch(batch_data: list[tuple[str, object]]) -> list[dict[str, object]]:
    """Process a batch of grid cells and calculate H3 intersections."""
    batch_results = []
    
    for grid_id, grid_geom_3035 in batch_data:
        try:
            # Transform grid polygon from EPSG:3035 to EPSG:4326
            coords_3035 = list(grid_geom_3035.exterior.coords)
            coords_4326 = [transformer.transform(x, y) for x, y in coords_3035]
            grid_geom_4326 = Polygon(coords_4326)
            grid_area = grid_geom_4326.area
            
            if grid_area <= 0:
                continue
            
            # Find center to get candidate H3 cells
            centroid = grid_geom_4326.centroid
            center_h3 = h3.latlng_to_cell(centroid.y, centroid.x, H3_RES)
            candidates = h3.grid_disk(center_h3, K_RING_SIZE)
            
            # Calculate intersections
            for h3_idx in candidates:
                boundary = h3.cell_to_boundary(h3_idx)
                boundary_lonlat = [(p[1], p[0]) for p in boundary]
                h3_poly = Polygon(boundary_lonlat)
                
                if grid_geom_4326.intersects(h3_poly):
                    intersection = grid_geom_4326.intersection(h3_poly)
                    weight = intersection.area / grid_area
                    
                    if weight > 0.001:
                        batch_results.append({
                            "grid_id": grid_id,
                            "h3_index": h3_idx,
                            "weight": weight
                        })
        except Exception:
            continue
    
    return batch_results


def load_grid_cells(gpkg_path: Path) -> list[tuple[str, object]]:
    """Load grid cells from GPKG file."""
    print(f"Loading grid cells from: {gpkg_path}")
    
    if not gpkg_path.exists():
        print(f"Error: GPKG file not found: {gpkg_path}")
        return []
    
    try:
        gdf = gpd.read_file(gpkg_path)
        print(f"  Loaded {len(gdf):,} grid cells (CRS: {gdf.crs})")
        
        grid_data = [(row['GRD_ID'], row['geometry']) for _, row in gdf.iterrows()]
        return grid_data
        
    except Exception as exc:
        print(f"Error reading GPKG: {exc}")
        return []


def calculate_intersection_weights(grid_data: list[tuple[str, object]]) -> pl.DataFrame:
    """Calculate grid -> H3 intersection weights with parallel processing."""
    results = []
    total = len(grid_data)
    start_time = time.time()
    
    print(f"Calculating intersections for {total:,} grid cells...")
    batches = [grid_data[i : i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    processed = 0
    
    if MAX_WORKERS == 1:
        for batch in batches:
            results.extend(process_grid_batch(batch))
            processed += len(batch)
            if processed % 5000 == 0 or processed == total:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed else 0
                print(f"  Progress: {processed:,}/{total:,} ({rate:.0f} cells/sec)", end="\r")
    else:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for batch_results in executor.map(process_grid_batch, batches, chunksize=1):
                results.extend(batch_results)
                processed += len(batch_results)
                if processed % 5000 == 0:
                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed else 0
                    print(f"  Progress: {processed:,} mappings ({rate:.0f}/sec)", end="\r")
    
    print("\nGeometric calculation completed")
    return pl.DataFrame(results)


def main() -> None:
    OUTPUT_MATRIX.parent.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("GRID -> H3 WEIGHT MATRIX GENERATION")
    print("=" * 80)
    
    # Check if matrix already exists
    if OUTPUT_MATRIX.exists():
        size_mb = OUTPUT_MATRIX.stat().st_size / 1024**2
        df = pl.read_parquet(OUTPUT_MATRIX)
        print(f"Matrix already exists: {len(df):,} rows ({size_mb:.1f} MB)")
        print(f"  Unique grid cells: {df['grid_id'].n_unique():,}")
        print(f"  Unique H3 cells: {df['h3_index'].n_unique():,}")
        return
    
    # Load grid cells
    grid_data = load_grid_cells(GPKG_FILE)
    if not grid_data:
        return
    
    # Calculate intersections
    df_matrix = calculate_intersection_weights(grid_data)
    
    if df_matrix.is_empty():
        print("Error: No intersections found")
        return
    
    print(f"\nMatrix statistics:")
    print(f"  Total mappings: {len(df_matrix):,}")
    print(f"  Unique grid cells: {df_matrix['grid_id'].n_unique():,}")
    print(f"  Unique H3 cells: {df_matrix['h3_index'].n_unique():,}")
    
    print("\nNormalizing weights (sum = 1.0 per grid cell)...")
    df_matrix = df_matrix.with_columns(
        (pl.col("weight") / pl.col("weight").sum().over("grid_id")).alias("weight")
    )
    
    print(f"\nSaving matrix to: {OUTPUT_MATRIX}...")
    df_matrix.write_parquet(OUTPUT_MATRIX)
    
    size_mb = OUTPUT_MATRIX.stat().st_size / 1024**2
    print(f"Matrix saved successfully ({size_mb:.1f} MB)")
    print("=" * 80)


if __name__ == "__main__":
    main()
