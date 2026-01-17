#!/usr/bin/env python3
"""
Build a quadkey -> H3 weight matrix by geometric intersection.
"""

from pathlib import Path
import os
import time
from concurrent.futures import ProcessPoolExecutor

import h3
import mercantile
import polars as pl
from shapely.geometry import Polygon, box

# Configuration
REPO_ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = REPO_ROOT / "data" / "internet"
OUTPUT_MATRIX = REPO_ROOT / "matrix" / "outputs" / "matrix_quadkey_h3_weights.parquet"

H3_RES = 8
K_RING_SIZE = 1
MAX_WORKERS = max(1, (os.cpu_count() or 1) - 1)
BATCH_SIZE = 500


def process_quadkey_batch(quadkeys: list[str]) -> list[dict[str, object]]:
    """Process a batch of quadkeys and calculate H3 intersections."""
    batch_results = []
    
    for qk in quadkeys:
        try:
            tile = mercantile.quadkey_to_tile(qk)
            bounds = mercantile.bounds(tile)
            qk_geom = box(bounds.west, bounds.south, bounds.east, bounds.north)
            qk_area = qk_geom.area

            center_lng = (bounds.west + bounds.east) / 2
            center_lat = (bounds.south + bounds.north) / 2
            center_h3 = h3.latlng_to_cell(center_lat, center_lng, H3_RES)
            candidates = h3.grid_disk(center_h3, K_RING_SIZE)

            for h3_idx in candidates:
                boundary = h3.cell_to_boundary(h3_idx)
                boundary_lonlat = [(p[1], p[0]) for p in boundary]
                h3_poly = Polygon(boundary_lonlat)

                if qk_geom.intersects(h3_poly):
                    intersection = qk_geom.intersection(h3_poly)
                    weight = intersection.area / qk_area

                    if weight > 0.001:
                        batch_results.append({
                            "quadkey": qk,
                            "h3_index": h3_idx,
                            "weight": weight
                        })
        except Exception:
            continue
            
    return batch_results


def get_all_unique_quadkeys(folder_path: Path) -> list[str]:
    """Scan folder and extract unique quadkeys from all parquet files."""
    print(f"Scanning folder: {folder_path}")
    
    parquet_files = [
        p for p in folder_path.glob("*.parquet")
        if "h3res" not in p.stem
        and "matrix_quadkey_h3_weights" not in p.stem
        and "internet_speed_h3_res8" not in p.stem
    ]
    
    if not parquet_files:
        print("Error: No parquet files found to analyze")
        return []
        
    try:
        q = pl.scan_parquet([str(p) for p in parquet_files])
        unique_qks = q.select("quadkey").unique().collect()
        count = len(unique_qks)
        print(f"  Found {count:,} unique quadkeys to process")
        return unique_qks["quadkey"].to_list()
    except Exception as exc:
        print(f"Error reading files: {exc}")
        return []


def calculate_intersection_weights(quadkeys: list[str]) -> pl.DataFrame:
    """Calculate quadkey -> H3 intersection weights with parallel processing."""
    results = []
    total = len(quadkeys)
    start_time = time.time()

    print(f"Calculating intersections for {total:,} quadkeys...")
    batches = [quadkeys[i : i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    processed = 0

    if MAX_WORKERS == 1:
        for batch in batches:
            results.extend(process_quadkey_batch(batch))
            processed += len(batch)
            if processed % 5000 == 0 or processed == total:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed else 0
                print(f"  Progress: {processed:,}/{total:,} ({rate:.0f} qk/sec)", end="\r")
    else:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for batch_results in executor.map(process_quadkey_batch, batches, chunksize=1):
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
    print("QUADKEY -> H3 WEIGHT MATRIX GENERATION")
    print("=" * 80)
    
    # Check if matrix already exists
    if OUTPUT_MATRIX.exists():
        size_mb = OUTPUT_MATRIX.stat().st_size / 1024**2
        df = pl.read_parquet(OUTPUT_MATRIX)
        print(f"Matrix already exists: {len(df):,} rows ({size_mb:.1f} MB)")
        print(f"  Unique quadkeys: {df['quadkey'].n_unique():,}")
        print(f"  Unique H3 cells: {df['h3_index'].n_unique():,}")
        return

    # Get unique quadkeys
    all_qks = get_all_unique_quadkeys(INPUT_DIR)
    if not all_qks:
        return

    # Calculate intersections
    df_matrix = calculate_intersection_weights(all_qks)

    if df_matrix.is_empty():
        print("Error: No intersections found")
        return

    print(f"\nMatrix statistics:")
    print(f"  Total mappings: {len(df_matrix):,}")
    print(f"  Unique quadkeys: {df_matrix['quadkey'].n_unique():,}")
    print(f"  Unique H3 cells: {df_matrix['h3_index'].n_unique():,}")

    print("\nNormalizing weights (sum = 1.0 per quadkey)...")
    df_matrix = df_matrix.with_columns(
        (pl.col("weight") / pl.col("weight").sum().over("quadkey")).alias("weight")
    )

    print(f"\nSaving matrix to: {OUTPUT_MATRIX}...")
    df_matrix.write_parquet(OUTPUT_MATRIX)
    
    size_mb = OUTPUT_MATRIX.stat().st_size / 1024**2
    print(f"Matrix saved successfully ({size_mb:.1f} MB)")
    print("=" * 80)


if __name__ == "__main__":
    main()
