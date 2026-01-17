#!/usr/bin/env python3
"""
ETL Health Data - Convert European healthcare accessibility raster to H3 hexagons.
"""

from pathlib import Path
import time

import polars as pl
import rasterio
import numpy as np
import h3
from pyproj import Transformer

INPUT_FILE = "data/health/euro_access_healthcare_2023_100m_v2025_11.tif"
OUTPUT_DIR = "data/health"
OUTPUT_FILE = f"{OUTPUT_DIR}/euro_access_healthcare_2023_h3_res8.parquet"
H3_RESOLUTION = 8
CHUNK_ROWS = 1000


def process_raster_to_h3(src, transformer):
    """Process entire raster and aggregate to H3."""
    height, width = src.shape
    nodata = src.nodata
    transform = src.transform
    
    print(f"Processing raster: {height:,} x {width:,} pixels")
    
    # Dictionary to accumulate values per H3 cell
    h3_values = {}
    total_pixels = 0
    start_time = time.time()
    
    # Process in chunks for memory efficiency
    num_chunks = (height + CHUNK_ROWS - 1) // CHUNK_ROWS
    
    for chunk_idx in range(num_chunks):
        row_start = chunk_idx * CHUNK_ROWS
        row_end = min(row_start + CHUNK_ROWS, height)
        
        # Read chunk
        window = rasterio.windows.Window(0, row_start, width, row_end - row_start)
        band1 = src.read(1, window=window)
        band2 = src.read(2, window=window)
        
        # Find valid pixels
        valid_mask = band1 != nodata
        valid_count = valid_mask.sum()
        
        if valid_count == 0:
            continue
        
        # Get row/col indices of valid pixels
        local_rows, cols = np.where(valid_mask)
        global_rows = local_rows + row_start
        
        # Get pixel coordinates
        xs, ys = rasterio.transform.xy(transform, global_rows, cols)
        
        # Transform to WGS84
        lons, lats = transformer.transform(xs, ys)
        
        # Convert to H3 and accumulate values
        for i in range(len(lons)):
            try:
                h3_index = h3.latlng_to_cell(lats[i], lons[i], H3_RESOLUTION)
                if h3_index not in h3_values:
                    h3_values[h3_index] = {'band1': [], 'band2': []}
                h3_values[h3_index]['band1'].append(int(band1[local_rows[i], cols[i]]))
                h3_values[h3_index]['band2'].append(int(band2[local_rows[i], cols[i]]))
                total_pixels += 1
            except:
                continue
        
        # Progress update
        if (chunk_idx + 1) % 10 == 0 or (chunk_idx + 1) == num_chunks:
            progress = (chunk_idx + 1) / num_chunks * 100
            elapsed = time.time() - start_time
            print(f"  Progress: {progress:.1f}% | H3 cells: {len(h3_values):,} | Time: {elapsed:.0f}s")
    
    return h3_values, total_pixels


def aggregate_h3_values(h3_values):
    """Aggregate pixel values per H3 cell."""
    print(f"\nAggregating {len(h3_values):,} H3 cells...")
    
    records = []
    for h3_index, values in h3_values.items():
        vals1 = np.array(values['band1'])
        vals2 = np.array(values['band2'])
        
        records.append({
            'h3_index': h3_index,
            'accessibility_mean': float(np.mean(vals1)),
            'accessibility_median': float(np.median(vals1)),
            'accessibility_min': int(np.min(vals1)),
            'accessibility_max': int(np.max(vals1)),
            'accessibility_std': float(np.std(vals1)),
            'band2_sum': int(np.sum(vals2)),
            'band2_mean': float(np.mean(vals2)),
            'band2_median': float(np.median(vals2)),
            'pixel_count': len(vals1)
        })
    
    # Create Polars DataFrame
    df = pl.DataFrame(records)
    
    # Add H3 metadata using batch conversion
    h3_indices = df['h3_index'].to_list()
    coords = [h3.cell_to_latlng(idx) for idx in h3_indices]
    
    df = df.with_columns([
        pl.Series('lat', [c[0] for c in coords]),
        pl.Series('lon', [c[1] for c in coords]),
        pl.lit(H3_RESOLUTION).alias('h3_resolution')
    ])
    
    # Reorder columns
    cols = ['h3_index', 'h3_resolution', 'lat', 'lon', 'accessibility_mean', 
            'accessibility_median', 'accessibility_min', 'accessibility_max', 
            'accessibility_std', 'band2_sum', 'band2_mean', 'band2_median', 'pixel_count']
    
    return df.select(cols)


def main():
    start_time = time.time()
    
    print("=" * 80)
    print("ETL HEALTH - RASTER TO H3 AGGREGATION")
    print("=" * 80)
    
    if Path(OUTPUT_FILE).exists():
        df = pl.read_parquet(OUTPUT_FILE)
        print(f"Output already exists: {len(df):,} rows")
        return
    
    print(f"\nInput: {INPUT_FILE}")
    
    # Read raster
    with rasterio.open(INPUT_FILE) as src:
        print(f"Bands: {src.count} - {src.descriptions}")
        print(f"CRS: {src.crs}")
        
        # Create transformer
        transformer = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)
        
        # Process raster
        h3_values, total_pixels = process_raster_to_h3(src, transformer)
    
    # Aggregate to DataFrame
    df = aggregate_h3_values(h3_values)
    
    # Save output
    print(f"\nSaving to: {OUTPUT_FILE}")
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUTPUT_FILE, compression='snappy')
    
    # Statistics
    total_time = time.time() - start_time
    size_mb = Path(OUTPUT_FILE).stat().st_size / 1024**2
    
    print(f"\n" + "=" * 80)
    print(f"COMPLETED")
    print(f"=" * 80)
    print(f"File: {OUTPUT_FILE}")
    print(f"Size: {size_mb:.1f} MB | Rows: {len(df):,}")
    print(f"Pixels: {total_pixels:,} | Time: {total_time/60:.1f} min")
    print(f"Speed: {total_pixels/total_time:,.0f} pixels/second")
    print("=" * 80)


if __name__ == "__main__":
    main()
