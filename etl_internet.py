#!/usr/bin/env python3
"""
ETL Internet - Aggregate Ookla speed data to H3 resolution 8.
Uses a precomputed quadkey->H3 weight matrix for spatial joins.
Memory-optimized version with streaming merge.
"""

from pathlib import Path
import time
import h3
import polars as pl

INPUT_DIR = "data/internet"
OUTPUT_FILE = "data/internet/internet_speed_h3_res8.parquet"
MATRIX_FILE = "matrix/outputs/matrix_quadkey_h3_weights.parquet"
H3_RESOLUTION = 8

# All available quarters
QUARTERS = [
    (2019, 1), (2019, 2), (2019, 3), (2019, 4),
    (2020, 1), (2020, 2), (2020, 3), (2020, 4),
    (2021, 1), (2021, 2), (2021, 3), (2021, 4),
    (2022, 1), (2022, 2), (2022, 3), (2022, 4),
    (2023, 1), (2023, 2), (2023, 3), (2023, 4),
    (2024, 1), (2024, 2), (2024, 3), (2024, 4),
    (2025, 1), (2025, 2), (2025, 3),
]


def process_quarter_file(file_path: Path, data_type: str, year: int, quarter: int) -> pl.DataFrame | None:
    """Process and save intermediate H3 aggregation using the matrix."""
    cache_file = file_path.parent / f"{file_path.stem}_h3res{H3_RESOLUTION}.parquet"

    if cache_file.exists():
        return cache_file

    print(f"  {year} Q{quarter} {data_type}: Processing...")

    try:
        matrix = pl.scan_parquet(MATRIX_FILE).select(["quadkey", "h3_index", "weight"])
        df = pl.scan_parquet(file_path)

        # Determine available columns
        schema = df.collect_schema()
        cols = list(schema.keys())
        avg_cols = [c for c in ["avg_d_kbps", "avg_u_kbps", "avg_lat_ms"] if c in cols]
        count_cols = [c for c in ["tests", "devices"] if c in cols]

        # Join with matrix
        joined = df.join(matrix, on="quadkey", how="inner")

        # Weighted aggregation
        weighted_exprs = [(pl.col(c) * pl.col("weight")).alias(f"w_{c}") 
                         for c in avg_cols + count_cols]
        
        grouped = (
            joined
            .with_columns(weighted_exprs)
            .group_by("h3_index")
            .agg(
                [pl.col("weight").sum().alias("weight_sum")]
                + [pl.col(f"w_{c}").sum().alias(f"sum_{c}") for c in avg_cols + count_cols]
            )
        )

        # Compute weighted averages for avg_* columns; sums for count columns
        final_exprs = []
        for c in avg_cols:
            final_exprs.append((pl.col(f"sum_{c}") / pl.col("weight_sum")).alias(c))
        for c in count_cols:
            final_exprs.append(pl.col(f"sum_{c}").alias(c))

        h3_agg = grouped.with_columns(final_exprs).select(["h3_index"] + avg_cols + count_cols)

        # Rename columns
        prefix = f"{data_type}_{year}_q{quarter}"
        h3_agg_collected = h3_agg.collect()
        rename_dict = {col: f"{prefix}_{col}" for col in h3_agg_collected.columns if col != "h3_index"}
        h3_agg_collected = h3_agg_collected.rename(rename_dict)

        # Save cache
        h3_agg_collected.write_parquet(cache_file)
        print(f"    Cached to H3")
        return cache_file

    except Exception as exc:
        print(f"    Error: {exc}")
        return None


def main() -> None:
    start_time = time.time()

    print("=" * 80)
    print("ETL INTERNET - H3 AGGREGATION")
    print("=" * 80)

    if not Path(MATRIX_FILE).exists():
        print(f"Error: Matrix file not found: {MATRIX_FILE}")
        print("Please run: python matrix/quadkey_h3_matrix.py")
        return

    if Path(OUTPUT_FILE).exists():
        df = pl.read_parquet(OUTPUT_FILE)
        print(f"Output already exists: {len(df):,} rows, {df.width} columns")
        return

    print(f"\nProcessing {len(QUARTERS)} quarters x 2 types = {len(QUARTERS) * 2} files")

    # Process all quarters (returns file paths)
    cache_files = []
    for year, quarter in QUARTERS:
        for data_type in ["fixed", "mobile"]:
            file_name = f"{year}_q{quarter}_{data_type}.parquet"
            file_path = Path(INPUT_DIR) / file_name

            if file_path.exists():
                cache_file = process_quarter_file(file_path, data_type, year, quarter)
                if cache_file is not None:
                    cache_files.append(cache_file)

    if not cache_files:
        print("Error: No data to process")
        return

    print(f"\nMerging {len(cache_files)} cached files using streaming...")

    # Use streaming lazy merge
    print("  Scanning all cached files...")
    lazy_dfs = [pl.scan_parquet(f) for f in cache_files]
    
    # Get all unique H3 cells efficiently
    print("  Finding unique H3 cells...")
    all_h3 = pl.concat([df.select("h3_index") for df in lazy_dfs]).unique().collect()
    print(f"  Total unique H3 cells: {len(all_h3):,}")

    # Merge in batches to reduce memory
    print("  Merging columns...")
    result = all_h3.lazy()
    
    for i, ldf in enumerate(lazy_dfs):
        result = result.join(ldf, on="h3_index", how="left")
        if (i + 1) % 10 == 0:
            print(f"    Joined {i + 1}/{len(lazy_dfs)} files...")
    
    # Collect result
    print("  Collecting result...")
    result = result.collect()

    print(f"\nCalculating aggregated metrics...")
    
    # Add H3 coordinates (batch conversion)
    h3_indices = result["h3_index"].to_list()
    coords = [h3.cell_to_latlng(idx) for idx in h3_indices]
    
    result = result.with_columns([
        pl.Series("lat", [c[0] for c in coords]),
        pl.Series("lon", [c[1] for c in coords]),
        pl.lit(H3_RESOLUTION).alias("h3_resolution")
    ])

    # Calculate 2023 and total averages for fixed
    fixed_2023_download = [col for col in result.columns if col.startswith('fixed_2023_') and 'avg_d_kbps' in col]
    fixed_2023_upload = [col for col in result.columns if col.startswith('fixed_2023_') and 'avg_u_kbps' in col]
    fixed_2023_latency = [col for col in result.columns if col.startswith('fixed_2023_') and 'avg_lat_ms' in col]
    
    fixed_all_download = [col for col in result.columns if col.startswith('fixed_') and 'avg_d_kbps' in col and '_q' in col]
    fixed_all_upload = [col for col in result.columns if col.startswith('fixed_') and 'avg_u_kbps' in col and '_q' in col]
    fixed_all_latency = [col for col in result.columns if col.startswith('fixed_') and 'avg_lat_ms' in col and '_q' in col]
    
    # Calculate 2023 and total averages for mobile
    mobile_2023_download = [col for col in result.columns if col.startswith('mobile_2023_') and 'avg_d_kbps' in col]
    mobile_2023_upload = [col for col in result.columns if col.startswith('mobile_2023_') and 'avg_u_kbps' in col]
    mobile_2023_latency = [col for col in result.columns if col.startswith('mobile_2023_') and 'avg_lat_ms' in col]
    
    mobile_all_download = [col for col in result.columns if col.startswith('mobile_') and 'avg_d_kbps' in col and '_q' in col]
    mobile_all_upload = [col for col in result.columns if col.startswith('mobile_') and 'avg_u_kbps' in col and '_q' in col]
    mobile_all_latency = [col for col in result.columns if col.startswith('mobile_') and 'avg_lat_ms' in col and '_q' in col]
    
    # Add aggregated columns
    new_cols = []
    
    if fixed_2023_download:
        new_cols.append(pl.concat_list([pl.col(c) for c in fixed_2023_download]).list.mean().alias("fixed_download_2023"))
    if fixed_2023_upload:
        new_cols.append(pl.concat_list([pl.col(c) for c in fixed_2023_upload]).list.mean().alias("fixed_upload_2023"))
    if fixed_2023_latency:
        new_cols.append(pl.concat_list([pl.col(c) for c in fixed_2023_latency]).list.mean().alias("fixed_latency_2023"))
    
    if fixed_all_download:
        new_cols.append(pl.concat_list([pl.col(c) for c in fixed_all_download]).list.mean().alias("fixed_download_total"))
    if fixed_all_upload:
        new_cols.append(pl.concat_list([pl.col(c) for c in fixed_all_upload]).list.mean().alias("fixed_upload_total"))
    if fixed_all_latency:
        new_cols.append(pl.concat_list([pl.col(c) for c in fixed_all_latency]).list.mean().alias("fixed_latency_total"))
    
    if mobile_2023_download:
        new_cols.append(pl.concat_list([pl.col(c) for c in mobile_2023_download]).list.mean().alias("mobile_download_2023"))
    if mobile_2023_upload:
        new_cols.append(pl.concat_list([pl.col(c) for c in mobile_2023_upload]).list.mean().alias("mobile_upload_2023"))
    if mobile_2023_latency:
        new_cols.append(pl.concat_list([pl.col(c) for c in mobile_2023_latency]).list.mean().alias("mobile_latency_2023"))
    
    if mobile_all_download:
        new_cols.append(pl.concat_list([pl.col(c) for c in mobile_all_download]).list.mean().alias("mobile_download_total"))
    if mobile_all_upload:
        new_cols.append(pl.concat_list([pl.col(c) for c in mobile_all_upload]).list.mean().alias("mobile_upload_total"))
    if mobile_all_latency:
        new_cols.append(pl.concat_list([pl.col(c) for c in mobile_all_latency]).list.mean().alias("mobile_latency_total"))
    
    result = result.with_columns(new_cols)
    
    # Select only metadata and aggregated columns (drop quarter-level columns)
    meta_cols = ["h3_index", "h3_resolution", "lat", "lon"]
    agg_cols = [col for col in result.columns if col.endswith('_2023') or col.endswith('_total')]
    
    result = result.select(meta_cols + agg_cols)

    print(f"\nSaving to: {OUTPUT_FILE}")
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    result.write_parquet(OUTPUT_FILE, compression="snappy")

    size_mb = Path(OUTPUT_FILE).stat().st_size / 1024**2
    elapsed = time.time() - start_time

    print(f"\n" + "=" * 80)
    print(f"ETL INTERNET COMPLETED")
    print(f"=" * 80)
    print(f"Output: {OUTPUT_FILE}")
    print(f"Size: {size_mb:.1f} MB")
    print(f"Rows: {len(result):,}")
    print(f"Columns: {result.width}")
    print(f"Time: {elapsed:.1f}s ({elapsed / 60:.1f} minutes)")
    print("=" * 80)


if __name__ == "__main__":
    main()
