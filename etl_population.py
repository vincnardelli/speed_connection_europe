#!/usr/bin/env python3
"""
ETL Population Data - Process Eurostat Census Grid data.
Convert to H3 resolution 8 hexagons with population aggregation using matrix-based approach.
"""

import zipfile
from pathlib import Path
import polars as pl
import geopandas as gpd
import h3
import time

OUTPUT_DIR = "data/population"
ZIP_FILE = f"{OUTPUT_DIR}/Eurostat_Census-GRID_2021_V2.2.zip"
MATRIX_FILE = "matrix/outputs/matrix_grid_h3_weights.parquet"
H3_RESOLUTION = 8
OUTPUT_H3_FILE = f"{OUTPUT_DIR}/population_census_2021_h3_res8.parquet"


def extract_census_data():
    """Extract Eurostat Census Grid data from zip."""
    zip_path = Path(ZIP_FILE)
    
    if not zip_path.exists():
        print(f"Error: Zip file not found: {ZIP_FILE}")
        print("Please download it manually from:")
        print("https://gisco-services.ec.europa.eu/census/2021/")
        return None
    
    print("=" * 80)
    print("EXTRACTING ZIP FILE")
    print("=" * 80)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            gpkg_files = [f for f in zip_ref.namelist() if f.endswith('.gpkg')]
            
            if not gpkg_files:
                print("Error: No GPKG files found in archive")
                return None
            
            print(f"Found GPKG file: {gpkg_files[0]}")
            print("Extracting...")
            zip_ref.extractall(OUTPUT_DIR)
            
            gpkg_path = Path(OUTPUT_DIR) / gpkg_files[0]
            return gpkg_path
            
    except Exception as e:
        print(f"Error during extraction: {e}")
        return None


def convert_to_h3_matrix(gpkg_path):
    """Convert GPKG to H3 resolution 8 hexagons using precomputed grid->H3 weight matrix."""
    if not gpkg_path or not gpkg_path.exists():
        print(f"Error: GPKG file not found: {gpkg_path}")
        return False
    
    print("\n" + "=" * 80)
    print(f"CONVERTING GPKG TO H3 RESOLUTION {H3_RESOLUTION}")
    print("=" * 80)
    
    output_file = Path(OUTPUT_H3_FILE)
    matrix_path = Path(MATRIX_FILE)
    
    # Skip if already converted
    if output_file.exists():
        df = pl.read_parquet(output_file)
        size_mb = output_file.stat().st_size / 1024**2
        print(f"Output already exists ({size_mb:.1f} MB, {len(df):,} hexagons)")
        return True
    
    # Check if matrix exists
    if not matrix_path.exists():
        print(f"Error: Matrix file not found: {MATRIX_FILE}")
        print("Please run: python matrix/grid_h3_matrix.py")
        return False
    
    try:
        start_time = time.time()
        
        # Load matrix
        print("\nLoading matrix...")
        matrix = pl.read_parquet(matrix_path)
        print(f"  Loaded {len(matrix):,} grid->H3 mappings")
        
        # Read GPKG
        print(f"\nReading GPKG: {gpkg_path.name}")
        gdf = gpd.read_file(gpkg_path)
        print(f"  Loaded {len(gdf):,} grid cells")
        
        # Define population columns to aggregate
        population_cols = ['T', 'M', 'F', 'Y_LT15', 'Y_1564', 'Y_GE65', 
                          'EMP', 'NAT', 'EU_OTH', 'OTH', 'SAME', 'CHG_IN', 'CHG_OUT']
        
        available_pop_cols = [col for col in population_cols if col in gdf.columns]
        print(f"  Population columns: {len(available_pop_cols)}")
        
        # Clean data: replace -9999 (nodata) with None
        keep_cols = ['GRD_ID'] + available_pop_cols
        if 'LAND_SURFACE' in gdf.columns:
            keep_cols.append('LAND_SURFACE')
        
        gdf_clean = gdf[keep_cols].copy()
        for col in available_pop_cols:
            gdf_clean[col] = gdf_clean[col].replace(-9999, None)
        
        # Convert to Polars
        print("\nJoining with matrix...")
        df_grid = pl.from_pandas(gdf_clean).rename({'GRD_ID': 'grid_id'})
        
        # Join with matrix
        joined = df_grid.join(matrix, on='grid_id', how='inner')
        print(f"  Joined: {len(joined):,} rows")
        
        # Apply weighted aggregation
        print("Applying weighted aggregation...")
        
        # Calculate weighted values
        weighted_exprs = [(pl.col(col) * pl.col('weight')).alias(f'w_{col}') 
                         for col in available_pop_cols]
        
        if 'LAND_SURFACE' in joined.columns:
            weighted_exprs.append((pl.col('LAND_SURFACE') * pl.col('weight')).alias('w_LAND_SURFACE'))
        
        joined = joined.with_columns(weighted_exprs)
        
        # Group by H3 and sum weighted values
        agg_exprs = [pl.col(f'w_{col}').sum().alias(col) for col in available_pop_cols]
        
        if 'LAND_SURFACE' in joined.columns:
            agg_exprs.append(pl.col('w_LAND_SURFACE').sum().alias('LAND_SURFACE'))
        
        agg_exprs.append(pl.col('grid_id').n_unique().alias('cell_count'))
        
        h3_agg = joined.group_by('h3_index').agg(agg_exprs)
        print(f"  Aggregated to {len(h3_agg):,} H3 cells")
        
        # Add H3 metadata
        print("Adding H3 metadata...")
        h3_indices = h3_agg['h3_index'].to_list()
        coords = [h3.cell_to_latlng(idx) for idx in h3_indices]
        
        h3_agg = h3_agg.with_columns([
            pl.Series('lat', [c[0] for c in coords]),
            pl.Series('lon', [c[1] for c in coords]),
            pl.lit(H3_RESOLUTION).alias('h3_resolution')
        ])
        
        # Reorder columns
        meta_cols = ['h3_index', 'h3_resolution', 'lat', 'lon', 'cell_count']
        data_cols = available_pop_cols.copy()
        if 'LAND_SURFACE' in h3_agg.columns:
            data_cols.append('LAND_SURFACE')
        
        final_cols = meta_cols + data_cols
        h3_agg = h3_agg.select(final_cols)
        
        # Save
        print(f"\nSaving to: {output_file.name}")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        h3_agg.write_parquet(output_file, compression='snappy')
        
        elapsed = time.time() - start_time
        output_size_mb = output_file.stat().st_size / 1024**2
        
        print(f"Completed ({output_size_mb:.1f} MB, {elapsed:.1f}s)")
        
        # Statistics
        print(f"\nStatistics:")
        total_pop = h3_agg.select(pl.col('T').sum()).item()
        avg_cells = h3_agg.select(pl.col('cell_count').mean()).item()
        print(f"  Total population: {total_pop:,.0f}")
        print(f"  Cells per hexagon (avg): {avg_cells:.2f}")
        
        return True
        
    except Exception as e:
        print(f"Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("=" * 80)
    print("ETL POPULATION - EUROSTAT CENSUS GRID 2021 TO H3")
    print("=" * 80)
    
    # Extract
    gpkg_path = extract_census_data()
    if not gpkg_path:
        print("\nError: Extraction failed")
        return
    
    # Convert to H3
    if not convert_to_h3_matrix(gpkg_path):
        print("\nError: H3 conversion failed")
        return
    
    print("\n" + "=" * 80)
    print("ETL POPULATION COMPLETED")
    print("=" * 80)
    print(f"Output: {OUTPUT_H3_FILE}")
    print("=" * 80)


if __name__ == "__main__":
    main()
