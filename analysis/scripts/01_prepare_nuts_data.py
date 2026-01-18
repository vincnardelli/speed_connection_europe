#!/usr/bin/env python3
"""
Prepare NUTS data: Spatial join H3 hexagons to NUTS regions at all levels.

This script:
1. Loads H3 hexagon data with population, health, and internet metrics
2. Loads NUTS boundaries (levels 0-3) from GeoPackage
3. Performs spatial join to assign each H3 hexagon to NUTS regions
4. Exports enriched dataset for downstream analysis

Output: analysis/data/h3_with_nuts.parquet
"""

import sys
from pathlib import Path
import time

import polars as pl
import geopandas as gpd
import h3
from shapely.geometry import Point
from tqdm import tqdm

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
ANALYSIS_DIR = PROJECT_ROOT / "analysis"
OUTPUT_DIR = ANALYSIS_DIR / "data"

H3_DATA = DATA_DIR / "data_h3_res8.parquet"
NUTS_DATA = DATA_DIR / "statistical_units" / "NUTS_RG_60M_2024_3035.gpkg"
OUTPUT_FILE = OUTPUT_DIR / "h3_with_nuts.parquet"

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_h3_data():
    """Load H3 hexagon data."""
    print("Loading H3 data...")
    start = time.time()
    
    df = pl.read_parquet(H3_DATA)
    
    print(f"  Loaded {len(df):,} hexagons in {time.time()-start:.1f}s")
    print(f"  Columns: {df.width}")
    print(f"  Memory: {df.estimated_size('mb'):.1f} MB")
    
    return df


def load_nuts_boundaries():
    """Load NUTS boundaries for all levels from GeoPackage."""
    print("\nLoading NUTS boundaries...")
    start = time.time()
    
    # Load the single layer containing all NUTS levels
    gdf_all = gpd.read_file(NUTS_DATA)
    
    print(f"  Loaded {len(gdf_all)} NUTS regions total")
    print(f"  Columns: {list(gdf_all.columns)}")
    
    # Split by LEVL_CODE into separate dict entries
    nuts_levels = {}
    
    for level in [0, 1, 2, 3]:
        level_gdf = gdf_all[gdf_all['LEVL_CODE'] == level].copy()
        
        if len(level_gdf) == 0:
            continue
        
        # Keep only essential columns
        level_gdf = level_gdf[['NUTS_ID', 'NAME_LATN', 'geometry']].copy()
        level_gdf.columns = ['nuts_id', 'nuts_name', 'geometry']
        
        # Create spatial index for faster joins
        level_gdf.sindex
        
        nuts_levels[level] = level_gdf
        print(f"  NUTS level {level}: {len(level_gdf)} regions")
    
    print(f"  Loaded {len(nuts_levels)} NUTS levels in {time.time()-start:.1f}s")
    return nuts_levels


def create_h3_geometries(df):
    """Convert H3 data to GeoDataFrame with point geometries (centroids)."""
    print("\nCreating H3 point geometries...")
    start = time.time()
    
    # Use lat/lon from dataframe
    geometry = [Point(lon, lat) for lat, lon in zip(df['lat'].to_list(), df['lon'].to_list())]
    
    # Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df.to_pandas(),
        geometry=geometry,
        crs='EPSG:4326'
    )
    
    # Reproject to match NUTS (EPSG:3035)
    gdf = gdf.to_crs('EPSG:3035')
    
    print(f"  Created {len(gdf):,} geometries in {time.time()-start:.1f}s")
    
    return gdf


def spatial_join_nuts(h3_gdf, nuts_levels):
    """Perform spatial join for each NUTS level."""
    print("\nPerforming spatial joins...")
    
    result_gdf = h3_gdf.copy()
    
    for level in sorted(nuts_levels.keys()):
        print(f"  Joining NUTS level {level}...")
        start = time.time()
        
        nuts_gdf = nuts_levels[level]
        
        # Spatial join (point in polygon)
        joined = gpd.sjoin(
            result_gdf[['geometry']].reset_index(),
            nuts_gdf,
            how='left',
            predicate='within'
        )
        
        # Add NUTS columns to result
        nuts_id_col = f'nuts_id_{level}'
        nuts_name_col = f'nuts_name_{level}'
        
        result_gdf[nuts_id_col] = joined['nuts_id'].values
        if 'nuts_name' in joined.columns:
            result_gdf[nuts_name_col] = joined['nuts_name'].values
        
        # Report coverage
        coverage = joined['nuts_id'].notna().sum() / len(joined) * 100
        print(f"    Coverage: {coverage:.1f}% ({joined['nuts_id'].notna().sum():,}/{len(joined):,} hexagons)")
        print(f"    Time: {time.time()-start:.1f}s")
    
    return result_gdf


def handle_missing_assignments(gdf, nuts_levels):
    """Handle hexagons not assigned to any NUTS region (edge cases)."""
    print("\nHandling missing NUTS assignments...")
    
    for level in sorted(nuts_levels.keys()):
        nuts_id_col = f'nuts_id_{level}'
        nuts_name_col = f'nuts_name_{level}'
        
        if nuts_id_col not in gdf.columns:
            continue
        
        missing = gdf[nuts_id_col].isna().sum()
        
        if missing > 0:
            print(f"  NUTS level {level}: {missing:,} hexagons without assignment")
            print(f"    Assigning to nearest region...")
            
            # Find nearest NUTS region for unassigned hexagons
            unassigned_idx = gdf[gdf[nuts_id_col].isna()].index
            nuts_gdf = nuts_levels[level]
            
            for idx in tqdm(unassigned_idx, desc=f"    NUTS {level}"):
                point = gdf.loc[idx, 'geometry']
                # Find nearest region
                distances = nuts_gdf.geometry.distance(point)
                nearest_idx = distances.idxmin()
                
                gdf.loc[idx, nuts_id_col] = nuts_gdf.loc[nearest_idx, 'nuts_id']
                if nuts_name_col in gdf.columns and 'nuts_name' in nuts_gdf.columns:
                    gdf.loc[idx, nuts_name_col] = nuts_gdf.loc[nearest_idx, 'nuts_name']
            
            print(f"    Completed assignments for level {level}")
    
    return gdf


def export_results(gdf):
    """Export enriched dataset to parquet."""
    print("\nExporting results...")
    start = time.time()
    
    # Drop geometry column for parquet export
    df = gdf.drop(columns=['geometry'])
    
    # Convert to polars for efficient export
    df_pl = pl.from_pandas(df)
    
    # Save
    df_pl.write_parquet(OUTPUT_FILE)
    
    file_size = OUTPUT_FILE.stat().st_size / 1024**2
    print(f"  Saved to: {OUTPUT_FILE}")
    print(f"  File size: {file_size:.1f} MB")
    print(f"  Time: {time.time()-start:.1f}s")
    
    # Print summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total hexagons: {len(df_pl):,}")
    print(f"Total columns: {df_pl.width}")
    
    for level in [0, 1, 2, 3]:
        col = f'nuts_id_{level}'
        if col in df_pl.columns:
            unique = df_pl[col].n_unique()
            coverage = (df_pl[col].is_not_null().sum() / len(df_pl) * 100)
            print(f"NUTS level {level}: {unique} regions, {coverage:.1f}% coverage")
    
    print("="*80)


def main():
    """Main execution."""
    print("="*80)
    print("STEP 1: PREPARE NUTS DATA")
    print("="*80)
    
    overall_start = time.time()
    
    try:
        # Check input files
        if not H3_DATA.exists():
            print(f"✗ Error: H3 data not found: {H3_DATA}")
            print("  Run the main pipeline first: python3 run_pipeline.py")
            return 1
        
        if not NUTS_DATA.exists():
            print(f"✗ Error: NUTS data not found: {NUTS_DATA}")
            print("  Download NUTS boundaries from Eurostat GISCO")
            return 1
        
        # Load data
        h3_df = load_h3_data()
        nuts_levels = load_nuts_boundaries()
        
        # Create geometries
        h3_gdf = create_h3_geometries(h3_df)
        
        # Spatial join
        result_gdf = spatial_join_nuts(h3_gdf, nuts_levels)
        
        # Handle missing assignments
        result_gdf = handle_missing_assignments(result_gdf, nuts_levels)
        
        # Export
        export_results(result_gdf)
        
        print(f"\n✓ COMPLETED in {time.time()-overall_start:.1f}s")
        return 0
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
