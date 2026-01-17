#!/usr/bin/env python3
"""
Validation script for final output dataset.
"""

import polars as pl
from pathlib import Path

OUTPUT_FILE = "data/data_h3_res8.parquet"


def validate_output():
    """Validate the final output file."""
    print("=" * 80)
    print("VALIDATION: data_h3_res8.parquet")
    print("=" * 80)
    
    if not Path(OUTPUT_FILE).exists():
        print(f"✗ Error: File not found: {OUTPUT_FILE}")
        return False
    
    try:
        # Load data
        df = pl.read_parquet(OUTPUT_FILE)
        
        # Basic info
        print(f"\n1. Basic Information:")
        print(f"   Rows: {len(df):,}")
        print(f"   Columns: {df.width}")
        
        size_mb = Path(OUTPUT_FILE).stat().st_size / 1024**2
        print(f"   File size: {size_mb:.1f} MB")
        
        # Schema
        print(f"\n2. Schema:")
        for col, dtype in df.schema.items():
            print(f"   {col}: {dtype}")
        
        # Null counts
        print(f"\n3. Null Counts:")
        null_counts = df.null_count()
        for col in df.columns:
            count = null_counts[col][0]
            if count > 0:
                pct = count / len(df) * 100
                print(f"   {col}: {count:,} ({pct:.1f}%)")
        
        # Expected columns
        print(f"\n4. Expected Columns Check:")
        expected_cols = [
            'h3_index', 'lat', 'lon',
            'pop_total', 'pop_male', 'pop_female',
            'health_distance',
            'fixed_download_2023', 'fixed_upload_2023', 'fixed_latency_2023',
            'mobile_download_2023', 'mobile_upload_2023', 'mobile_latency_2023'
        ]
        
        for col in expected_cols:
            if col in df.columns:
                print(f"   ✓ {col}")
            else:
                print(f"   ✗ Missing: {col}")
        
        # Statistics for key columns
        print(f"\n5. Key Statistics:")
        
        if 'pop_total' in df.columns:
            print(f"   Population (pop_total):")
            print(f"     Sum: {df['pop_total'].sum():,.0f}")
            print(f"     Mean: {df['pop_total'].mean():.1f}")
            print(f"     Median: {df['pop_total'].median():.0f}")
            print(f"     Max: {df['pop_total'].max():,.0f}")
        
        if 'health_distance' in df.columns:
            print(f"   Healthcare Distance (health_distance):")
            print(f"     Mean: {df['health_distance'].mean():.2f} minutes")
            print(f"     Median: {df['health_distance'].median():.2f} minutes")
            print(f"     Max: {df['health_distance'].max():.2f} minutes")
        
        if 'fixed_download_2023' in df.columns:
            valid = df.filter(pl.col('fixed_download_2023').is_not_null())
            print(f"   Fixed Download 2023 (kbps):")
            print(f"     Non-null: {len(valid):,} ({len(valid)/len(df)*100:.1f}%)")
            if len(valid) > 0:
                print(f"     Mean: {valid['fixed_download_2023'].mean():,.0f}")
                print(f"     Median: {valid['fixed_download_2023'].median():,.0f}")
        
        # Sample data
        print(f"\n6. Sample Data (first 3 rows):")
        print(df.head(3))
        
        print(f"\n" + "=" * 80)
        print(f"✓ VALIDATION PASSED")
        print(f"=" * 80)
        
        return True
        
    except Exception as e:
        print(f"\n✗ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = validate_output()
    exit(0 if success else 1)
