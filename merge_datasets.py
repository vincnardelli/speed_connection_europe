#!/usr/bin/env python3
"""
Merge Population, Health, and Internet datasets into a single H3 resolution 8 parquet file.
"""

import polars as pl
from pathlib import Path
import time

# Input files
POPULATION_FILE = "data/population/population_census_2021_h3_res8.parquet"
HEALTH_FILE = "data/health/euro_access_healthcare_2023_h3_res8.parquet"
INTERNET_FILE = "data/internet/internet_speed_h3_res8.parquet"

# Output file
OUTPUT_FILE = "data/data_h3_res8.parquet"


def prepare_population_data(pop_df):
    """Prepare and rename population columns."""
    print("Preparing population data...")
    
    rename_map = {
        'T': 'pop_total',
        'M': 'pop_male',
        'F': 'pop_female',
        'Y_LT15': 'pop_age_lt15',
        'Y_1564': 'pop_age_15_64',
        'Y_GE65': 'pop_age_ge65',
        'EMP': 'pop_employed',
        'NAT': 'pop_national',
        'EU_OTH': 'pop_eu_other',
        'OTH': 'pop_other',
        'SAME': 'pop_same_residence',
        'CHG_IN': 'pop_change_in',
        'CHG_OUT': 'pop_change_out'
    }
    
    # Select and rename
    keep_cols = ['h3_index', 'lat', 'lon'] + [k for k in rename_map.keys() if k in pop_df.columns]
    pop_clean = pop_df.select(keep_cols).rename(rename_map)
    
    print(f"  {len(pop_clean):,} rows, {len(pop_clean.columns)} columns")
    return pop_clean


def prepare_health_data(health_df):
    """Prepare health data - keep only accessibility_mean."""
    print("Preparing health data...")
    
    health_clean = health_df.select(['h3_index', 'accessibility_mean']).rename({
        'accessibility_mean': 'health_distance'
    })
    
    print(f"  {len(health_clean):,} rows")
    return health_clean


def prepare_internet_data(internet_df):
    """Prepare internet data - already aggregated to 2023 and total."""
    print("Preparing internet data...")
    
    # Keep only h3_index and aggregated columns (2023 and total)
    keep_cols = ['h3_index'] + [col for col in internet_df.columns 
                                 if col.endswith('_2023') or col.endswith('_total')]
    
    internet_clean = internet_df.select(keep_cols)
    print(f"  {len(internet_clean):,} rows, {len(internet_clean.columns)} columns")
    return internet_clean


def merge_datasets(pop_df, health_df, internet_df):
    """Merge the three datasets."""
    print("\nMerging datasets...")
    
    # First merge: population + health (inner join)
    print(f"  Population ({len(pop_df):,}) + Health ({len(health_df):,})")
    merged = pop_df.join(health_df, on='h3_index', how='inner')
    print(f"    After inner join: {len(merged):,} rows")
    
    # Second merge: result + internet (left join)
    print(f"  Merged + Internet ({len(internet_df):,})")
    merged = merged.join(internet_df, on='h3_index', how='left')
    print(f"    After left join: {len(merged):,} rows")
    
    return merged


def filter_and_finalize(df):
    """Filter rows and reorder columns."""
    print("\nFiltering and finalizing...")
    
    initial_rows = len(df)
    
    # Filter: pop_total > 0 and health_distance not null
    df = df.filter(
        (pl.col('pop_total') > 0) & 
        (pl.col('health_distance').is_not_null())
    )
    
    print(f"  After filters: {len(df):,} rows ({len(df)/initial_rows*100:.1f}%)")
    
    # Define column order
    metadata_cols = ['h3_index', 'lat', 'lon']
    
    population_cols = [
        'pop_total', 'pop_male', 'pop_female',
        'pop_age_lt15', 'pop_age_15_64', 'pop_age_ge65',
        'pop_employed',
        'pop_national', 'pop_eu_other', 'pop_other',
        'pop_same_residence', 'pop_change_in', 'pop_change_out'
    ]
    
    health_cols = ['health_distance']
    
    internet_cols = [col for col in df.columns if col.endswith('_2023') or col.endswith('_total')]
    
    # Reorder - remove duplicates
    ordered_cols = metadata_cols + population_cols + health_cols + internet_cols
    # Remove duplicates while preserving order
    seen = set()
    unique_ordered_cols = []
    for col in ordered_cols:
        if col in df.columns and col not in seen:
            unique_ordered_cols.append(col)
            seen.add(col)
    
    df = df.select(unique_ordered_cols)
    
    return df


def save_output(df):
    """Save to parquet with optimized types."""
    print("\nSaving output...")
    
    # Round numeric columns for file size reduction
    integer_cols = ['pop_total', 'pop_male', 'pop_female', 'pop_age_lt15', 'pop_age_15_64', 
                    'pop_age_ge65', 'pop_employed', 'pop_national', 'pop_eu_other', 'pop_other',
                    'pop_same_residence', 'pop_change_in', 'pop_change_out']
    
    cast_exprs = []
    for col in df.columns:
        dtype = df[col].dtype
        if col == 'h3_index':
            cast_exprs.append(pl.col(col).cast(pl.Utf8))
        elif col in ['lat', 'lon']:
            cast_exprs.append(pl.col(col).round(6).cast(pl.Float64))
        elif col in integer_cols:
            cast_exprs.append(pl.col(col).round(0).cast(pl.Int64))
        elif dtype in [pl.Float32, pl.Float64]:
            cast_exprs.append(pl.col(col).round(2).cast(pl.Float64))
        else:
            cast_exprs.append(pl.col(col))
    
    df = df.select(cast_exprs)
    
    print(f"  Writing to: {OUTPUT_FILE}")
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUTPUT_FILE, compression='snappy')
    
    size_mb = Path(OUTPUT_FILE).stat().st_size / 1024**2
    print(f"  Saved: {size_mb:.1f} MB")
    
    return df


def print_statistics(df):
    """Print final statistics."""
    print("\n" + "=" * 80)
    print("FINAL STATISTICS")
    print("=" * 80)
    print(f"File: {OUTPUT_FILE}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    
    # Null counts
    print(f"\nNull values:")
    null_counts = df.null_count()
    for col in df.columns:
        count = null_counts[col][0]
        if count > 0:
            pct = count / len(df) * 100
            print(f"  {col}: {count:,} ({pct:.1f}%)")
    
    # Sample data
    print(f"\nSample (first 3 rows):")
    print(df.head(3))


def main():
    start_time = time.time()
    
    print("=" * 80)
    print("MERGE DATASETS: POPULATION + HEALTH + INTERNET")
    print("=" * 80)
    
    # Load datasets
    print("\nLoading datasets...")
    pop_df = pl.read_parquet(POPULATION_FILE)
    print(f"  Population: {len(pop_df):,} rows")
    
    health_df = pl.read_parquet(HEALTH_FILE)
    print(f"  Health: {len(health_df):,} rows")
    
    internet_df = pl.read_parquet(INTERNET_FILE)
    print(f"  Internet: {len(internet_df):,} rows")
    
    # Prepare datasets
    print()
    pop_clean = prepare_population_data(pop_df)
    health_clean = prepare_health_data(health_df)
    internet_clean = prepare_internet_data(internet_df)
    
    # Merge
    merged = merge_datasets(pop_clean, health_clean, internet_clean)
    
    # Filter and finalize
    final = filter_and_finalize(merged)
    
    # Save
    final = save_output(final)
    
    # Statistics
    print_statistics(final)
    
    elapsed = time.time() - start_time
    print(f"\n" + "=" * 80)
    print(f"MERGE COMPLETED in {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print("=" * 80)


if __name__ == "__main__":
    main()
