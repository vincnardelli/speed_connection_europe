#!/usr/bin/env python3
"""
Analyze healthcare accessibility by NUTS regions.

This script:
1. Loads H3 data with NUTS assignments
2. Classifies travel time into categories
3. Aggregates by NUTS levels (0, 1, 2, 3) and Europe total
4. Calculates population by travel time category
5. Exports results for Excel export

Output: analysis/data/healthcare_analysis.parquet
"""

import sys
from pathlib import Path
import time

import polars as pl

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
ANALYSIS_DIR = PROJECT_ROOT / "analysis"
DATA_DIR = ANALYSIS_DIR / "data"

INPUT_FILE = DATA_DIR / "h3_with_nuts.parquet"
OUTPUT_FILE = DATA_DIR / "healthcare_analysis.parquet"

# Travel time thresholds (in minutes)
THRESHOLDS = {
    'very_close': 5,
    'close': 10,
    'moderate': 15,
    'far': 30,
    # very_far: >30
}


def load_data():
    """Load H3 data with NUTS assignments."""
    print("Loading H3 data with NUTS assignments...")
    start = time.time()
    
    if not INPUT_FILE.exists():
        print(f"✗ Error: {INPUT_FILE} not found")
        print("  Run 01_prepare_nuts_data.py first")
        sys.exit(1)
    
    df = pl.read_parquet(INPUT_FILE)
    
    print(f"  Loaded {len(df):,} hexagons in {time.time()-start:.1f}s")
    
    return df


def classify_travel_time(df):
    """Classify hexagons into travel time categories."""
    print("\nClassifying travel time categories...")
    start = time.time()
    
    # Convert health_distance from seconds to minutes
    df = df.with_columns([
        (pl.col('health_distance') / 60.0).alias('health_distance_minutes')
    ])
    
    df = df.with_columns([
        pl.when(pl.col('health_distance_minutes').is_null())
          .then(pl.lit('no_data'))
          .when(pl.col('health_distance_minutes') < THRESHOLDS['very_close'])
          .then(pl.lit('very_close'))
          .when(pl.col('health_distance_minutes') < THRESHOLDS['close'])
          .then(pl.lit('close'))
          .when(pl.col('health_distance_minutes') < THRESHOLDS['moderate'])
          .then(pl.lit('moderate'))
          .when(pl.col('health_distance_minutes') < THRESHOLDS['far'])
          .then(pl.lit('far'))
          .otherwise(pl.lit('very_far'))
          .alias('healthcare_category')
    ])
    
    # Print distribution
    print("  Healthcare accessibility categories:")
    category_counts = df.group_by('healthcare_category').agg([
        pl.count().alias('hexagons'),
        pl.col('pop_total').sum().alias('population')
    ]).sort('healthcare_category')
    print(category_counts)
    
    print(f"  Time: {time.time()-start:.1f}s")
    
    return df


def aggregate_europe(df):
    """Aggregate healthcare metrics for all of Europe."""
    print("\nAggregating Europe-wide metrics...")
    start = time.time()
    
    metrics = []
    
    total_pop = df['pop_total'].sum()
    
    # Population by category
    for category in ['very_close', 'close', 'moderate', 'far', 'very_far', 'no_data']:
        cat_df = df.filter(pl.col('healthcare_category') == category)
        pop = cat_df['pop_total'].sum()
        pct = (pop / total_pop * 100) if total_pop > 0 else 0
        
        metrics.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',
            'category': category,
            'population': pop,
            'percentage': pct,
            'hexagon_count': len(cat_df)
        })
    
    # Distance statistics (using minutes)
    stats = []
    
    valid_df = df.filter(pl.col('health_distance_minutes').is_not_null())
    if len(valid_df) > 0:
        stats.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',
            'metric_name': 'mean_distance_minutes',
            'value': valid_df['health_distance_minutes'].mean(),
        })
        stats.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',
            'metric_name': 'median_distance_minutes',
            'value': valid_df['health_distance_minutes'].median(),
        })
        stats.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',
            'metric_name': 'max_distance_minutes',
            'value': valid_df['health_distance_minutes'].max(),
        })
        stats.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',
            'metric_name': 'coverage_pct',
            'value': (len(valid_df) / len(df) * 100),
        })
    
    # Population beyond thresholds (using minutes)
    for threshold, label in [(5, 'pop_gt_5min'), (10, 'pop_gt_10min'), 
                              (15, 'pop_gt_15min'), (30, 'pop_gt_30min')]:
        beyond_df = df.filter(pl.col('health_distance_minutes') > threshold)
        pop = beyond_df['pop_total'].sum()
        pct = (pop / total_pop * 100) if total_pop > 0 else 0
        
        stats.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',
            'metric_name': label,
            'value': pop,
        })
        stats.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',
            'metric_name': f'{label}_pct',
            'value': pct,
        })
    
    result = pl.DataFrame(metrics)
    stats_df = pl.DataFrame(stats)
    
    print(f"  Europe total population: {total_pop:,.0f}")
    print(f"  Mean travel time: {valid_df['health_distance_minutes'].mean():.2f} minutes")
    print(f"  Time: {time.time()-start:.1f}s")
    
    return result, stats_df


def aggregate_by_nuts(df, level):
    """Aggregate healthcare metrics by NUTS level."""
    print(f"\nAggregating NUTS level {level} metrics...")
    start = time.time()
    
    nuts_id_col = f'nuts_id_{level}'
    nuts_name_col = f'nuts_name_{level}'
    
    if nuts_id_col not in df.columns:
        print(f"  ✗ NUTS level {level} not found in data")
        return None, None
    
    # Get unique regions
    regions = df.select([nuts_id_col, nuts_name_col]).unique().sort(nuts_id_col)
    
    print(f"  Processing {len(regions)} regions...")
    
    # Population by category
    metrics = []
    
    for region_row in regions.iter_rows(named=True):
        region_id = region_row[nuts_id_col]
        region_name = region_row.get(nuts_name_col, region_id)
        
        region_df = df.filter(pl.col(nuts_id_col) == region_id)
        total_pop = region_df['pop_total'].sum()
        
        for category in ['very_close', 'close', 'moderate', 'far', 'very_far', 'no_data']:
            cat_df = region_df.filter(pl.col('healthcare_category') == category)
            pop = cat_df['pop_total'].sum()
            pct = (pop / total_pop * 100) if total_pop > 0 else 0
            
            metrics.append({
                'region': region_id,
                'region_name': region_name,
                'nuts_level': str(level),
                'category': category,
                'population': pop,
                'percentage': pct,
                'hexagon_count': len(cat_df)
            })
    
    # Distance statistics by region
    stats = []
    
    for region_row in regions.iter_rows(named=True):
        region_id = region_row[nuts_id_col]
        region_name = region_row.get(nuts_name_col, region_id)
        
        region_df = df.filter(pl.col(nuts_id_col) == region_id)
        total_pop = region_df['pop_total'].sum()
        valid_df = region_df.filter(pl.col('health_distance_minutes').is_not_null())
        
        if len(valid_df) > 0:
            stats.append({
                'region': region_id,
                'region_name': region_name,
                'nuts_level': str(level),
                'metric_name': 'mean_distance_minutes',
                'value': valid_df['health_distance_minutes'].mean(),
            })
            stats.append({
                'region': region_id,
                'region_name': region_name,
                'nuts_level': str(level),
                'metric_name': 'median_distance_minutes',
                'value': valid_df['health_distance_minutes'].median(),
            })
            stats.append({
                'region': region_id,
                'region_name': region_name,
                'nuts_level': str(level),
                'metric_name': 'coverage_pct',
                'value': (len(valid_df) / len(region_df) * 100),
            })
            
            # Population beyond thresholds (using minutes)
            for threshold, label in [(5, 'pop_gt_5min'), (10, 'pop_gt_10min'),
                                      (15, 'pop_gt_15min'), (30, 'pop_gt_30min')]:
                beyond_df = region_df.filter(pl.col('health_distance_minutes') > threshold)
                pop = beyond_df['pop_total'].sum()
                pct = (pop / total_pop * 100) if total_pop > 0 else 0
                
                stats.append({
                    'region': region_id,
                    'region_name': region_name,
                    'nuts_level': str(level),
                    'metric_name': label,
                    'value': pop,
                })
                stats.append({
                    'region': region_id,
                    'region_name': region_name,
                    'nuts_level': str(level),
                    'metric_name': f'{label}_pct',
                    'value': pct,
                })
    
    result = pl.DataFrame(metrics)
    stats_df = pl.DataFrame(stats)
    
    print(f"  Time: {time.time()-start:.1f}s")
    
    return result, stats_df


def main():
    """Main execution."""
    print("="*80)
    print("STEP 3: ANALYZE HEALTHCARE ACCESSIBILITY")
    print("="*80)
    
    overall_start = time.time()
    
    try:
        # Load data
        df = load_data()
        
        # Classify categories
        df = classify_travel_time(df)
        
        # Aggregate metrics
        all_category_data = []
        all_stats_data = []
        
        # Europe total
        europe_cats, europe_stats = aggregate_europe(df)
        all_category_data.append(europe_cats)
        all_stats_data.append(europe_stats)
        
        # Each NUTS level
        for level in [0, 1, 2, 3]:
            nuts_cats, nuts_stats = aggregate_by_nuts(df, level)
            if nuts_cats is not None:
                all_category_data.append(nuts_cats)
                all_stats_data.append(nuts_stats)
        
        # Combine all results
        print("\nCombining results...")
        category_results = pl.concat(all_category_data)
        stats_results = pl.concat(all_stats_data)
        
        # Save category data
        category_results.write_parquet(OUTPUT_FILE)
        
        # Save stats data
        stats_file = OUTPUT_FILE.parent / "healthcare_stats.parquet"
        stats_results.write_parquet(stats_file)
        
        print(f"\n✓ Saved category data: {OUTPUT_FILE}")
        print(f"✓ Saved stats data: {stats_file}")
        print(f"  Total records (categories): {len(category_results):,}")
        print(f"  Total records (stats): {len(stats_results):,}")
        
        print(f"\n✓ COMPLETED in {time.time()-overall_start:.1f}s")
        return 0
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
