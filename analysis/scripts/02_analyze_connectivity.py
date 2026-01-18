#!/usr/bin/env python3
"""
Analyze internet connectivity by NUTS regions.

This script:
1. Loads H3 data with NUTS assignments
2. Classifies connectivity into speed tiers
3. Aggregates by NUTS levels (0, 1, 2, 3) and Europe total
4. Calculates population by tier and connectivity metrics
5. Exports results for Excel export

Output: analysis/data/connectivity_analysis.parquet
"""

import sys
from pathlib import Path
import time

import polars as pl
import pandas as pd

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
ANALYSIS_DIR = PROJECT_ROOT / "analysis"
DATA_DIR = ANALYSIS_DIR / "data"

INPUT_FILE = DATA_DIR / "h3_with_nuts.parquet"
OUTPUT_FILE = DATA_DIR / "connectivity_analysis.parquet"

# Speed tier thresholds (in kbps)
THRESHOLDS = {
    'very_poor': 10_000,    # <10 Mbps
    'poor': 25_000,         # 10-25 Mbps
    'basic': 100_000,       # 25-100 Mbps
    # good: ≥100 Mbps
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
    print(f"  Columns: {df.width}")
    
    return df


def classify_connectivity(df):
    """Classify hexagons into connectivity tiers for fixed and mobile."""
    print("\nClassifying connectivity tiers...")
    start = time.time()
    
    # Fixed internet tiers
    df = df.with_columns([
        pl.when(pl.col('fixed_download_2023').is_null())
          .then(pl.lit('disconnected'))
          .when(pl.col('fixed_download_2023') < THRESHOLDS['very_poor'])
          .then(pl.lit('very_poor'))
          .when(pl.col('fixed_download_2023') < THRESHOLDS['poor'])
          .then(pl.lit('poor'))
          .when(pl.col('fixed_download_2023') < THRESHOLDS['basic'])
          .then(pl.lit('basic'))
          .otherwise(pl.lit('good'))
          .alias('fixed_tier')
    ])
    
    # Mobile internet tiers
    df = df.with_columns([
        pl.when(pl.col('mobile_download_2023').is_null())
          .then(pl.lit('disconnected'))
          .when(pl.col('mobile_download_2023') < THRESHOLDS['very_poor'])
          .then(pl.lit('very_poor'))
          .when(pl.col('mobile_download_2023') < THRESHOLDS['poor'])
          .then(pl.lit('poor'))
          .when(pl.col('mobile_download_2023') < THRESHOLDS['basic'])
          .then(pl.lit('basic'))
          .otherwise(pl.lit('good'))
          .alias('mobile_tier')
    ])
    
    # Print distribution
    print("  Fixed internet tiers:")
    tier_counts = df.group_by('fixed_tier').agg([
        pl.count().alias('hexagons'),
        pl.col('pop_total').sum().alias('population')
    ]).sort('fixed_tier')
    print(tier_counts)
    
    print("\n  Mobile internet tiers:")
    tier_counts = df.group_by('mobile_tier').agg([
        pl.count().alias('hexagons'),
        pl.col('pop_total').sum().alias('population')
    ]).sort('mobile_tier')
    print(tier_counts)
    
    print(f"  Time: {time.time()-start:.1f}s")
    
    return df


def aggregate_europe(df):
    """Aggregate connectivity metrics for all of Europe."""
    print("\nAggregating Europe-wide metrics...")
    start = time.time()
    
    # Fixed internet
    fixed_metrics = []
    
    total_pop = df['pop_total'].sum()
    
    for tier in ['disconnected', 'very_poor', 'poor', 'basic', 'good']:
        tier_df = df.filter(pl.col('fixed_tier') == tier)
        pop = tier_df['pop_total'].sum()
        pct = (pop / total_pop * 100) if total_pop > 0 else 0
        
        fixed_metrics.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',  # String type
            'metric_type': 'fixed',
            'tier': tier,
            'population': pop,
            'percentage': pct,
            'hexagon_count': len(tier_df)
        })
    
    # Mobile internet
    mobile_metrics = []
    
    for tier in ['disconnected', 'very_poor', 'poor', 'basic', 'good']:
        tier_df = df.filter(pl.col('mobile_tier') == tier)
        pop = tier_df['pop_total'].sum()
        pct = (pop / total_pop * 100) if total_pop > 0 else 0
        
        mobile_metrics.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',  # String type
            'metric_type': 'mobile',
            'tier': tier,
            'population': pop,
            'percentage': pct,
            'hexagon_count': len(tier_df)
        })
    
    # Speed statistics
    speed_stats = [{
        'region': 'Europe',
        'region_name': 'Europe',
        'nuts_level': 'All',  # String type
        'metric_type': 'fixed',
        'metric_name': 'mean_speed_mbps',
        'value': df.filter(pl.col('fixed_download_2023').is_not_null())['fixed_download_2023'].mean() / 1000 if len(df.filter(pl.col('fixed_download_2023').is_not_null())) > 0 else None,
    }, {
        'region': 'Europe',
        'region_name': 'Europe',
        'nuts_level': 'All',  # String type
        'metric_type': 'mobile',
        'metric_name': 'mean_speed_mbps',
        'value': df.filter(pl.col('mobile_download_2023').is_not_null())['mobile_download_2023'].mean() / 1000 if len(df.filter(pl.col('mobile_download_2023').is_not_null())) > 0 else None,
    }, {
        'region': 'Europe',
        'region_name': 'Europe',
        'nuts_level': 'All',  # String type
        'metric_type': 'fixed',
        'metric_name': 'coverage_pct',
        'value': (df.filter(pl.col('fixed_download_2023').is_not_null())['pop_total'].sum() / total_pop * 100) if total_pop > 0 else 0,
    }, {
        'region': 'Europe',
        'region_name': 'Europe',
        'nuts_level': 'All',  # String type
        'metric_type': 'mobile',
        'metric_name': 'coverage_pct',
        'value': (df.filter(pl.col('mobile_download_2023').is_not_null())['pop_total'].sum() / total_pop * 100) if total_pop > 0 else 0,
    }]
    
    result = pl.DataFrame(fixed_metrics + mobile_metrics)
    stats = pl.DataFrame(speed_stats)
    
    print(f"  Europe total population: {total_pop:,.0f}")
    print(f"  Time: {time.time()-start:.1f}s")
    
    return result, stats


def aggregate_by_nuts(df, level):
    """Aggregate connectivity metrics by NUTS level."""
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
    
    # Fixed internet by tier
    fixed_metrics = []
    
    for region_row in regions.iter_rows(named=True):
        region_id = region_row[nuts_id_col]
        region_name = region_row.get(nuts_name_col, region_id)
        
        region_df = df.filter(pl.col(nuts_id_col) == region_id)
        total_pop = region_df['pop_total'].sum()
        
        for tier in ['disconnected', 'very_poor', 'poor', 'basic', 'good']:
            tier_df = region_df.filter(pl.col('fixed_tier') == tier)
            pop = tier_df['pop_total'].sum()
            pct = (pop / total_pop * 100) if total_pop > 0 else 0
            
            fixed_metrics.append({
                'region': region_id,
                'region_name': region_name,
                'nuts_level': str(level),  # Convert to string
                'metric_type': 'fixed',
                'tier': tier,
                'population': pop,
                'percentage': pct,
                'hexagon_count': len(tier_df)
            })
    
    # Mobile internet by tier
    mobile_metrics = []
    
    for region_row in regions.iter_rows(named=True):
        region_id = region_row[nuts_id_col]
        region_name = region_row.get(nuts_name_col, region_id)
        
        region_df = df.filter(pl.col(nuts_id_col) == region_id)
        total_pop = region_df['pop_total'].sum()
        
        for tier in ['disconnected', 'very_poor', 'poor', 'basic', 'good']:
            tier_df = region_df.filter(pl.col('mobile_tier') == tier)
            pop = tier_df['pop_total'].sum()
            pct = (pop / total_pop * 100) if total_pop > 0 else 0
            
            mobile_metrics.append({
                'region': region_id,
                'region_name': region_name,
                'nuts_level': str(level),  # Convert to string
                'metric_type': 'mobile',
                'tier': tier,
                'population': pop,
                'percentage': pct,
                'hexagon_count': len(tier_df)
            })
    
    # Speed statistics by region
    speed_stats = []
    
    for region_row in regions.iter_rows(named=True):
        region_id = region_row[nuts_id_col]
        region_name = region_row.get(nuts_name_col, region_id)
        
        region_df = df.filter(pl.col(nuts_id_col) == region_id)
        total_pop = region_df['pop_total'].sum()
        
        # Fixed stats
        fixed_with_data = region_df.filter(pl.col('fixed_download_2023').is_not_null())
        speed_stats.append({
            'region': region_id,
            'region_name': region_name,
            'nuts_level': str(level),  # Convert to string
            'metric_type': 'fixed',
            'metric_name': 'mean_speed_mbps',
            'value': fixed_with_data['fixed_download_2023'].mean() / 1000 if len(fixed_with_data) > 0 else None,
        })
        speed_stats.append({
            'region': region_id,
            'region_name': region_name,
            'nuts_level': str(level),  # Convert to string
            'metric_type': 'fixed',
            'metric_name': 'coverage_pct',
            'value': (fixed_with_data['pop_total'].sum() / total_pop * 100) if total_pop > 0 else 0,
        })
        
        # Mobile stats
        mobile_with_data = region_df.filter(pl.col('mobile_download_2023').is_not_null())
        speed_stats.append({
            'region': region_id,
            'region_name': region_name,
            'nuts_level': str(level),  # Convert to string
            'metric_type': 'mobile',
            'metric_name': 'mean_speed_mbps',
            'value': mobile_with_data['mobile_download_2023'].mean() / 1000 if len(mobile_with_data) > 0 else None,
        })
        speed_stats.append({
            'region': region_id,
            'region_name': region_name,
            'nuts_level': str(level),  # Convert to string
            'metric_type': 'mobile',
            'metric_name': 'coverage_pct',
            'value': (mobile_with_data['pop_total'].sum() / total_pop * 100) if total_pop > 0 else 0,
        })
    
    result = pl.DataFrame(fixed_metrics + mobile_metrics)
    stats = pl.DataFrame(speed_stats)
    
    print(f"  Time: {time.time()-start:.1f}s")
    
    return result, stats


def main():
    """Main execution."""
    print("="*80)
    print("STEP 2: ANALYZE CONNECTIVITY")
    print("="*80)
    
    overall_start = time.time()
    
    try:
        # Load data
        df = load_data()
        
        # Classify tiers
        df = classify_connectivity(df)
        
        # Aggregate metrics
        all_tier_data = []
        all_stats_data = []
        
        # Europe total
        europe_tiers, europe_stats = aggregate_europe(df)
        all_tier_data.append(europe_tiers)
        all_stats_data.append(europe_stats)
        
        # Each NUTS level
        for level in [0, 1, 2, 3]:
            nuts_tiers, nuts_stats = aggregate_by_nuts(df, level)
            if nuts_tiers is not None:
                all_tier_data.append(nuts_tiers)
                all_stats_data.append(nuts_stats)
        
        # Combine all results
        print("\nCombining results...")
        tier_results = pl.concat(all_tier_data)
        stats_results = pl.concat(all_stats_data)
        
        # Save tier data
        tier_results.write_parquet(OUTPUT_FILE)
        
        # Save stats data
        stats_file = OUTPUT_FILE.parent / "connectivity_stats.parquet"
        stats_results.write_parquet(stats_file)
        
        print(f"\n✓ Saved tier data: {OUTPUT_FILE}")
        print(f"✓ Saved stats data: {stats_file}")
        print(f"  Total records (tiers): {len(tier_results):,}")
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
