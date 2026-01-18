#!/usr/bin/env python3
"""
Analyze demographics of underserved vs. well-connected populations.

This script:
1. Loads H3 data with NUTS and connectivity classifications
2. Defines underserved (disconnected + <10 Mbps) and well-connected (≥100 Mbps) populations
3. Analyzes demographic profiles for each group
4. Compares age, gender, employment, and other demographics
5. Exports results for Excel export

Output: analysis/data/demographics_analysis.parquet
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
OUTPUT_FILE = DATA_DIR / "demographics_analysis.parquet"


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


def classify_connectivity_groups(df):
    """Classify hexagons into underserved and well-connected groups."""
    print("\nClassifying connectivity groups...")
    start = time.time()
    
    # Underserved: disconnected (null) or <10 Mbps
    df = df.with_columns([
        pl.when(
            pl.col('fixed_download_2023').is_null() |
            (pl.col('fixed_download_2023') < 10_000)
        )
        .then(pl.lit('underserved'))
        .when(pl.col('fixed_download_2023') >= 100_000)
        .then(pl.lit('well_connected'))
        .otherwise(pl.lit('intermediate'))
        .alias('connectivity_group')
    ])
    
    # Print distribution
    print("  Connectivity groups:")
    group_counts = df.group_by('connectivity_group').agg([
        pl.count().alias('hexagons'),
        pl.col('pop_total').sum().alias('population')
    ]).sort('connectivity_group')
    print(group_counts)
    
    print(f"  Time: {time.time()-start:.1f}s")
    
    return df


def aggregate_demographics_europe(df):
    """Aggregate demographics for Europe by connectivity group."""
    print("\nAggregating Europe-wide demographics...")
    start = time.time()
    
    results = []
    
    for group in ['underserved', 'well_connected', 'intermediate']:
        group_df = df.filter(pl.col('connectivity_group') == group)
        
        if len(group_df) == 0:
            continue
        
        # Basic population metrics
        total_pop = group_df['pop_total'].sum()
        pop_male = group_df['pop_male'].sum() if 'pop_male' in group_df.columns else None
        pop_female = group_df['pop_female'].sum() if 'pop_female' in group_df.columns else None
        
        # Gender ratio
        pct_female = (pop_female / total_pop * 100) if pop_female and total_pop > 0 else None
        
        # Age groups (if available) - FIXED column names
        pop_0_14 = None
        if 'pop_age_lt15' in group_df.columns:
            val = group_df['pop_age_lt15'].sum()
            pop_0_14 = val if val is not None else None
            
        pop_15_64 = None
        if 'pop_age_15_64' in group_df.columns:
            val = group_df['pop_age_15_64'].sum()
            pop_15_64 = val if val is not None else None
            
        pop_65_plus = None
        if 'pop_age_ge65' in group_df.columns:
            val = group_df['pop_age_ge65'].sum()
            pop_65_plus = val if val is not None else None
        
        pct_0_14 = (pop_0_14 / total_pop * 100) if pop_0_14 and total_pop > 0 else None
        pct_15_64 = (pop_15_64 / total_pop * 100) if pop_15_64 and total_pop > 0 else None
        pct_65_plus = (pop_65_plus / total_pop * 100) if pop_65_plus and total_pop > 0 else None
        
        # Employment (if available)
        pop_employed = group_df['pop_employed'].sum() if 'pop_employed' in group_df.columns else None
        employment_rate = (pop_employed / total_pop * 100) if pop_employed and total_pop > 0 else None
        
        # Citizenship (if available)
        pop_foreign = group_df['pop_foreign_citizenship'].sum() if 'pop_foreign_citizenship' in group_df.columns else None
        pct_foreign = (pop_foreign / total_pop * 100) if pop_foreign and total_pop > 0 else None
        
        # Population density (approximate)
        # H3 res 8 hexagon area ≈ 0.74 km²
        hexagon_area_km2 = 0.737327598
        total_area_km2 = len(group_df) * hexagon_area_km2
        pop_density = total_pop / total_area_km2 if total_area_km2 > 0 else 0
        
        # Mean healthcare distance (convert from seconds to minutes)
        mean_healthcare = None
        if 'health_distance' in group_df.columns:
            filtered = group_df.filter(pl.col('health_distance').is_not_null())
            if len(filtered) > 0:
                mean_healthcare = filtered['health_distance'].mean() / 60.0  # Convert to minutes
        
        # Speed metrics
        mean_fixed_speed = group_df.filter(
            pl.col('fixed_download_2023').is_not_null()
        )['fixed_download_2023'].mean() / 1000 if 'fixed_download_2023' in group_df.columns else None
        
        results.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',
            'connectivity_group': group,
            'total_population': total_pop,
            'pop_male': pop_male,
            'pop_female': pop_female,
            'pct_female': pct_female,
            'pop_0_14': pop_0_14,
            'pop_15_64': pop_15_64,
            'pop_65_plus': pop_65_plus,
            'pct_0_14': pct_0_14,
            'pct_15_64': pct_15_64,
            'pct_65_plus': pct_65_plus,
            'pop_employed': pop_employed,
            'employment_rate': employment_rate,
            'pop_foreign': pop_foreign,
            'pct_foreign': pct_foreign,
            'hexagon_count': len(group_df),
            'total_area_km2': total_area_km2,
            'pop_density_per_km2': pop_density,
            'mean_healthcare_minutes': mean_healthcare,
            'mean_fixed_speed_mbps': mean_fixed_speed,
        })
    
    result = pl.DataFrame(results)
    
    print(f"  Processed {len(results)} groups")
    print(f"  Time: {time.time()-start:.1f}s")
    
    return result


def aggregate_demographics_by_nuts(df, level):
    """Aggregate demographics by NUTS level and connectivity group."""
    print(f"\nAggregating NUTS level {level} demographics...")
    start = time.time()
    
    nuts_id_col = f'nuts_id_{level}'
    nuts_name_col = f'nuts_name_{level}'
    
    if nuts_id_col not in df.columns:
        print(f"  ✗ NUTS level {level} not found in data")
        return None
    
    # Get unique regions
    regions = df.select([nuts_id_col, nuts_name_col]).unique().sort(nuts_id_col)
    
    print(f"  Processing {len(regions)} regions...")
    
    results = []
    hexagon_area_km2 = 0.737327598
    
    for region_row in regions.iter_rows(named=True):
        region_id = region_row[nuts_id_col]
        region_name = region_row.get(nuts_name_col, region_id)
        
        region_df = df.filter(pl.col(nuts_id_col) == region_id)
        
        for group in ['underserved', 'well_connected', 'intermediate']:
            group_df = region_df.filter(pl.col('connectivity_group') == group)
            
            if len(group_df) == 0:
                continue
            
            # Calculate metrics (same as Europe aggregation)
            total_pop = group_df['pop_total'].sum()
            
            # Check if columns exist and values are not None
            pop_male = None
            if 'pop_male' in group_df.columns:
                val = group_df['pop_male'].sum()
                pop_male = val if val is not None else None
                
            pop_female = None
            if 'pop_female' in group_df.columns:
                val = group_df['pop_female'].sum()
                pop_female = val if val is not None else None
            
            pct_female = (pop_female / total_pop * 100) if pop_female is not None and pop_female > 0 and total_pop > 0 else None
            
            # Age groups (if available) - FIXED column names
            pop_0_14 = None
            if 'pop_age_lt15' in group_df.columns:
                val = group_df['pop_age_lt15'].sum()
                pop_0_14 = val if val is not None else None
                
            pop_15_64 = None
            if 'pop_age_15_64' in group_df.columns:
                val = group_df['pop_age_15_64'].sum()
                pop_15_64 = val if val is not None else None
                
            pop_65_plus = None
            if 'pop_age_ge65' in group_df.columns:
                val = group_df['pop_age_ge65'].sum()
                pop_65_plus = val if val is not None else None
            
            pct_0_14 = (pop_0_14 / total_pop * 100) if pop_0_14 is not None and pop_0_14 > 0 and total_pop > 0 else None
            pct_15_64 = (pop_15_64 / total_pop * 100) if pop_15_64 is not None and pop_15_64 > 0 and total_pop > 0 else None
            pct_65_plus = (pop_65_plus / total_pop * 100) if pop_65_plus is not None and pop_65_plus > 0 and total_pop > 0 else None
            
            pop_employed = None
            if 'pop_employed' in group_df.columns:
                val = group_df['pop_employed'].sum()
                pop_employed = val if val is not None else None
            employment_rate = (pop_employed / total_pop * 100) if pop_employed is not None and pop_employed > 0 and total_pop > 0 else None
            
            pop_foreign = None
            if 'pop_foreign_citizenship' in group_df.columns:
                val = group_df['pop_foreign_citizenship'].sum()
                pop_foreign = val if val is not None else None
            pct_foreign = (pop_foreign / total_pop * 100) if pop_foreign is not None and pop_foreign > 0 and total_pop > 0 else None
            
            total_area_km2 = len(group_df) * hexagon_area_km2
            pop_density = total_pop / total_area_km2 if total_area_km2 > 0 else 0
            
            # Mean healthcare distance (convert from seconds to minutes)
            mean_healthcare = None
            if 'health_distance' in group_df.columns:
                filtered = group_df.filter(pl.col('health_distance').is_not_null())
                if len(filtered) > 0:
                    mean_healthcare = filtered['health_distance'].mean() / 60.0  # Convert to minutes
            
            mean_fixed_speed = None
            if 'fixed_download_2023' in group_df.columns:
                filtered = group_df.filter(pl.col('fixed_download_2023').is_not_null())
                if len(filtered) > 0:
                    mean_fixed_speed = filtered['fixed_download_2023'].mean() / 1000
            
            results.append({
                'region': region_id,
                'region_name': region_name,
                'nuts_level': str(level),
                'connectivity_group': group,
                'total_population': total_pop,
                'pop_male': pop_male,
                'pop_female': pop_female,
                'pct_female': pct_female,
                'pop_0_14': pop_0_14,
                'pop_15_64': pop_15_64,
                'pop_65_plus': pop_65_plus,
                'pct_0_14': pct_0_14,
                'pct_15_64': pct_15_64,
                'pct_65_plus': pct_65_plus,
                'pop_employed': pop_employed,
                'employment_rate': employment_rate,
                'pop_foreign': pop_foreign,
                'pct_foreign': pct_foreign,
                'hexagon_count': len(group_df),
                'total_area_km2': total_area_km2,
                'pop_density_per_km2': pop_density,
                'mean_healthcare_minutes': mean_healthcare,
                'mean_fixed_speed_mbps': mean_fixed_speed,
            })
    
    result = pl.DataFrame(results)
    
    print(f"  Time: {time.time()-start:.1f}s")
    
    return result


def analyze_joint_vulnerability(df):
    """Analyze population with BOTH poor connectivity AND far from hospital."""
    print("\nAnalyzing joint vulnerability (disconnected + far from hospital)...")
    start = time.time()
    
    # Convert health distance to minutes for filtering
    df = df.with_columns([
        (pl.col('health_distance') / 60.0).alias('health_distance_minutes')
    ])
    
    results = []
    
    # Define vulnerability thresholds
    thresholds = [
        ('15min', 15),
        ('30min', 30)
    ]
    
    # Europe-wide
    total_pop = df['pop_total'].sum()
    
    for label, health_threshold in thresholds:
        # Vulnerable: (disconnected OR <10 Mbps) AND >threshold minutes from hospital
        vulnerable_df = df.filter(
            ((pl.col('fixed_download_2023').is_null()) | 
             (pl.col('fixed_download_2023') < 10_000)) &
            (pl.col('health_distance_minutes') > health_threshold)
        )
        
        vuln_pop = vulnerable_df['pop_total'].sum()
        vuln_pct = (vuln_pop / total_pop * 100) if total_pop > 0 else 0
        
        results.append({
            'region': 'Europe',
            'region_name': 'Europe',
            'nuts_level': 'All',
            'vulnerability_type': f'poor_internet_and_gt{label}',
            'threshold_minutes': health_threshold,
            'vulnerable_population': vuln_pop,
            'vulnerable_percentage': vuln_pct,
            'total_population': total_pop,
        })
    
    # By NUTS levels
    for level in [0, 1, 2, 3]:
        nuts_id_col = f'nuts_id_{level}'
        nuts_name_col = f'nuts_name_{level}'
        
        if nuts_id_col not in df.columns:
            continue
        
        print(f"  Processing NUTS level {level}...")
        regions = df.select([nuts_id_col, nuts_name_col]).unique().sort(nuts_id_col)
        
        for region_row in regions.iter_rows(named=True):
            region_id = region_row[nuts_id_col]
            region_name = region_row.get(nuts_name_col, region_id)
            
            region_df = df.filter(pl.col(nuts_id_col) == region_id)
            region_total_pop = region_df['pop_total'].sum()
            
            for label, health_threshold in thresholds:
                vulnerable_df = region_df.filter(
                    ((pl.col('fixed_download_2023').is_null()) | 
                     (pl.col('fixed_download_2023') < 10_000)) &
                    (pl.col('health_distance_minutes') > health_threshold)
                )
                
                vuln_pop = vulnerable_df['pop_total'].sum()
                vuln_pct = (vuln_pop / region_total_pop * 100) if region_total_pop > 0 else 0
                
                results.append({
                    'region': region_id,
                    'region_name': region_name,
                    'nuts_level': str(level),
                    'vulnerability_type': f'poor_internet_and_gt{label}',
                    'threshold_minutes': health_threshold,
                    'vulnerable_population': vuln_pop,
                    'vulnerable_percentage': vuln_pct,
                    'total_population': region_total_pop,
                })
    
    result = pl.DataFrame(results)
    print(f"  Time: {time.time()-start:.1f}s")
    
    return result


def main():
    """Main execution."""
    print("="*80)
    print("STEP 4: ANALYZE DEMOGRAPHICS")
    print("="*80)
    
    overall_start = time.time()
    
    try:
        # Load data
        df = load_data()
        
        # Classify groups
        df = classify_connectivity_groups(df)
        
        # Aggregate demographics
        all_data = []
        
        # Europe total
        europe_demo = aggregate_demographics_europe(df)
        all_data.append(europe_demo)
        
        # Each NUTS level
        for level in [0, 1, 2, 3]:
            nuts_demo = aggregate_demographics_by_nuts(df, level)
            if nuts_demo is not None:
                all_data.append(nuts_demo)
        
        # Combine all results
        print("\nCombining results...")
        final_results = pl.concat(all_data)
        
        # Save demographics
        final_results.write_parquet(OUTPUT_FILE)
        
        print(f"\n✓ Saved: {OUTPUT_FILE}")
        print(f"  Total records: {len(final_results):,}")
        
        # Analyze joint vulnerability
        joint_vuln = analyze_joint_vulnerability(df)
        
        # Save joint vulnerability
        vuln_file = OUTPUT_FILE.parent / "joint_vulnerability.parquet"
        joint_vuln.write_parquet(vuln_file)
        
        print(f"\n✓ Saved: {vuln_file}")
        print(f"  Total records: {len(joint_vuln):,}")
        
        # Print summary
        print("\n" + "="*80)
        print("DEMOGRAPHIC SUMMARY")
        print("="*80)
        
        europe_data = final_results.filter(pl.col('region') == 'Europe')
        
        for group in ['underserved', 'well_connected']:
            group_data = europe_data.filter(pl.col('connectivity_group') == group)
            if len(group_data) > 0:
                row = group_data.row(0, named=True)
                print(f"\n{group.upper()}:")
                print(f"  Population: {row['total_population']:,.0f}")
                print(f"  Population density: {row['pop_density_per_km2']:.1f} per km²")
                if row['pct_female']:
                    print(f"  Female: {row['pct_female']:.1f}%")
                if row['pct_0_14']:
                    print(f"  Age 0-14: {row['pct_0_14']:.1f}%")
                if row['pct_65_plus']:
                    print(f"  Age 65+: {row['pct_65_plus']:.1f}%")
                if row['mean_healthcare_minutes']:
                    print(f"  Mean healthcare distance: {row['mean_healthcare_minutes']:.1f} min")
        
        print("="*80)
        
        print(f"\n✓ COMPLETED in {time.time()-overall_start:.1f}s")
        return 0
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
