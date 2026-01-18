#!/usr/bin/env python3
"""
Generate publication-ready maps and charts.

This script:
1. Loads NUTS boundaries and analysis results
2. Creates choropleth maps for connectivity and healthcare
3. Generates charts for distributions and comparisons
4. Exports all visualizations as 300 DPI PNG files

Output: analysis/figures/maps/*.png and analysis/figures/charts/*.png
"""

import sys
from pathlib import Path
import time
import warnings
warnings.filterwarnings('ignore')

import polars as pl
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import BoundaryNorm
import numpy as np

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
ANALYSIS_DIR = PROJECT_ROOT / "analysis"
NUTS_DATA = DATA_DIR / "statistical_units" / "NUTS_RG_60M_2024_3035.gpkg"

CONNECTIVITY_FILE = ANALYSIS_DIR / "data" / "connectivity_analysis.parquet"
HEALTHCARE_FILE = ANALYSIS_DIR / "data" / "healthcare_analysis.parquet"
DEMOGRAPHICS_FILE = ANALYSIS_DIR / "data" / "demographics_analysis.parquet"
JOINT_VULN_FILE = ANALYSIS_DIR / "data" / "joint_vulnerability.parquet"

MAPS_DIR = ANALYSIS_DIR / "figures" / "maps"
CHARTS_DIR = ANALYSIS_DIR / "figures" / "charts"

# Central Europe country codes (exclude remote islands)
CENTRAL_EUROPE_COUNTRIES = [
    'AT', 'BE', 'BG', 'HR', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU',  
    'IE', 'IT', 'LV', 'LT', 'LU', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE',
    'NO', 'CH', 'GB', 'RS', 'BA', 'MK', 'AL', 'ME', 'XK'
    # Excluded: IS (Iceland), CY (Cyprus), MT (Malta)
]

# Publication settings
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.size'] = 10
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['axes.labelsize'] = 10
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['xtick.labelsize'] = 9
plt.rcParams['ytick.labelsize'] = 9
plt.rcParams['legend.fontsize'] = 9


def filter_central_europe(gdf):
    """Filter GeoDataFrame to show only central Europe (exclude remote islands)."""
    if 'CNTR_CODE' in gdf.columns:
        return gdf[gdf['CNTR_CODE'].isin(CENTRAL_EUROPE_COUNTRIES)].copy()
    # If no country code, try to extract from NUTS_ID
    elif 'nuts_id' in gdf.columns:
        gdf['country'] = gdf['nuts_id'].str[:2]
        result = gdf[gdf['country'].isin(CENTRAL_EUROPE_COUNTRIES)].copy()
        return result.drop(columns=['country'])
    return gdf


def create_quantile_norm(data_series, cmap_name, n_quantiles=7):
    """Create quantile-based normalization and colormap for mapping."""
    import matplotlib
    
    # Remove NaN values
    clean_data = data_series.dropna()
    
    if len(clean_data) == 0:
        return None, None, None
    
    # Calculate quantiles
    quantiles = np.linspace(0, 1, n_quantiles + 1)
    boundaries = clean_data.quantile(quantiles).values
    
    # Ensure boundaries are unique
    boundaries = np.unique(boundaries)
    n_colors = len(boundaries) - 1
    
    # Create colormap with exact number of colors
    cmap = matplotlib.colormaps[cmap_name].resampled(n_colors)
    
    # Create boundary normalization
    norm = BoundaryNorm(boundaries, ncolors=n_colors)
    
    return cmap, norm, boundaries


def load_nuts_boundaries(level):
    """Load NUTS boundaries for a specific level."""
    print(f"  Loading NUTS level {level} boundaries...")
    
    # Load the single layer containing all NUTS levels
    gdf_all = gpd.read_file(NUTS_DATA)
    
    # Filter by LEVL_CODE
    gdf = gdf_all[gdf_all['LEVL_CODE'] == level].copy()
    
    if len(gdf) == 0:
        print(f"    ✗ No regions found for level {level}")
        return None
    
    # Standardize column names
    gdf = gdf[['NUTS_ID', 'NAME_LATN', 'geometry']].copy()
    gdf.columns = ['nuts_id', 'nuts_name', 'geometry']
    
    print(f"    Loaded {len(gdf)} regions")
    return gdf


def create_connectivity_map(level):
    """Create choropleth map of % with good connectivity (≥100 Mbps) by NUTS level."""
    print(f"\nCreating connectivity map for NUTS level {level}...")
    start = time.time()
    
    # Load data
    gdf = load_nuts_boundaries(level)
    if gdf is None:
        return
    
    connectivity_df = pl.read_parquet(CONNECTIVITY_FILE)
    
    # Filter for this level, fixed internet, GOOD tier (≥100 Mbps)
    level_data = connectivity_df.filter(
        (pl.col('nuts_level') == str(level)) &
        (pl.col('metric_type') == 'fixed') &
        (pl.col('tier') == 'good')
    )
    
    # Convert to pandas for merge
    level_pd = level_data.to_pandas()
    
    # Merge with geometries
    gdf = gdf.merge(level_pd[['region', 'percentage']], 
                    left_on='nuts_id', right_on='region', how='left')
    
    # Filter to central Europe BEFORE creating quantiles
    gdf = filter_central_europe(gdf)
    
    # Remove regions with no data (NaN percentage)
    gdf = gdf[gdf['percentage'].notna()].copy()
    
    print(f"    Regions after filtering: {len(gdf)}")
    print(f"    Percentage range: {gdf['percentage'].min():.1f} - {gdf['percentage'].max():.1f}")
    
    # Create quantile-based normalization (7 buckets)
    cmap, norm, boundaries = create_quantile_norm(gdf['percentage'], 'YlGn', n_quantiles=7)
    
    if norm is None:
        print("    ✗ Could not create quantile normalization")
        return
    
    print(f"    Quantile boundaries: {[f'{b:.1f}' for b in boundaries]}")
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # Plot with quantile-based colors (GREEN for good connectivity)
    gdf.plot(column='percentage',
             ax=ax,
             cmap=cmap,
             edgecolor='black',
             linewidth=0.5,
             norm=norm,
             legend=True,
             legend_kwds={'label': '% Population with Good Connectivity (≥100 Mbps)',
                          'orientation': 'horizontal',
                          'shrink': 0.6,
                          'pad': 0.05})
    
    # Crop to central Europe mainland (exclude Iceland, etc.)
    # Set bounds: approximately 10°W to 35°E, 35°N to 72°N in EPSG:3035
    bounds = gdf.total_bounds
    # Add 5% margin
    x_margin = (bounds[2] - bounds[0]) * 0.05
    y_margin = (bounds[3] - bounds[1]) * 0.05
    ax.set_xlim(bounds[0] - x_margin, bounds[2] + x_margin)
    ax.set_ylim(bounds[1] - y_margin, bounds[3] + y_margin)
    
    ax.set_title(f'Internet Connectivity - NUTS Level {level}\n% Population with Good Fixed Broadband (≥100 Mbps)',
                 fontsize=16, fontweight='bold', pad=20)
    ax.axis('off')
    
    # Add note
    fig.text(0.5, 0.02, 
             'Data: Ookla 2023, Eurostat Census 2021 | NUTS 2024 Classification | 7-Quantile Classification',
             ha='center', fontsize=9, style='italic')
    
    plt.tight_layout()
    
    # Save
    output_file = MAPS_DIR / f'connectivity_nuts{level}.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"    ✓ Saved: {output_file}")
    print(f"    Time: {time.time()-start:.1f}s")


def create_healthcare_map(level):
    """Create choropleth map of mean healthcare travel time by NUTS level."""
    print(f"\nCreating healthcare map for NUTS level {level}...")
    start = time.time()
    
    # Load data
    gdf = load_nuts_boundaries(level)
    if gdf is None:
        return
    
    # Load healthcare stats
    healthcare_stats = pl.read_parquet(ANALYSIS_DIR / "data" / "healthcare_stats.parquet")
    
    # Filter for this level, mean distance
    level_data = healthcare_stats.filter(
        (pl.col('nuts_level') == str(level)) &
        (pl.col('metric_name') == 'mean_distance_minutes')
    )
    
    # Convert to pandas for merge
    level_pd = level_data.to_pandas()
    
    # Merge with geometries
    gdf = gdf.merge(level_pd[['region', 'value']], 
                    left_on='nuts_id', right_on='region', how='left')
    
    # Filter to central Europe BEFORE creating quantiles
    gdf = filter_central_europe(gdf)
    
    # Remove regions with no data
    gdf = gdf[gdf['value'].notna()].copy()
    
    print(f"    Regions after filtering: {len(gdf)}")
    print(f"    Value range: {gdf['value'].min():.1f} - {gdf['value'].max():.1f} minutes")
    
    # Create quantile-based normalization (7 buckets)
    cmap, norm, boundaries = create_quantile_norm(gdf['value'], 'RdYlGn_r', n_quantiles=7)
    
    if norm is None:
        print("    ✗ Could not create quantile normalization")
        return
    
    print(f"    Quantile boundaries: {[f'{b:.1f}' for b in boundaries]}")
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # Plot with quantile-based colors
    gdf.plot(column='value',
             ax=ax,
             cmap=cmap,
             edgecolor='black',
             linewidth=0.5,
             norm=norm,
             legend=True,
             legend_kwds={'label': 'Mean Travel Time (minutes)',
                          'orientation': 'horizontal',
                          'shrink': 0.6,
                          'pad': 0.05})
    
    # Crop to central Europe mainland
    bounds = gdf.total_bounds
    x_margin = (bounds[2] - bounds[0]) * 0.05
    y_margin = (bounds[3] - bounds[1]) * 0.05
    ax.set_xlim(bounds[0] - x_margin, bounds[2] + x_margin)
    ax.set_ylim(bounds[1] - y_margin, bounds[3] + y_margin)
    
    ax.set_title(f'Healthcare Accessibility - NUTS Level {level}\nMean Travel Time to Nearest Hospital',
                 fontsize=16, fontweight='bold', pad=20)
    ax.axis('off')
    
    # Add note
    fig.text(0.5, 0.02, 
             'Data: JRC Healthcare Accessibility 2023 | NUTS 2024 Classification | 7-Quantile Classification',
             ha='center', fontsize=9, style='italic')
    
    plt.tight_layout()
    
    # Save
    output_file = MAPS_DIR / f'healthcare_access_nuts{level}.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"    ✓ Saved: {output_file}")
    print(f"    Time: {time.time()-start:.1f}s")


def create_underserved_map(level):
    """Create map showing % underserved population (disconnected + <10 Mbps)."""
    print(f"\nCreating underserved population map for NUTS level {level}...")
    start = time.time()
    
    # Load data
    gdf = load_nuts_boundaries(level)
    if gdf is None:
        return
    
    connectivity_df = pl.read_parquet(CONNECTIVITY_FILE)
    
    # Filter for this level, fixed internet, disconnected + very_poor tiers
    level_data = connectivity_df.filter(
        (pl.col('nuts_level') == str(level)) &
        (pl.col('metric_type') == 'fixed') &
        (pl.col('tier').is_in(['disconnected', 'very_poor']))
    )
    
    # Sum percentages by region
    underserved = level_data.group_by('region').agg([
        pl.col('percentage').sum().alias('pct_underserved'),
        pl.col('population').sum().alias('pop_underserved')
    ])
    
    # Convert to pandas for merge
    underserved_pd = underserved.to_pandas()
    
    # Merge with geometries
    gdf = gdf.merge(underserved_pd[['region', 'pct_underserved', 'pop_underserved']], 
                    left_on='nuts_id', right_on='region', how='left')
    
    # Filter to central Europe BEFORE creating quantiles
    gdf = filter_central_europe(gdf)
    
    # Remove regions with no data
    gdf = gdf[gdf['pct_underserved'].notna()].copy()
    
    print(f"    Regions after filtering: {len(gdf)}")
    print(f"    Underserved % range: {gdf['pct_underserved'].min():.1f} - {gdf['pct_underserved'].max():.1f}")
    
    # Create quantile-based normalization (7 buckets)
    cmap, norm, boundaries = create_quantile_norm(gdf['pct_underserved'], 'OrRd', n_quantiles=7)
    
    if norm is None:
        print("    ✗ Could not create quantile normalization")
        return
    
    print(f"    Quantile boundaries: {[f'{b:.1f}' for b in boundaries]}")
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # Plot with quantile-based colors
    gdf.plot(column='pct_underserved',
             ax=ax,
             cmap=cmap,
             edgecolor='black',
             linewidth=0.5,
             norm=norm,
             legend=True,
             legend_kwds={'label': '% Population Underserved',
                          'orientation': 'horizontal',
                          'shrink': 0.6,
                          'pad': 0.05})
    
    # Crop to central Europe mainland
    bounds = gdf.total_bounds
    x_margin = (bounds[2] - bounds[0]) * 0.05
    y_margin = (bounds[3] - bounds[1]) * 0.05
    ax.set_xlim(bounds[0] - x_margin, bounds[2] + x_margin)
    ax.set_ylim(bounds[1] - y_margin, bounds[3] + y_margin)
    
    ax.set_title(f'Underserved Population - NUTS Level {level}\n% with No Data or <10 Mbps Fixed Internet',
                 fontsize=16, fontweight='bold', pad=20)
    ax.axis('off')
    
    # Add note
    fig.text(0.5, 0.02, 
             'Data: Ookla 2023, Eurostat Census 2021 | NUTS 2024 Classification | 7-Quantile Classification',
             ha='center', fontsize=9, style='italic')
    
    plt.tight_layout()
    
    # Save
    output_file = MAPS_DIR / f'underserved_population_nuts{level}.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"    ✓ Saved: {output_file}")
    print(f"    Time: {time.time()-start:.1f}s")


def create_connectivity_distribution_chart():
    """Create stacked bar chart of population by connectivity tier."""
    print("\nCreating connectivity distribution chart...")
    start = time.time()
    
    connectivity_df = pl.read_parquet(CONNECTIVITY_FILE)
    
    # Filter for Europe and NUTS0, fixed internet
    data = connectivity_df.filter(
        (pl.col('nuts_level').cast(pl.Utf8).is_in(['All', '0'])) &
        (pl.col('metric_type') == 'fixed')
    )
    
    # Pivot data for plotting
    pivot = data.pivot(
        values='population',
        index='region_name',
        columns='tier'
    ).to_pandas().set_index('region_name')
    
    # Reorder tiers
    tier_order = ['disconnected', 'very_poor', 'poor', 'basic', 'good']
    pivot = pivot[[c for c in tier_order if c in pivot.columns]]
    
    # Convert to millions
    pivot = pivot / 1_000_000
    
    # Sort by total population (descending), keep Europe first
    europe_row = pivot.loc[['Europe']] if 'Europe' in pivot.index else None
    other_rows = pivot[pivot.index != 'Europe'].sum(axis=1).sort_values(ascending=False).index[:10]
    
    if europe_row is not None:
        import pandas as pd
        pivot = pd.concat([europe_row, pivot.loc[other_rows]])
    else:
        pivot = pivot.loc[other_rows]
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Colors
    colors = ['#d62728', '#ff7f0e', '#ffbb78', '#98df8a', '#2ca02c']
    
    # Plot
    pivot.plot(kind='barh', stacked=True, ax=ax, color=colors, width=0.7)
    
    ax.set_xlabel('Population (millions)', fontsize=11)
    ax.set_ylabel('')
    ax.set_title('Fixed Internet Connectivity Distribution\nPopulation by Speed Tier',
                 fontsize=14, fontweight='bold', pad=20)
    
    ax.legend(title='Speed Tier',
              labels=['Disconnected', 'Very Poor (<10 Mbps)', 'Poor (10-25 Mbps)', 
                      'Basic (25-100 Mbps)', 'Good (≥100 Mbps)'],
              loc='lower right')
    
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    
    # Save
    output_file = CHARTS_DIR / 'connectivity_distribution_europe.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"  ✓ Saved: {output_file}")
    print(f"  Time: {time.time()-start:.1f}s")


def create_population_by_tier_chart():
    """Create chart showing population counts by speed tier."""
    print("\nCreating population by tier chart...")
    start = time.time()
    
    connectivity_df = pl.read_parquet(CONNECTIVITY_FILE)
    
    # Filter for Europe, both fixed and mobile
    data = connectivity_df.filter(
        (pl.col('region') == 'Europe')
    )
    
    # Separate fixed and mobile
    fixed = data.filter(pl.col('metric_type') == 'fixed').sort('tier')
    mobile = data.filter(pl.col('metric_type') == 'mobile').sort('tier')
    
    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Fixed internet
    tiers = fixed['tier'].to_list()
    pop_millions = (fixed['population'] / 1_000_000).to_list()
    percentages = fixed['percentage'].to_list()
    
    colors = ['#d62728', '#ff7f0e', '#ffbb78', '#98df8a', '#2ca02c']
    
    bars1 = ax1.bar(range(len(tiers)), pop_millions, color=colors)
    ax1.set_xticks(range(len(tiers)))
    ax1.set_xticklabels(['Disconnected', 'Very Poor\n(<10 Mbps)', 'Poor\n(10-25 Mbps)',
                          'Basic\n(25-100 Mbps)', 'Good\n(≥100 Mbps)'], rotation=0, ha='center')
    ax1.set_ylabel('Population (millions)', fontsize=11)
    ax1.set_title('Fixed Internet', fontsize=12, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on bars with percentages
    for bar, pct in zip(bars1, percentages):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}M\n({pct:.1f}%)',
                ha='center', va='bottom', fontsize=8)
    
    # Mobile internet
    tiers_mobile = mobile['tier'].to_list()
    pop_millions_mobile = (mobile['population'] / 1_000_000).to_list()
    percentages_mobile = mobile['percentage'].to_list()
    
    bars2 = ax2.bar(range(len(tiers_mobile)), pop_millions_mobile, color=colors)
    ax2.set_xticks(range(len(tiers_mobile)))
    ax2.set_xticklabels(['Disconnected', 'Very Poor\n(<10 Mbps)', 'Poor\n(10-25 Mbps)',
                          'Basic\n(25-100 Mbps)', 'Good\n(≥100 Mbps)'], rotation=0, ha='center')
    ax2.set_ylabel('Population (millions)', fontsize=11)
    ax2.set_title('Mobile Internet', fontsize=12, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on bars with percentages
    for bar, pct in zip(bars2, percentages_mobile):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}M\n({pct:.1f}%)',
                ha='center', va='bottom', fontsize=8)
    
    fig.suptitle('European Population by Internet Speed Tier',
                 fontsize=14, fontweight='bold', y=1.00)
    
    plt.tight_layout()
    
    # Save
    output_file = CHARTS_DIR / 'population_by_speed_tier.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"  ✓ Saved: {output_file}")
    print(f"  Time: {time.time()-start:.1f}s")


def create_healthcare_distribution_chart():
    """Create histogram of healthcare travel time distribution."""
    print("\nCreating healthcare distribution chart...")
    start = time.time()
    
    healthcare_df = pl.read_parquet(HEALTHCARE_FILE)
    
    # Filter for Europe
    data = healthcare_df.filter(pl.col('region') == 'Europe')
    
    # Get categories, populations, and percentages
    categories = data['category'].to_list()
    populations = (data['population'] / 1_000_000).to_list()
    percentages = data['percentage'].to_list()
    
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Define order and colors
    cat_order = ['very_close', 'close', 'moderate', 'far', 'very_far', 'no_data']
    cat_labels = ['<5 min\n(Very Close)', '5-10 min\n(Close)', '10-15 min\n(Moderate)',
                  '15-30 min\n(Far)', '>30 min\n(Very Far)', 'No Data']
    colors = ['#2ca02c', '#98df8a', '#ffdd71', '#ff7f0e', '#d62728', '#7f7f7f']
    
    # Reorder data
    ordered_pops = []
    ordered_pcts = []
    for cat in cat_order:
        idx = categories.index(cat) if cat in categories else None
        if idx is not None:
            ordered_pops.append(populations[idx])
            ordered_pcts.append(percentages[idx])
        else:
            ordered_pops.append(0)
            ordered_pcts.append(0)
    
    bars = ax.bar(range(len(cat_order)), ordered_pops, color=colors, width=0.7)
    
    ax.set_xticks(range(len(cat_order)))
    ax.set_xticklabels(cat_labels, rotation=0, ha='center')
    ax.set_ylabel('Population (millions)', fontsize=11)
    ax.set_title('Healthcare Accessibility Distribution\nPopulation by Travel Time to Nearest Hospital',
                 fontsize=14, fontweight='bold', pad=20)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on bars with percentages
    for bar, pct in zip(bars, ordered_pcts):
        height = bar.get_height()
        if height > 0:
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}M\n({pct:.1f}%)',
                   ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    
    # Save
    output_file = CHARTS_DIR / 'healthcare_distance_distribution.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"  ✓ Saved: {output_file}")
    print(f"  Time: {time.time()-start:.1f}s")


def create_demographics_chart():
    """Create comparison chart of demographics: underserved vs well-connected."""
    print("\nCreating demographics comparison chart...")
    start = time.time()
    
    demographics_df = pl.read_parquet(DEMOGRAPHICS_FILE)
    
    # Filter for Europe
    data = demographics_df.filter(pl.col('region') == 'Europe')
    
    underserved = data.filter(pl.col('connectivity_group') == 'underserved')
    well_connected = data.filter(pl.col('connectivity_group') == 'well_connected')
    
    if len(underserved) == 0 or len(well_connected) == 0:
        print("  ✗ Insufficient data for demographics chart")
        return
    
    # Extract metrics
    metrics = ['pct_0_14', 'pct_15_64', 'pct_65_plus', 'pct_female', 
               'pop_density_per_km2', 'mean_healthcare_minutes']
    
    underserved_vals = []
    connected_vals = []
    
    for metric in metrics:
        u_val = underserved[metric][0] if len(underserved) > 0 else 0
        c_val = well_connected[metric][0] if len(well_connected) > 0 else 0
        underserved_vals.append(u_val if u_val is not None else 0)
        connected_vals.append(c_val if c_val is not None else 0)
    
    # Create figure with subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))
    
    # 1. Age distribution
    age_categories = ['0-14', '15-64', '65+']
    x = np.arange(len(age_categories))
    width = 0.35
    
    ax1.bar(x - width/2, underserved_vals[:3], width, label='Underserved', color='#d62728')
    ax1.bar(x + width/2, connected_vals[:3], width, label='Well-Connected', color='#2ca02c')
    ax1.set_ylabel('Percentage (%)')
    ax1.set_title('Age Distribution', fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(age_categories)
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)
    
    # 2. Gender ratio
    gender_cats = ['Female %']
    x2 = np.arange(len(gender_cats))
    
    ax2.bar(x2 - width/2, [underserved_vals[3]], width, label='Underserved', color='#d62728')
    ax2.bar(x2 + width/2, [connected_vals[3]], width, label='Well-Connected', color='#2ca02c')
    ax2.set_ylabel('Percentage (%)')
    ax2.set_title('Gender Distribution', fontweight='bold')
    ax2.set_xticks(x2)
    ax2.set_xticklabels(gender_cats)
    ax2.set_ylim(0, 100)
    ax2.legend()
    ax2.grid(axis='y', alpha=0.3)
    
    # 3. Population density
    density_cats = ['Pop. Density\n(per km²)']
    x3 = np.arange(len(density_cats))
    
    ax3.bar(x3 - width/2, [underserved_vals[4]], width, label='Underserved', color='#d62728')
    ax3.bar(x3 + width/2, [connected_vals[4]], width, label='Well-Connected', color='#2ca02c')
    ax3.set_ylabel('People per km²')
    ax3.set_title('Population Density', fontweight='bold')
    ax3.set_xticks(x3)
    ax3.set_xticklabels(density_cats)
    ax3.legend()
    ax3.grid(axis='y', alpha=0.3)
    
    # 4. Healthcare accessibility
    healthcare_cats = ['Mean Travel Time\n(minutes)']
    x4 = np.arange(len(healthcare_cats))
    
    ax4.bar(x4 - width/2, [underserved_vals[5]], width, label='Underserved', color='#d62728')
    ax4.bar(x4 + width/2, [connected_vals[5]], width, label='Well-Connected', color='#2ca02c')
    ax4.set_ylabel('Minutes')
    ax4.set_title('Healthcare Accessibility', fontweight='bold')
    ax4.set_xticks(x4)
    ax4.set_xticklabels(healthcare_cats)
    ax4.legend()
    ax4.grid(axis='y', alpha=0.3)
    
    fig.suptitle('Demographic Comparison: Underserved vs Well-Connected Populations',
                 fontsize=14, fontweight='bold', y=0.995)
    
    plt.tight_layout()
    
    # Save
    output_file = CHARTS_DIR / 'demographics_comparison.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"  ✓ Saved: {output_file}")
    print(f"  Time: {time.time()-start:.1f}s")


def main():
    """Main execution."""
    print("="*80)
    print("STEP 5: GENERATE MAPS AND CHARTS")
    print("="*80)
    
    overall_start = time.time()
    
    try:
        # Check input files
        for file in [CONNECTIVITY_FILE, HEALTHCARE_FILE, DEMOGRAPHICS_FILE]:
            if not file.exists():
                print(f"✗ Error: {file} not found")
                print("  Run previous analysis scripts first")
                return 1
        
        # Create maps for each NUTS level
        print("\n" + "="*80)
        print("GENERATING MAPS")
        print("="*80)
        
        for level in [0, 1, 2, 3]:
            create_connectivity_map(level)
            create_healthcare_map(level)
            if level == 2:  # Create underserved map for NUTS2 only
                create_underserved_map(level)
        
        # Create charts
        print("\n" + "="*80)
        print("GENERATING CHARTS")
        print("="*80)
        
        create_connectivity_distribution_chart()
        create_population_by_tier_chart()
        create_healthcare_distribution_chart()
        create_demographics_chart()
        
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        
        map_count = len(list(MAPS_DIR.glob('*.png')))
        chart_count = len(list(CHARTS_DIR.glob('*.png')))
        
        print(f"✓ Generated {map_count} maps in {MAPS_DIR}")
        print(f"✓ Generated {chart_count} charts in {CHARTS_DIR}")
        
        print(f"\n✓ COMPLETED in {time.time()-overall_start:.1f}s")
        return 0
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
