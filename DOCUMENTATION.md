# Speed Connection Europe - Technical Documentation

## Project Overview

This project aggregates three heterogeneous spatial datasets for Europe into a unified H3 hexagonal grid at resolution 8 (~0.74 km² per hexagon):

1. **Population**: Eurostat Census Grid 2021 (1km × 1km cells)
2. **Healthcare Access**: JRC Healthcare Accessibility 2023 (100m raster)
3. **Internet Speed**: Ookla Open Data 2019-2025 (quadkey tiles)

The output is a single parquet file with harmonized data suitable for spatial analysis, visualization, and modeling.

## Architecture

### Data Flow

```
Raw Data Sources
├── Eurostat Census Grid (GPKG, EPSG:3035)
├── Healthcare Raster (GeoTIFF, EPSG:3035)  
└── Ookla Speed Test (Parquet, Quadkeys)
                ↓
        Weight Matrices
├── grid_h3_matrix.py → matrix_grid_h3_weights.parquet
└── quadkey_h3_matrix.py → matrix_quadkey_h3_weights.parquet
                ↓
            ETL Scripts
├── etl_population.py → population_census_2021_h3_res8.parquet
├── etl_health.py → euro_access_healthcare_2023_h3_res8.parquet
└── etl_internet.py → internet_speed_h3_res8.parquet
                ↓
         Merge Script
└── merge_datasets.py → data_h3_res8.parquet
```

## Installation

### Prerequisites

- Python 3.9+
- 16GB+ RAM recommended
- ~2GB disk space for source data
- ~5GB disk space for intermediate and final outputs

### Setup

```bash
# Clone repository
cd speed_connection_europe

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Quick Start

Run the complete pipeline:

```bash
python3 run_pipeline.py
```

This executes all steps in order. The pipeline will skip steps with existing outputs.

### Individual Scripts

Run scripts separately for more control:

```bash
# Step 1: Generate weight matrices (run once)
python3 matrix/grid_h3_matrix.py
python3 matrix/quadkey_h3_matrix.py

# Step 2: Process datasets
python3 etl_population.py
python3 etl_health.py
python3 etl_internet.py

# Step 3: Merge
python3 merge_datasets.py
```

## Configuration

### Key Parameters

All scripts use these constants (defined at top of each file):

- `H3_RESOLUTION = 8`: H3 resolution (~0.74 km² hexagons)
- `K_RING_SIZE = 1`: Search radius for geometric intersections
- `BATCH_SIZE = 500`: Parallel processing batch size
- `MAX_WORKERS`: Auto-detected (CPU count - 1)

### Modifying H3 Resolution

To change resolution, update `H3_RESOLUTION` in all scripts:
- Resolution 7: ~5.16 km² (fewer, larger hexagons)
- Resolution 8: ~0.74 km² (default)
- Resolution 9: ~0.10 km² (more, smaller hexagons)

**Note**: Changing resolution requires regenerating weight matrices and rerunning all ETL scripts.

## Script Documentation

### Matrix Generation Scripts

#### `matrix/grid_h3_matrix.py`

Generates spatial join weights between Eurostat 1km grid cells and H3 hexagons.

**Input**: `data/population/ESTAT_Census_2021_V2.gpkg`

**Output**: `matrix/outputs/matrix_grid_h3_weights.parquet`

**Columns**:
- `grid_id` (string): Grid cell identifier
- `h3_index` (string): H3 hexagon identifier
- `weight` (float): Intersection weight (normalized, sum=1 per grid)

**Method**:
1. Load grid cells from GPKG
2. Transform from EPSG:3035 to WGS84
3. Find candidate H3 cells using k-ring search
4. Calculate geometric intersection areas
5. Normalize weights (sum = 1.0 per source grid cell)

**Performance**: ~40s on M1 Mac, 254 MB output

#### `matrix/quadkey_h3_matrix.py`

Generates spatial join weights between Ookla quadkey tiles and H3 hexagons.

**Input**: All parquet files in `data/internet/` (scans for unique quadkeys)

**Output**: `matrix/outputs/matrix_quadkey_h3_weights.parquet`

**Columns**:
- `quadkey` (string): Quadkey tile identifier
- `h3_index` (string): H3 hexagon identifier  
- `weight` (float): Intersection weight (normalized, sum=1 per quadkey)

**Method**: Same as grid matrix, using mercantile for quadkey geometry

**Performance**: ~10s on M1 Mac, 1020 MB output

### ETL Scripts

#### `etl_population.py`

Processes Eurostat Census Grid to H3 hexagons with weighted aggregation.

**Input**: 
- `data/population/Eurostat_Census-GRID_2021_V2.2.zip`
- `matrix/outputs/matrix_grid_h3_weights.parquet`

**Output**: `data/population/population_census_2021_h3_res8.parquet`

**Columns**:
- `h3_index`, `h3_resolution`, `lat`, `lon`: Hexagon metadata
- `T`, `M`, `F`: Total, male, female population
- `Y_LT15`, `Y_1564`, `Y_GE65`: Age groups
- `EMP`: Employed population
- `NAT`, `EU_OTH`, `OTH`: Nationality groups
- `SAME`, `CHG_IN`, `CHG_OUT`: Residence changes
- `LAND_SURFACE`: Land area (km²)
- `cell_count`: Number of source grid cells

**Processing**:
1. Extract GPKG from zip
2. Join with weight matrix
3. Calculate weighted values: `value_h3 = Σ(value_grid × weight)`
4. Group by H3 and sum
5. Add coordinates and metadata

**Performance**: ~40s, 585 MB output, 7M hexagons

#### `etl_health.py`

Processes healthcare accessibility raster to H3 hexagons with pixel aggregation.

**Input**: `data/health/euro_access_healthcare_2023_100m_v2025_11.tif`

**Output**: `data/health/euro_access_healthcare_2023_h3_res8.parquet`

**Columns**:
- `h3_index`, `h3_resolution`, `lat`, `lon`: Hexagon metadata
- `accessibility_mean`: Mean travel time to nearest hospital (minutes)
- `accessibility_median`, `accessibility_min`, `accessibility_max`, `accessibility_std`: Statistics
- `band2_*`: Auxiliary band statistics
- `pixel_count`: Number of raster pixels per hexagon

**Processing**:
1. Read raster in chunks (1000 rows at a time)
2. Filter valid pixels (nodata != value)
3. Transform coordinates from EPSG:3035 to WGS84
4. Convert to H3 indices
5. Accumulate pixel values per H3 cell
6. Calculate statistics (mean, median, min, max, std)

**Performance**: ~8 min, 297 MB output, 5.3M hexagons, 238M pixels

#### `etl_internet.py`

Aggregates Ookla speed test data to H3 hexagons with weighted averages.

**Input**:
- `data/internet/*.parquet` (54 files: 27 quarters × 2 types)
- `matrix/outputs/matrix_quadkey_h3_weights.parquet`

**Output**: `data/internet/internet_speed_h3_res8.parquet`

**Columns** (for fixed and mobile):
- `h3_index`, `h3_resolution`, `lat`, `lon`: Hexagon metadata
- `fixed_download_2023`, `fixed_upload_2023`, `fixed_latency_2023`: 2023 averages (kbps, ms)
- `fixed_download_total`, `fixed_upload_total`, `fixed_latency_total`: All-time averages
- Same for `mobile_*`

**Processing**:
1. For each quarter file:
   - Join with weight matrix
   - Calculate weighted values
   - Group by H3 and aggregate
   - Cache intermediate result
2. Merge all quarters using lazy streaming
3. Calculate 2023 and total averages
4. Drop quarter-level columns (keep only aggregated)

**Performance**: ~30 min, ~500 MB output, 24M hexagons

**Caching**: Intermediate files (`*_h3res8.parquet`) are cached. Delete to reprocess.

### Merge Script

#### `merge_datasets.py`

Merges population, health, and internet datasets into final output.

**Inputs**:
- `data/population/population_census_2021_h3_res8.parquet`
- `data/health/euro_access_healthcare_2023_h3_res8.parquet`
- `data/internet/internet_speed_h3_res8.parquet`

**Output**: `data/data_h3_res8.parquet`

**Processing**:
1. Prepare datasets:
   - Rename population columns with `pop_` prefix
   - Rename `accessibility_mean` to `health_distance`
   - Keep only aggregated internet columns (2023 and total)
2. Merge:
   - Population ∩ Health (inner join): ~3.8M cells
   - Result + Internet (left join): ~3.8M cells
3. Filter: Keep only cells with `pop_total > 0` AND `health_distance IS NOT NULL`
4. Optimize types and precision:
   - Round floats to 2 decimals (lat/lon to 6)
   - Cast integers to Int64
5. Reorder columns: metadata, population, health, internet

**Final Schema**:

| Column | Type | Description |
|--------|------|-------------|
| `h3_index` | string | H3 hexagon ID |
| `lat`, `lon` | float64 | Centroid coordinates (WGS84) |
| `pop_total` | int64 | Total population |
| `pop_male`, `pop_female` | int64 | Population by gender |
| `pop_age_lt15`, `pop_age_15_64`, `pop_age_ge65` | int64 | Age groups |
| `pop_employed` | int64 | Employed population |
| `pop_national`, `pop_eu_other`, `pop_other` | int64 | Citizenship |
| `pop_same_residence`, `pop_change_in`, `pop_change_out` | int64 | Migration |
| `health_distance` | float64 | Travel time to hospital (min) |
| `fixed_download_2023` | float64 | Fixed download 2023 (kbps) |
| `fixed_upload_2023` | float64 | Fixed upload 2023 (kbps) |
| `fixed_latency_2023` | float64 | Fixed latency 2023 (ms) |
| `fixed_download_total` | float64 | Fixed download all-time (kbps) |
| `fixed_upload_total` | float64 | Fixed upload all-time (kbps) |
| `fixed_latency_total` | float64 | Fixed latency all-time (ms) |
| `mobile_*` | float64 | Same metrics for mobile |

**Performance**: ~1 min, final output ~250 MB, ~3.8M rows

## Data Sources

### Population Data

**Source**: Eurostat Census Grid 2021 Version 2.2  
**URL**: https://ec.europa.eu/eurostat/web/gisco/geodata/population-distribution/geostat  
**License**: CC BY 4.0  
**Format**: GeoPackage (GPKG)  
**CRS**: EPSG:3035 (ETRS89-extended / LAEA Europe)  
**Resolution**: 1 km × 1 km grid cells  
**Coverage**: EU27 + EFTA + UK + candidate countries  
**Temporal**: 2021 census  

### Healthcare Accessibility Data

**Source**: JRC European Commission  
**Dataset**: Healthcare accessibility 2023  
**URL**: https://data.jrc.ec.europa.eu/  
**License**: CC BY 4.0  
**Format**: GeoTIFF  
**CRS**: EPSG:3035  
**Resolution**: 100 m × 100 m pixels  
**Values**: Travel time to nearest hospital (minutes)  
**Coverage**: Europe  
**Temporal**: 2023  

### Internet Speed Data

**Source**: Ookla Open Data  
**URL**: https://github.com/teamookla/ookla-open-data  
**License**: CC BY-NC-SA 4.0  
**Format**: Parquet (partitioned by year/quarter)  
**Spatial**: Quadkey tiles (Bing Maps)  
**Metrics**: Download/upload speed (kbps), latency (ms), test counts  
**Types**: Fixed broadband, mobile  
**Coverage**: Global (filtered to Europe via quadkeys)  
**Temporal**: Q1 2019 - Q3 2025  

## Troubleshooting

### Common Issues

#### "Matrix file not found"
Run the matrix generation scripts first:
```bash
python3 matrix/grid_h3_matrix.py
python3 matrix/quadkey_h3_matrix.py
```

#### "GPKG file not found"
Download and extract the population data manually to `data/population/`.

#### "Out of memory" during internet ETL
The internet ETL processes 24M+ hexagons. Increase available RAM or reduce data:
- Process fewer quarters (edit `QUARTERS` list in `etl_internet.py`)
- Use a lower H3 resolution (fewer hexagons)

#### Slow performance
- Use SSD storage
- Ensure virtual environment is activated
- Check that polars is using multiple cores (`MAX_WORKERS`)

### Data Validation

Check output with polars:

```python
import polars as pl

df = pl.read_parquet("data/data_h3_res8.parquet")
print(df.shape)
print(df.schema)
print(df.describe())
print(df.null_count())
```

Expected output:
- ~3.8M rows
- ~30 columns
- No nulls in population or health columns
- Some nulls in internet columns (not all hexagons have internet data)

## Performance Optimization

### Memory Management

**Polars Lazy Evaluation**: Use `scan_parquet()` instead of `read_parquet()` where possible to avoid loading entire datasets into memory.

**Streaming Merges**: The internet ETL uses streaming to merge 54 files without loading all at once.

**Caching**: Intermediate results are cached to avoid reprocessing.

### Parallelization

**Matrix Generation**: Uses `ProcessPoolExecutor` with `MAX_WORKERS` processes.

**Batch Processing**: Processes data in batches (500 items) to balance memory and speed.

### Disk I/O

**Parquet Format**: Columnar format with compression (snappy) for fast reads/writes.

**Chunked Reading**: Raster data is read in chunks to limit memory usage.

## Technical Choices

### Why Polars?

- **Performance**: 5-10× faster than pandas for large datasets
- **Memory efficiency**: Lazy evaluation and optimized query engine
- **Native types**: Better handling of nulls and categorical data
- **Future-proof**: Modern, actively developed library

### Why H3 Resolution 8?

Balance between spatial precision and data volume:
- Resolution 7: 5.16 km² (too coarse for urban analysis)
- Resolution 8: 0.74 km² (good balance)
- Resolution 9: 0.10 km² (too fine, 7× more hexagons)

### Why Precomputed Matrices?

Direct spatial joins for 4M grid cells × 7M H3 cells would require 28 trillion comparisons. Precomputed matrices:
- Calculate once, use many times
- ~1 GB storage vs hours of repeated computation
- Enable fast weighted aggregation

### Why Weighted Aggregation?

Source data doesn't align with H3 boundaries. Weighted aggregation:
- Preserves statistical accuracy
- Avoids arbitrary assignment
- Normalizes different spatial resolutions

## License

This project: MIT License

Data sources have separate licenses (see Data Sources section).

## Contact & Support

For issues or questions, please open an issue on GitHub.
