# Speed Connection Europe

Unified spatial dataset combining European population, healthcare accessibility, and internet speed data on H3 hexagonal grid (resolution 8, ~0.74 km² per hexagon).

## Quick Start

```bash
# Setup
git clone <repository-url>
cd speed_connection_europe
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run complete pipeline
python3 run_pipeline.py
```

Output: `data/data_h3_res8.parquet` (~250 MB, 3.8M hexagons)

## What's Included

### Data Sources

1. **Population** (Eurostat Census 2021)
   - Total, gender, age groups, employment, citizenship, migration
   - 1 km grid cells → H3 weighted aggregation

2. **Healthcare Accessibility** (JRC 2023)
   - Travel time to nearest hospital (minutes)
   - 100 m raster → H3 pixel aggregation

3. **Internet Speed** (Ookla 2019-2025)
   - Fixed and mobile: download, upload, latency
   - Quarterly data → 2023 and all-time averages

### Output Schema

| Column Group | Examples | Count |
|--------------|----------|-------|
| Metadata | `h3_index`, `lat`, `lon` | 3 |
| Population | `pop_total`, `pop_male`, `pop_age_15_64` | 13 |
| Healthcare | `health_distance` | 1 |
| Internet | `fixed_download_2023`, `mobile_latency_total` | 12 |

**Total**: 29 columns, 3.8M rows, ~250 MB

## Project Structure

```
speed_connection_europe/
├── data/
│   ├── population/          # Eurostat Census data
│   ├── health/              # Healthcare accessibility raster
│   ├── internet/            # Ookla speed test data
│   └── data_h3_res8.parquet # Final output
├── matrix/
│   ├── outputs/             # Precomputed weight matrices
│   ├── grid_h3_matrix.py    # Population matrix generation
│   └── quadkey_h3_matrix.py # Internet matrix generation
├── etl_population.py        # Population ETL
├── etl_health.py            # Health ETL
├── etl_internet.py          # Internet ETL
├── merge_datasets.py        # Merge all datasets
├── run_pipeline.py          # Orchestrator script
└── requirements.txt         # Dependencies
```

## Usage

### Complete Pipeline

```bash
python3 run_pipeline.py
```

Runs all steps:
1. Generate weight matrices (if needed)
2. Process population, health, internet data
3. Merge into final dataset

### Individual Steps

```bash
# Generate matrices (one-time setup)
python3 matrix/grid_h3_matrix.py
python3 matrix/quadkey_h3_matrix.py

# Process datasets
python3 etl_population.py
python3 etl_health.py
python3 etl_internet.py

# Merge
python3 merge_datasets.py
```

### Using the Output

```python
import polars as pl

# Load data
df = pl.read_parquet("data/data_h3_res8.parquet")

# Basic info
print(df.shape)  # (3,806,492, 29)
print(df.schema)

# Filter example: Urban areas with good internet
urban_high_speed = df.filter(
    (pl.col('pop_total') > 500) &
    (pl.col('fixed_download_2023') > 100_000)  # >100 Mbps
)

# Aggregate by region
df.group_by('health_distance').agg([
    pl.col('pop_total').sum(),
    pl.col('fixed_download_2023').mean()
])
```

## Key Features

- **Spatial Harmonization**: Three different spatial formats unified to H3 hexagons
- **Weighted Aggregation**: Preserves accuracy when converting between resolutions
- **Fast Processing**: Precomputed matrices enable <5 min incremental updates
- **Memory Efficient**: Polars-based pipeline handles large datasets (24M+ hexagons)
- **Reproducible**: All code and data sources publicly available

## Requirements

- Python 3.9+
- 16 GB RAM (32 GB recommended)
- 20 GB disk space
- ~45 minutes for first run (incremental updates: ~5 min)

## Documentation

- **[DOCUMENTATION.md](DOCUMENTATION.md)**: Complete technical documentation
  - Installation and setup
  - Detailed script documentation
  - Configuration options
  - Troubleshooting guide

- **[TECHNICAL_REPORT.md](TECHNICAL_REPORT.md)**: Methodology and analysis
  - Data source specifications
  - Spatial aggregation algorithms
  - Transformation details with statistics
  - Performance metrics
  - Validation results

## Data Sources & Licenses

| Dataset | Source | License |
|---------|--------|---------|
| Population | [Eurostat Census 2021](https://ec.europa.eu/eurostat/web/gisco/geodata/population-distribution/geostat) | CC BY 4.0 |
| Healthcare | [JRC European Commission](https://data.jrc.ec.europa.eu/) | CC BY 4.0 |
| Internet | [Ookla Open Data](https://github.com/teamookla/ookla-open-data) | CC BY-NC-SA 4.0 |

**Project License**: MIT (see code repository)

## Citation

If you use this dataset in your research, please cite:

```
Speed Connection Europe Dataset (2024)
Aggregated European population, healthcare accessibility, and internet speed data
H3 Resolution 8 (~0.74 km² hexagons)
https://github.com/[username]/speed_connection_europe
```

## Technical Highlights

### Spatial Aggregation

- **Grid → H3**: Geometric intersection weights (23.4M mappings)
- **Raster → H3**: Pixel-level aggregation (238M pixels → 5.3M hexagons)
- **Quadkey → H3**: Tile intersection weights (75.4M mappings)

### Performance

| Stage | Time | Output |
|-------|------|--------|
| Matrices | ~50s | 1.3 GB |
| Population ETL | ~40s | 585 MB |
| Health ETL | ~8 min | 297 MB |
| Internet ETL | ~30 min* | ~500 MB |
| Merge | ~1 min | 250 MB |

*First run; ~5 min if quarters cached

### Coverage

- **Final dataset**: 3.8M hexagons
- **Geographic extent**: Europe (EU27 + EFTA + UK + candidates)
- **Population coverage**: 455M people (94.7% of census data)
- **Internet coverage**: 79% of populated areas have data

## Troubleshooting

**"Matrix file not found"**
```bash
python3 matrix/grid_h3_matrix.py
python3 matrix/quadkey_h3_matrix.py
```

**"Out of memory" during internet ETL**
- Increase available RAM
- Close other applications
- Process fewer quarters (edit `QUARTERS` in `etl_internet.py`)

**Slow performance**
- Use SSD storage
- Check `MAX_WORKERS` utilizes all CPU cores
- Ensure virtual environment is activated

See [DOCUMENTATION.md](DOCUMENTATION.md) for detailed troubleshooting.

## Contributing

Contributions welcome! Please open an issue or pull request.

Areas for contribution:
- Additional data sources (income, education, environment)
- Performance optimizations
- Data quality improvements
- Visualization tools
- Analysis examples

## Contact

For questions or support, please open an issue on GitHub.

---

**Version**: 1.0.0  
**Last Updated**: January 2026  
**Status**: Production
