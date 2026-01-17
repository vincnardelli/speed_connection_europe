#!/usr/bin/env python3
"""
ETL Internet Speed Data - Download Ookla speed test data from S3
"""

import boto3
from botocore.config import Config
from botocore import UNSIGNED
from pathlib import Path

# All quarters from 2019 Q1 to 2025 Q3
QUARTERS = [
    (2019, 1), (2019, 2), (2019, 3), (2019, 4),
    (2020, 1), (2020, 2), (2020, 3), (2020, 4),
    (2021, 1), (2021, 2), (2021, 3), (2021, 4),
    (2022, 1), (2022, 2), (2022, 3), (2022, 4),
    (2023, 1), (2023, 2), (2023, 3), (2023, 4),
    (2024, 1), (2024, 2), (2024, 3), (2024, 4),
    (2025, 1), (2025, 2), (2025, 3)
]

def download_quarter(year, quarter, data_type='fixed', output_dir='data/internet'):
    """Download single quarter from S3"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    quarter_dates = {1: '01-01', 2: '04-01', 3: '07-01', 4: '10-01'}
    date = f"{year}-{quarter_dates[quarter]}"
    
    key = f"parquet/performance/type={data_type}/year={year}/quarter={quarter}/{date}_performance_{data_type}_tiles.parquet"
    local_file = f"{output_dir}/{year}_q{quarter}_{data_type}.parquet"
    
    # Skip if already exists
    if Path(local_file).exists():
        size_mb = Path(local_file).stat().st_size / 1024**2
        print(f"  {year} Q{quarter}: already downloaded ({size_mb:.0f} MB)")
        return local_file
    
    try:
        print(f"  {year} Q{quarter}: downloading...", end=' ', flush=True)
        s3.download_file('ookla-open-data', key, local_file)
        size_mb = Path(local_file).stat().st_size / 1024**2
        print(f"✓ ({size_mb:.0f} MB)")
        return local_file
    except Exception as e:
        print(f"✗ ({e})")
        return None

def main():
    import sys
    data_type = sys.argv[1] if len(sys.argv) > 1 else 'fixed'
    
    print("="*80)
    print(f"ETL INTERNET - DOWNLOADING ALL QUARTERS - {data_type.upper()}")
    print("="*80)
    
    downloaded = 0
    failed = 0
    
    for year, quarter in QUARTERS:
        result = download_quarter(year, quarter, data_type)
        if result:
            downloaded += 1
        else:
            failed += 1
    
    print("\n" + "="*80)
    print(f"✓ Downloaded: {downloaded}")
    print(f"✗ Failed: {failed}")
    print("="*80)

if __name__ == "__main__":
    main()

