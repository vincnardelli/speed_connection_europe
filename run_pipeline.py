#!/usr/bin/env python3
"""
Pipeline Orchestrator - Run the complete ETL pipeline.
Executes all steps in order: matrices -> ETL -> merge
"""

import sys
import subprocess
from pathlib import Path
import time


def run_script(script_path: str, description: str) -> bool:
    """Run a Python script and return success status."""
    print("\n" + "=" * 80)
    print(f"RUNNING: {description}")
    print("=" * 80)
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=False,
            text=True
        )
        
        elapsed = time.time() - start_time
        print(f"\n✓ {description} completed in {elapsed:.1f}s")
        return True
        
    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        print(f"\n✗ {description} failed after {elapsed:.1f}s")
        print(f"Error code: {e.returncode}")
        return False


def check_file_exists(file_path: str) -> bool:
    """Check if a file exists."""
    return Path(file_path).exists()


def main():
    overall_start = time.time()
    
    print("=" * 80)
    print("SPEED CONNECTION EUROPE - COMPLETE PIPELINE")
    print("=" * 80)
    print("\nThis script will run the complete ETL pipeline:")
    print("1. Generate weight matrices (grid and quadkey)")
    print("2. Process population data")
    print("3. Process health data")
    print("4. Process internet data")
    print("5. Merge all datasets")
    print()
    
    # Step 1: Grid Matrix
    if not run_script("matrix/grid_h3_matrix.py", "Grid -> H3 Weight Matrix"):
        print("\n✗ Pipeline failed at grid matrix generation")
        sys.exit(1)
    
    # Step 2: Quadkey Matrix
    if not run_script("matrix/quadkey_h3_matrix.py", "Quadkey -> H3 Weight Matrix"):
        print("\n✗ Pipeline failed at quadkey matrix generation")
        sys.exit(1)
    
    # Step 3: ETL Population
    if not run_script("etl_population.py", "ETL Population"):
        print("\n✗ Pipeline failed at population ETL")
        sys.exit(1)
    
    # Step 4: ETL Health
    if not run_script("etl_health.py", "ETL Health"):
        print("\n✗ Pipeline failed at health ETL")
        sys.exit(1)
    
    # Step 5: ETL Internet
    if not run_script("etl_internet.py", "ETL Internet"):
        print("\n✗ Pipeline failed at internet ETL")
        sys.exit(1)
    
    # Step 6: Merge Datasets
    if not run_script("merge_datasets.py", "Merge Datasets"):
        print("\n✗ Pipeline failed at merge stage")
        sys.exit(1)
    
    # Final summary
    overall_elapsed = time.time() - overall_start
    
    print("\n" + "=" * 80)
    print("✓ PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print(f"Total time: {overall_elapsed:.1f}s ({overall_elapsed/60:.1f} minutes)")
    
    # Check output file
    output_file = "data/data_h3_res8.parquet"
    if check_file_exists(output_file):
        size_mb = Path(output_file).stat().st_size / 1024**2
        print(f"\nOutput file: {output_file}")
        print(f"Size: {size_mb:.1f} MB")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
