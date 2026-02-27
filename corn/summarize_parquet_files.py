"""
Summarize parquet files in the Nielsen purchases dataset
"""

import os
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

def format_size(size_bytes):
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"

def summarize_parquet_dataset(base_path):
    """
    Summarize all parquet files in a partitioned dataset
    """
    print("=" * 80)
    print("NIELSEN PARQUET DATASET SUMMARY")
    print("=" * 80)
    print(f"\nBase path: {base_path}\n")

    base = Path(base_path)

    if not base.exists():
        print(f"ERROR: Path does not exist: {base_path}")
        return

    # Find all partition directories
    partitions = sorted([d for d in base.iterdir() if d.is_dir() and d.name.startswith('panel_year=')])

    if not partitions:
        print("No partition directories found")
        return

    print(f"Found {len(partitions)} year partitions\n")
    print("-" * 80)

    total_size = 0
    total_rows = 0
    all_columns = None

    year_summaries = []

    for partition_dir in partitions:
        year = partition_dir.name.replace('panel_year=', '')

        # Find parquet files in this partition
        parquet_files = list(partition_dir.glob('*.parquet'))

        if not parquet_files:
            print(f"Year {year}: No parquet files found")
            continue

        partition_size = 0
        partition_rows = 0
        columns = None
        dtypes = None

        for pq_file in parquet_files:
            # Get file size
            file_size = pq_file.stat().st_size
            partition_size += file_size

            # Read parquet metadata without loading data
            pq_metadata = pq.read_metadata(pq_file)
            partition_rows += pq_metadata.num_rows

            # Get schema info (only need to do once per partition)
            if columns is None:
                schema = pq.read_schema(pq_file)
                columns = schema.names
                dtypes = {name: str(schema.field(name).type) for name in columns}

        total_size += partition_size
        total_rows += partition_rows

        if all_columns is None:
            all_columns = columns

        year_summaries.append({
            'year': year,
            'num_files': len(parquet_files),
            'rows': partition_rows,
            'size_bytes': partition_size,
            'columns': columns,
            'dtypes': dtypes
        })

        print(f"Year {year}:")
        print(f"  Files: {len(parquet_files)}")
        print(f"  Rows:  {partition_rows:,}")
        print(f"  Size:  {format_size(partition_size)}")
        print()

    # Print overall summary
    print("=" * 80)
    print("OVERALL SUMMARY")
    print("=" * 80)
    print(f"Total partitions: {len(year_summaries)}")
    print(f"Total rows:       {total_rows:,}")
    print(f"Total size:       {format_size(total_size)}")
    print(f"Years covered:    {year_summaries[0]['year']} - {year_summaries[-1]['year']}")

    # Print column info (from first partition)
    if year_summaries:
        print(f"\nColumns ({len(year_summaries[0]['columns'])} total):")
        print("-" * 40)
        for col_name, dtype in year_summaries[0]['dtypes'].items():
            print(f"  {col_name:<30} {dtype}")

    # Check for schema consistency across years
    print("\n" + "=" * 80)
    print("SCHEMA CONSISTENCY CHECK")
    print("=" * 80)

    reference_cols = set(year_summaries[0]['columns'])
    all_consistent = True

    for summary in year_summaries[1:]:
        current_cols = set(summary['columns'])
        if current_cols != reference_cols:
            all_consistent = False
            missing = reference_cols - current_cols
            extra = current_cols - reference_cols
            print(f"Year {summary['year']} differs from {year_summaries[0]['year']}:")
            if missing:
                print(f"  Missing columns: {missing}")
            if extra:
                print(f"  Extra columns: {extra}")

    if all_consistent:
        print("All years have consistent schemas")

    return year_summaries


if __name__ == "__main__":
    data_path = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_all_years_food_only'
    summarize_parquet_dataset(data_path)
