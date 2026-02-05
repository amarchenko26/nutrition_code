#!/usr/bin/env python3
"""
Create Sample Dataset

Creates a sample of the Nielsen purchases data by selecting a random subset of
households and following them across all years. This allows for faster iteration
during development while maintaining the same data structure.

Usage:
    python create_sample.py

The output has identical structure to the full dataset (partitioned by panel_year),
so downstream code can switch between sample and full data by changing only the path.
"""

import os
import pandas as pd
import numpy as np
from glob import glob
import shutil

# ============================================================================
# CONFIGURATION
# ============================================================================
# Number of households to sample
N_HOUSEHOLDS = 1000

# Random seed for reproducibility
RANDOM_SEED = 42

# Input path (full dataset)
INPUT_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_food'

# Output path (sample dataset)
OUTPUT_PATH = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/purchases_food_sample'


def get_all_households(input_path):
    """
    Get all unique household_code values across all years.

    Parameters:
    -----------
    input_path : str
        Path to the partitioned parquet dataset

    Returns:
    --------
    set: All unique household codes
    """
    print("Collecting unique household codes across all years...")

    year_dirs = sorted(glob(os.path.join(input_path, 'panel_year=*')))

    if not year_dirs:
        raise ValueError(f"No year partitions found in {input_path}")

    all_households = set()

    for year_dir in year_dirs:
        year = os.path.basename(year_dir).replace('panel_year=', '')

        # Read only the household_code column for efficiency
        df = pd.read_parquet(year_dir, columns=['household_code'])
        households = set(df['household_code'].unique())

        print(f"  Year {year}: {len(households):,} unique households")
        all_households.update(households)

        del df

    print(f"\nTotal unique households across all years: {len(all_households):,}")
    return all_households


def sample_households(all_households, n_households=N_HOUSEHOLDS, seed=RANDOM_SEED):
    """
    Randomly sample household codes.

    Parameters:
    -----------
    all_households : set
        All unique household codes
    n_households : int
        Number of households to sample
    seed : int
        Random seed for reproducibility

    Returns:
    --------
    set: Sampled household codes
    """
    np.random.seed(seed)

    households_list = sorted(list(all_households))  # Sort for reproducibility

    if n_households > len(households_list):
        print(f"Warning: Requested {n_households} households but only {len(households_list)} available")
        n_households = len(households_list)

    sampled = set(np.random.choice(households_list, size=n_households, replace=False))

    print(f"Sampled {len(sampled):,} households (seed={seed})")
    return sampled


def create_sample_dataset(input_path, output_path, sampled_households):
    """
    Create sample dataset by filtering to sampled households.

    Parameters:
    -----------
    input_path : str
        Path to full dataset
    output_path : str
        Path for sample dataset
    sampled_households : set
        Household codes to include
    """
    print(f"\nCreating sample dataset at: {output_path}")

    # Remove existing output directory if it exists
    if os.path.exists(output_path):
        print(f"Removing existing sample directory...")
        shutil.rmtree(output_path)

    os.makedirs(output_path, exist_ok=True)

    year_dirs = sorted(glob(os.path.join(input_path, 'panel_year=*')))

    total_rows_full = 0
    total_rows_sample = 0

    for year_dir in year_dirs:
        year = int(os.path.basename(year_dir).replace('panel_year=', ''))

        # Read full year data
        df = pd.read_parquet(year_dir)
        n_full = len(df)
        total_rows_full += n_full

        # Filter to sampled households
        df_sample = df[df['household_code'].isin(sampled_households)]
        n_sample = len(df_sample)
        total_rows_sample += n_sample

        # Count unique households in this year's sample
        n_hh_sample = df_sample['household_code'].nunique()

        print(f"  Year {year}: {n_full:,} -> {n_sample:,} rows ({n_sample/n_full*100:.1f}%), {n_hh_sample:,} households")

        if n_sample > 0:
            # Add panel_year column for partitioning (it gets stripped when reading partitioned data)
            df_sample['panel_year'] = year

            # Write to output with same partition structure
            df_sample.to_parquet(
                output_path,
                partition_cols=['panel_year'],
                engine='pyarrow',
                compression='snappy',
                index=False
            )

        del df, df_sample

    print(f"\n" + "=" * 60)
    print("SAMPLE CREATION COMPLETE")
    print("=" * 60)
    print(f"Full dataset rows: {total_rows_full:,}")
    print(f"Sample dataset rows: {total_rows_sample:,}")
    print(f"Reduction: {(1 - total_rows_sample/total_rows_full)*100:.1f}%")
    print(f"Output: {output_path}")

    return total_rows_sample


def save_sampled_households(sampled_households, output_path):
    """
    Save the list of sampled household codes for reference.
    """
    hh_file = os.path.join(output_path, 'sampled_households.csv')
    pd.DataFrame({'household_code': sorted(list(sampled_households))}).to_csv(hh_file, index=False)
    print(f"Saved sampled household codes to: {hh_file}")


def main():
    print("=" * 60)
    print("CREATING SAMPLE DATASET")
    print("=" * 60)
    print(f"Input: {INPUT_PATH}")
    print(f"Output: {OUTPUT_PATH}")
    print(f"Sample size: {N_HOUSEHOLDS:,} households")
    print(f"Random seed: {RANDOM_SEED}")
    print()

    # Step 1: Get all unique households
    all_households = get_all_households(INPUT_PATH)

    # Step 2: Sample households
    sampled_households = sample_households(all_households, N_HOUSEHOLDS, RANDOM_SEED)

    # Step 3: Create sample dataset
    create_sample_dataset(INPUT_PATH, OUTPUT_PATH, sampled_households)

    # Step 4: Save the list of sampled households for reference
    save_sampled_households(sampled_households, OUTPUT_PATH)

    print("\nTo use the sample in downstream code, change the data path:")
    print(f"  Full:   '{INPUT_PATH}'")
    print(f"  Sample: '{OUTPUT_PATH}'")


if __name__ == "__main__":
    main()
