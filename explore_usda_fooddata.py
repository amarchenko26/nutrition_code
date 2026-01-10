#!/usr/bin/env python3
"""
USDA FoodData Explorer
Explores USDA FoodData zip files to find branded_food.csv files and print their columns
"""

import os
import zipfile
from pathlib import Path
import pandas as pd
from io import BytesIO


def explore_usda_zips(base_path):
    """
    Explore USDA FoodData zip files and find branded_food.csv files

    Parameters:
    -----------
    base_path : str
        Path to directory containing USDA zip files
    """
    print("="*80)
    print("USDA FOODDATA BRANDED_FOOD.CSV EXPLORER")
    print("="*80)

    if not os.path.exists(base_path):
        print(f"ERROR: Directory not found: {base_path}")
        return

    # Find all .zip files in the directory
    zip_files = sorted(Path(base_path).glob("*.zip"))

    if not zip_files:
        print(f"No .zip files found in {base_path}")
        return

    print(f"\nFound {len(zip_files)} zip file(s) in directory:")
    print(f"Directory: {base_path}\n")

    # Track results
    branded_food_files = []

    # Explore each zip file
    for zip_path in zip_files:
        print(f"\n{'='*80}")
        print(f"Exploring: {zip_path.name}")
        print(f"Size: {zip_path.stat().st_size / 1024 / 1024:.2f} MB")
        print("="*80)

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Get all file names in the zip
                file_list = zf.namelist()
                print(f"Total files in archive: {len(file_list)}")

                # Special case for BFPD_csv_07132018.zip
                if zip_path.name == "BFPD_csv_07132018.zip":
                    target_filename = "Products.csv"
                else:
                    target_filename = "branded_food.csv"

                # Look for the branded_food file
                found_file = None
                for filename in file_list:
                    base_filename = os.path.basename(filename)
                    if base_filename.lower() == target_filename.lower():
                        found_file = filename
                        break

                if found_file:
                    print(f"\n✓ Found: {found_file}")

                    # Get file size
                    file_info = zf.getinfo(found_file)
                    file_size_mb = file_info.file_size / 1024 / 1024
                    print(f"  Size: {file_size_mb:.2f} MB")

                    # Read CSV headers without extracting
                    try:
                        with zf.open(found_file) as csv_file:
                            df_sample = pd.read_csv(BytesIO(csv_file.read()), nrows=0)
                            columns = df_sample.columns.tolist()

                            print(f"\n  Columns ({len(columns)} total):")
                            for i, col in enumerate(columns, 1):
                                print(f"    {i:2d}. {col}")

                            branded_food_files.append((zip_path.name, found_file, columns))
                    except Exception as e:
                        print(f"  ERROR reading CSV: {str(e)}")
                else:
                    print(f"\n✗ File '{target_filename}' not found")

                # Show first few files for context
                print(f"\nFirst 10 files in archive:")
                for f in file_list[:10]:
                    print(f"  - {f}")
                if len(file_list) > 10:
                    print(f"  ... and {len(file_list) - 10} more files")

        except zipfile.BadZipFile:
            print(f"ERROR: {zip_path.name} is not a valid zip file")
        except Exception as e:
            print(f"ERROR processing {zip_path.name}: {str(e)}")

    # Summary
    print("\n\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nTotal zip files explored: {len(zip_files)}")
    print(f"Zip files with branded_food.csv: {len(branded_food_files)}")

    if branded_food_files:
        print(f"\n\nBranded Food Files Found:")
        for i, (zip_name, csv_path, columns) in enumerate(branded_food_files, 1):
            print(f"\n{i}. {zip_name}")
            print(f"   CSV File: {csv_path}")
            print(f"   Columns ({len(columns)} total):")
            for j, col in enumerate(columns, 1):
                print(f"     {j:2d}. {col}")

        # Combine all branded food files
        print("\n\n" + "="*80)
        print("COMBINING ALL BRANDED FOOD FILES")
        print("="*80)

        all_dfs = []

        for i, (zip_name, csv_path, columns) in enumerate(branded_food_files, 1):
            print(f"\n{i}. Loading {zip_name}...")
            zip_path = next(zf for zf in zip_files if zf.name == zip_name)

            with zipfile.ZipFile(zip_path, 'r') as zf:
                with zf.open(csv_path) as csv_file:
                    df = pd.read_csv(BytesIO(csv_file.read()), low_memory=False)
                    print(f"   Rows: {len(df):,}")
                    all_dfs.append(df)

        # Concatenate all dataframes
        print(f"\nCombining all {len(all_dfs)} files...")
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f"Total rows after combining: {len(combined_df):,}")

        # Check for duplicates based on gtin_upc
        print("\n" + "="*80)
        print("HANDLING DUPLICATES")
        print("="*80)

        # Find duplicates
        duplicated_mask = combined_df.duplicated(subset=['gtin_upc'], keep='last')
        n_duplicates = duplicated_mask.sum()

        print(f"\nDuplicate rows found (based on gtin_upc): {n_duplicates:,}")

        if n_duplicates > 0:
            # Get sample of duplicated rows for inspection
            duplicate_gtins = combined_df[duplicated_mask]['gtin_upc'].head(10).tolist()

            print(f"\nShowing 10 example duplicate rows:")
            print("(For each gtin_upc, showing the FIRST occurrence and the LAST/KEPT occurrence)")
            print("-" * 80)

            for gtin in duplicate_gtins:
                matching_rows = combined_df[combined_df['gtin_upc'] == gtin]
                print(f"\ngtin_upc: {gtin} (appears {len(matching_rows)} times)")
                # Show first and last occurrence
                first_last = pd.concat([matching_rows.head(1), matching_rows.tail(1)])
                print(first_last.to_string())
                print("-" * 80)

            # Remove duplicates, keeping LAST (most recent) occurrence
            combined_df_deduped = combined_df.drop_duplicates(subset=['gtin_upc'], keep='last')
            print(f"\nRows after deduplication: {len(combined_df_deduped):,}")
            print(f"Rows removed: {n_duplicates:,}")
            print(f"Reduction: {n_duplicates/len(combined_df)*100:.1f}%")

            return combined_df_deduped
        else:
            print("\nNo duplicates found!")
            return combined_df
    else:
        print("\nNo branded_food.csv files found in any zip archives.")
        print("\nHint: Try examining the file list above to identify the correct pattern.")
        return None


if __name__ == "__main__":
    base_path = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/usda'
    result_df = explore_usda_zips(base_path)

    if result_df is not None:
        print("\n\n" + "="*80)
        print("FINAL DATASET INFO")
        print("="*80)
        print(f"Total rows: {len(result_df):,}")
        print(f"Total columns: {len(result_df.columns)}")
        print(f"\nColumns: {result_df.columns.tolist()}")
        print(f"\nFirst few rows:")
        print(result_df.head(10))

        # Export to CSV
        print("\n\n" + "="*80)
        print("EXPORTING TO CSV")
        print("="*80)

        output_dir = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/usda'
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, 'usda_branded_food_deduped.csv')
        print(f"\nSaving to: {output_path}")

        result_df.to_csv(output_path, index=False)

        # Get file size
        file_size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f"✓ File saved successfully!")
        print(f"  File size: {file_size_mb:.2f} MB")
        print(f"  Rows: {len(result_df):,}")
        print(f"  Columns: {len(result_df.columns)}")
