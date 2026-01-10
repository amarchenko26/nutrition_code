#!/usr/bin/env python3
"""
USDA FoodData Explorer
Explores USDA FoodData zip files to identify product files
"""

import os
import zipfile
from pathlib import Path


def explore_usda_zips(base_path):
    """
    Explore USDA FoodData zip files and find product-related files

    Parameters:
    -----------
    base_path : str
        Path to directory containing USDA zip files
    """
    print("="*80)
    print("USDA FOODDATA EXPLORER")
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
    zips_with_product_files = []
    all_product_files = []

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

                # Look for product-related files
                # Common patterns: product, branded, food
                product_patterns = ['product', 'branded', 'food']

                found_product_files = []
                for filename in file_list:
                    # Extract just the filename without path
                    base_filename = os.path.basename(filename).lower()

                    # Check if it's a CSV file with product-related keywords
                    if base_filename.endswith('.csv'):
                        for pattern in product_patterns:
                            if pattern in base_filename:
                                found_product_files.append(filename)
                                break

                if found_product_files:
                    print(f"\n✓ Found {len(found_product_files)} product-related file(s):")
                    for pf in found_product_files:
                        # Get file size
                        file_info = zf.getinfo(pf)
                        file_size_mb = file_info.file_size / 1024 / 1024
                        print(f"  - {pf}")
                        print(f"    Size: {file_size_mb:.2f} MB")

                    zips_with_product_files.append(zip_path.name)
                    all_product_files.extend([(zip_path.name, pf) for pf in found_product_files])
                else:
                    print("\n✗ No product-related files found")

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
    print(f"Zip files with product files: {len(zips_with_product_files)}")
    print(f"Total product files found: {len(all_product_files)}")

    if zips_with_product_files:
        print(f"\n\nZip files containing product data:")
        for i, zip_name in enumerate(zips_with_product_files, 1):
            print(f"  {i}. {zip_name}")

        print(f"\n\nDetailed product file list:")
        for zip_name, product_file in all_product_files:
            print(f"  {zip_name} -> {product_file}")
    else:
        print("\nNo product files found in any zip archives.")
        print("\nHint: Try examining the file list above to identify the correct pattern.")


if __name__ == "__main__":
    base_path = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/usda'
    explore_usda_zips(base_path)
