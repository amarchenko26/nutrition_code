#!/bin/bash
# Barcode x county files of every RMS store (build_upc_fips_spending_rms.py), one year per array task.
# Before: copy build_upc_fips_spending_rms.py to /users/amarche4/scratch/ and store_year_fips.parquet to
# /users/amarche4/data/rms_variety/, and run analyze/what_moves/run_store_upc_year_rms.sh for every year (2006-2020).
# Test first:  sbatch --array=2010 --export=ALL,MODE=check run_upc_fips_spending_rms.sh   (prints the comparison, writes nothing)
#SBATCH --job-name=upc_fips
#SBATCH --output=/users/amarche4/scratch/upc_fips_%A_%a.log
#SBATCH --time=1:00:00
#SBATCH --mem=48G
#SBATCH --cpus-per-task=1
#SBATCH -p batch
#SBATCH --array=2006-2020

module load anaconda3/2023.09-0-aqbc
set -o pipefail          # the job fails when python fails, not only when the filter below does
/usr/bin/time -v python /users/amarche4/scratch/build_upc_fips_spending_rms.py $SLURM_ARRAY_TASK_ID $MODE 2>&1 | grep -E "^[0-9]{2}:|Maximum resident|Elapsed|Error|error|Traceback|File|assert|run build"
