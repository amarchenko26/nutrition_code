#!/bin/bash
# Pass 1: store x barcode units and spending per food module (build_store_upc_year_rms.py), one year per array task.
# Submit with --array=<years>, e.g. sbatch --array=2009,2010,2014,2015,2018,2019 run_store_upc_year.sh
#SBATCH --job-name=npp_store_upc
#SBATCH --output=/users/amarche4/scratch/new_products_places/npp_store_upc_%A_%a.log
#SBATCH --time=4:00:00
#SBATCH --mem=24G
#SBATCH --cpus-per-task=2
#SBATCH -p batch

module load anaconda3/2023.09-0-aqbc
/usr/bin/time -v python /users/amarche4/scratch/new_products_places/build_store_upc_year_rms.py $SLURM_ARRAY_TASK_ID 2>&1 | grep -E "^[0-9]{2}:|Maximum resident|Elapsed|Error|error|Traceback|File"
