#!/bin/bash
# County price levels of healthy and unhealthy groceries from RMS (build_local_price_index_rms.py), one year per array task.
# Copy build_local_price_index_rms.py to /users/amarche4/scratch/ and rms_upc_groups.parquet to /users/amarche4/data/rms_variety/ first.
#SBATCH --job-name=rms_local_price
#SBATCH --output=/users/amarche4/scratch/rms_local_price_%A_%a.log
#SBATCH --time=1:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=1
#SBATCH -p batch
#SBATCH --array=2006-2020

module load anaconda3/2023.09-0-aqbc

python /users/amarche4/scratch/build_local_price_index_rms.py $SLURM_ARRAY_TASK_ID
