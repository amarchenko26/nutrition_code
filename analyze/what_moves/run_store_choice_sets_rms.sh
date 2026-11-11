#!/bin/bash
# What each RMS store offers (build_store_choice_sets_rms.py), one year per array task. A test: sbatch --array=2015 --export=ALL,N_MODULES=10 ...
# Copy build_store_choice_sets_rms.py to /users/amarche4/scratch/ and rms_upc_groups.parquet to /users/amarche4/data/rms_variety/ first.
#SBATCH --job-name=store_choice
#SBATCH --output=/users/amarche4/scratch/store_choice_%A_%a.log
#SBATCH --time=8:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=4
#SBATCH -p batch
#SBATCH --array=2006-2020

module load anaconda3/2023.09-0-aqbc

python /users/amarche4/scratch/build_store_choice_sets_rms.py $SLURM_ARRAY_TASK_ID $N_MODULES
