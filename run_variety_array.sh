#!/bin/bash
#SBATCH --job-name=rms_variety_year
#SBATCH --output=/users/amarche4/scratch/rms_variety_%A_%a.log
#SBATCH --time=24:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=2
#SBATCH -p batch
#SBATCH --array=2006-2020

module load anaconda3/2023.09-0-aqbc

python /users/amarche4/scratch/build_product_variety.py --year $SLURM_ARRAY_TASK_ID
