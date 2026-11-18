#!/bin/bash
# Oscar runner for 0_collapse_rms_national.py (all years in one 1-core job: about 10 s and 3 GB a year).
# Copy the script to /users/amarche4/scratch/jaravel_food/ first: mkdir -p /users/amarche4/scratch/jaravel_food
#SBATCH --job-name=jf_collapse
#SBATCH --output=/users/amarche4/scratch/jaravel_food/collapse_%j.log
#SBATCH --time=0:20:00
#SBATCH --mem=6G
#SBATCH --cpus-per-task=1
#SBATCH -p batch
module load anaconda3/2023.09-0-aqbc
set -eo pipefail         # the job fails when a year fails
for y in $(seq 2006 2020); do
  python /users/amarche4/scratch/jaravel_food/0_collapse_rms_national.py $y | tail -1
done
