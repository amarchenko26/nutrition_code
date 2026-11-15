#!/bin/bash
#SBATCH --job-name=npp_fy_all
#SBATCH --output=/users/amarche4/scratch/new_products_places/npp_fy_all_%j.log
#SBATCH --time=0:30:00
#SBATCH --mem=12G
#SBATCH --cpus-per-task=1
#SBATCH -p batch

module load anaconda3/2023.09-0-aqbc
python /users/amarche4/scratch/new_products_places/build_upc_first_year_all_rms.py
