#!/bin/bash
#SBATCH --job-name=upc_first_year_county
#SBATCH --output=/users/amarche4/scratch/upc_first_year_county_%j.log
#SBATCH --time=2:00:00
#SBATCH --mem=128G
#SBATCH --cpus-per-task=2
#SBATCH -p batch

module load anaconda3/2023.09-0-aqbc

python /users/amarche4/scratch/build_upc_first_year_county.py
