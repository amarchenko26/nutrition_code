#!/bin/bash
#SBATCH --job-name=rms_variety_combine
#SBATCH --output=/users/amarche4/scratch/rms_variety_combine_%j.log
#SBATCH --time=4:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=2
#SBATCH -p batch

module load anaconda3/2023.09-0-aqbc

python /users/amarche4/scratch/build_product_variety.py
