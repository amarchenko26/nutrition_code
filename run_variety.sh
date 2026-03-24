#!/bin/bash
#SBATCH --job-name=rms_variety
#SBATCH --output=/users/amarche4/scratch/rms_variety_%j.log
#SBATCH --time=24:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=2
#SBATCH -p batch

module load anaconda3/2023.09-0-aqbc

cd /users/amarche4/scratch
python /users/amarche4/scratch/build_product_variety.py
