#!/bin/bash
#SBATCH --job-name=price_index
#SBATCH --output=/users/amarche4/scratch/price_index_%j.log
#SBATCH --time=8:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=2
#SBATCH -p batch

module load anaconda3/2023.09-0-aqbc

python /users/amarche4/scratch/build_price_index.py
