#!/bin/bash
#SBATCH --job-name=innovation_reg
#SBATCH --output=/users/amarche4/scratch/innovation_reg_%j.log
#SBATCH --time=4:00:00
#SBATCH --mem=128G
#SBATCH --cpus-per-task=2
#SBATCH -p batch

module load anaconda3/2023.09-0-aqbc

python /users/amarche4/scratch/build_innovation_reg_data.py
