#!/bin/bash
# Pass 2: per store x module cell, barcodes offered, new nationally, and count-weighted price change pieces for t-1 -> t
# (build_store_new_options_rms.py), one year t per array task; needs pass 1 for t-1 and t.
#SBATCH --job-name=npp_store_opt
#SBATCH --output=/users/amarche4/scratch/new_products_places/npp_store_opt_%A_%a.log
#SBATCH --time=1:30:00
#SBATCH --mem=24G
#SBATCH --cpus-per-task=1
#SBATCH -p batch

module load anaconda3/2023.09-0-aqbc
/usr/bin/time -v python /users/amarche4/scratch/new_products_places/build_store_new_options_rms.py $SLURM_ARRAY_TASK_ID 2>&1 | grep -E "^[0-9]{2}:|^ *cell|^ *[0-9]+ |Maximum resident|Elapsed|Error|error|Traceback|File"
