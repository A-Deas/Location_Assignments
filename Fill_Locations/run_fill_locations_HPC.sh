#!/bin/bash
#SBATCH --job-name=fill_locs
#SBATCH --output=logs/fill_locs_%A_%a.out
#SBATCH --error=logs/fill_locs_%A_%a.err
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --array=0-39   # <-- adjust this based on #files and CHUNK_SIZE

# Activate your env
module load python/3.10  # or whatever your cluster uses
source ~/miniforge3/etc/profile.d/conda.sh
conda activate utah_abm

# Optional: set chunk size here instead of hardcoding in Python
export CHUNK_SIZE=5

python fill_locations_hpc.py
