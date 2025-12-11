#!/bin/bash
#SBATCH --job-name=fill_locs
#SBATCH --output=Logs/fill_locs_%A_%a.out
#SBATCH --error=Logs/fill_locs_%A_%a.err
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --array=0-39

cd /path/to/your/project

source .venv/bin/activate

export CHUNK_SIZE=5

python fill_locations_hpc.py