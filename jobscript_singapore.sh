#!/bin/bash

#======================================================
#
# Job script for running GROMACS on multiple cores on a single node
# Adapted to activate momepy conda environment and run Jupyter notebook
#
#======================================================

#======================================================
# Propagate environment variables to the compute node
#SBATCH --export=ALL
#
# Run in the standard partition (queue)
#SBATCH --partition=standard
#
# Specify project account
#SBATCH --account=porta-umm
#
# No. of tasks required (max of 40), all cores on the same node
#SBATCH --ntasks=1 --cpus-per-task=16
#
# Specify (hard) runtime (HH:MM:SS)
#SBATCH --time=08:00:00
#
# Job name
#SBATCH --job-name=morph
#
# Output file
#SBATCH --output=slurm-%j.out
#======================================================

module purge
module load miniconda/3.11.4

# Activate the momepy conda environment
conda activate momepy

# Set the number of threads
export OMP_NUM_THREADS=16

#======================================================
# Prologue script to record job details
# Do not change the line below
#======================================================
/opt/software/scripts/job_prologue.sh  
#------------------------------------------------------

# Run the Jupyter notebook
papermill 2_tessellation.ipynb output/singapore_2_tessellation-executed.ipynb -p local_crs 3414 -p place singapore -p lat 1.28795311 -p lng 103.851784 -p crs 4326

papermill 4_supercomp_morph.ipynb output/singapore_4_supercomp_morph.ipynb -p local_crs 3414 -p place singapore -p lat 1.28795311 -p lng 103.851784 -p crs 4326

#======================================================
# Epilogue script to record job endtime and runtime
# Do not change the line below
#======================================================
/opt/software/scripts/job_epilogue.sh 
#------------------------------------------------------