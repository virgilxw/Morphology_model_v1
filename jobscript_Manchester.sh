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
#SBATCH --job-name=Manchester_morph
#
# Output file
#SBATCH --output=slurm-%j.out
#======================================================

module purge
module load miniconda/3.11.4

export LD_LIBRARY_PATH=/users/wjb22189/.conda/envs/momepy/lib:$LD_LIBRARY_PATH

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

papermill 4_aggregation.ipynb output/Manchester_4_aggregation.ipynb -p local_crs 4326 -p place Manchester -p lat 53.483959 -p lng -2.244644 -p country UK -p crs 4326

#======================================================
# Epilogue script to record job endtime and runtime
# Do not change the line below
#======================================================
/opt/software/scripts/job_epilogue.sh 
#------------------------------------------------------