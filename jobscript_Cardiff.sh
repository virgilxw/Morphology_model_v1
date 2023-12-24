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
#SBATCH --job-name=Cardiff_morph
#
# Output file
#SBATCH --output=slurm-%j.out
#======================================================

module purge
module load miniconda/3.11.4
module load nvidia/sdk/23.3

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


papermill 1_downloading_data.ipynb output/Cardiff_1_downloading_data.ipynb -p local_crs 4326 -p place Cardiff -p lat 51.454514 -p lng -2.58791 -p country UK -p crs 4326 -p radius 27700

papermill 2_tessellation.ipynb output/Cardiff_2_tessellation.ipynb -p local_crs 4326 -p place Cardiff -p lat 51.454514 -p lng -2.58791 -p country UK -p crs 4326 -p radius 27700

papermill 3_supercomp_morph.ipynb output/Cardiff_3_supercomp_morph.ipynb -p local_crs 4326 -p place Cardiff -p lat 51.454514 -p lng -2.58791 -p country UK -p crs 4326 -p radius 27700

papermill 4_aggregation.ipynb output/Cardiff_4_aggregation.ipynb -p local_crs 4326 -p place Cardiff -p lat 51.454514 -p lng -2.58791 -p country UK -p crs 4326 -p radius 27700

papermill 5_clustering.ipynb output/Cardiff_5_clustering.ipynb -p local_crs 4326 -p place Cardiff -p lat 51.454514 -p lng -2.58791 -p country UK -p crs 4326 -p radius 27700

papermill 6_clustering_prep.ipynb output/Cardiff_6_clustering_prep.ipynb -p local_crs 4326 -p place Cardiff -p lat 51.454514 -p lng -2.58791 -p country UK -p crs 4326 -p radius 27700

#======================================================
# Epilogue script to record job endtime and runtime
# Do not change the line below
#======================================================
/opt/software/scripts/job_epilogue.sh 
#------------------------------------------------------
