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
#SBATCH --time=14:00:00
#
# Job name
#SBATCH --job-name=Cardiff_OS_morph
#
# Output file
#SBATCH --output=Cardiff_OS-slurm-%j.out

#SBATCH --mail-user=reuben.xianwei.wang@strath.ac.uk
#SBATCH --mail-type=TIME_LIMIT,FAIL


#======================================================

module purge
module load miniconda/3.11.4
module load nvidia/sdk/23.3
module load gcc/12.2.0

export LD_LIBRARY_PATH=/users/wjb22189/.conda/envs/momepy/lib:$LD_LIBRARY_PATH

# Activate the momepy conda environment
conda activate momepy

# Set the number of threads
export OMP_NUM_THREADS=16

# Immediately exit if any command has a non-zero exit status.
set -e

#======================================================
# Prologue script to record job details
# Do not change the line below
#======================================================
/opt/software/scripts/job_prologue.sh  
#------------------------------------------------------

dir="../output/Cardiff_OS"

# Check if the directory doesn't exist
if [ ! -d "$dir" ]; then
    # Create the directory
    mkdir -p "$dir"
    echo "Directory created: $dir"
fi

# Run the Jupyter notebook

conda activate downloader
papermill 1_ordanance_survey.ipynb ../output/Cardiff_OS/Cardiff_OS_1_ordanance_survey.ipynb -p local_crs 27700 -p place Cardiff_OS -p lat 51.454514 -p lng -2.58791 -p country UK -p crs 4326 -p radius 20
status=$?

# Check if papermill execution was successful
if [ $status -ne 0 ]; then
    echo "Papermill execution failed with status $status"
    exit $status
fi

#======================================================
# Epilogue script to record job endtime and runtime
# Do not change the line below
#======================================================
/opt/software/scripts/job_epilogue.sh
#------------------------------------------------------
