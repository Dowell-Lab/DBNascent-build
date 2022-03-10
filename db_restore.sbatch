#!/bin/bash
#SBATCH --job-name=dbnascent_restore  # Job name
#SBATCH --mail-type=NONE # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=lynn.sanford@colorado.edu # Where to send mail
#SBATCH --nodes=1
#SBATCH --ntasks=1 # Number of CPU (processer cores i.e. tasks)
#SBATCH --time=02:00:00 # Time limit hrs:min:sec
#SBATCH -p short
#SBATCH --mem=8gb # Memory limit
#SBATCH --output=/scratch/Shares/dowell/dbnascent/DBNascent-build/outerr/dbnascent_restore.%j.out
#SBATCH --error=/scratch/Shares/dowell/dbnascent/DBNascent-build/outerr/dbnascent_restore.%j.err

#################################################################
module load python/3.6.3

################## JOB INFO #####################################

printf "\nDirectory: $INDIR"
printf "\nRun on: $(hostname)"
printf "\nRun from: $(pwd)"
printf "\nScript: $0\n"

printf "\nYou've requested $SLURM_CPUS_ON_NODE core(s).\n"

# Run scripts
python3 ./db_restore.py
