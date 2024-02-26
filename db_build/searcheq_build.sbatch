#!/bin/bash
#SBATCH --job-name=dbnascent_build  # Job name
#SBATCH --mail-type=NONE # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=lynn.sanford@colorado.edu # Where to send mail
#SBATCH --nodes=1
#SBATCH --ntasks=1 # Number of CPU (processer cores i.e. tasks)
#SBATCH --time=01:00:00 # Time limit hrs:min:sec
#SBATCH -p short
#SBATCH --mem=8gb # Memory limit
#SBATCH --output=/Shares/dbnascent/DBNascent-build/outerr/searcheq_build.%j.out
#SBATCH --error=/Shares/dbnascent/DBNascent-build/outerr/searcheq_build.%j.err

#################################################################
module load python/3.6.3

################## JOB INFO #####################################

printf "\nDirectory: $INDIR"
printf "\nRun on: $(hostname)"
printf "\nRun from: $(pwd)"
printf "\nScript: $0\n"

printf "\nYou've requested $SLURM_CPUS_ON_NODE core(s).\n"

# Run scripts
python3 ./searcheq_build.py
