#!/bin/bash

#SBATCH --job-name=dedup_sigs
#SBATCH --array=0-99
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=9
#SBATCH --mem-per-cpu=4G
#SBATCH --partition=priority

INPUT=${1:?Usage: sbatch compute_sigs.sh INPUT_DIR SIGS_DIR}
SIGS=${2:?Usage: sbatch compute_sigs.sh INPUT_DIR SIGS_DIR}

LOG_DIR="/mnt/weka/home/romain/logs/dedup_sigs/${SLURM_ARRAY_JOB_ID}"
mkdir -p "${LOG_DIR}"
exec > "${LOG_DIR}/shard_${SLURM_ARRAY_TASK_ID}.out"
exec 2> "${LOG_DIR}/shard_${SLURM_ARRAY_TASK_ID}.err"

echo "Computing signatures for shard ${SLURM_ARRAY_TASK_ID}"
uv run dactory compute-sigs "$INPUT" "$SIGS" --shard "$SLURM_ARRAY_TASK_ID" -w 8
