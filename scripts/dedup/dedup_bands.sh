#!/bin/bash

# Usage: bash dedup_bands.sh SIGS_DIR DUPES_DIR [THRESHOLD]
# Computes the number of LSH bands and submits a Slurm array job.

SIGS=${1:?Usage: bash dedup_bands.sh SIGS_DIR DUPES_DIR [THRESHOLD]}
DUPES=${2:?Usage: bash dedup_bands.sh SIGS_DIR DUPES_DIR [THRESHOLD]}
THRESHOLD=${3:-0.75}

NUM_BANDS=$(uv run python -c "from datasketch import MinHashLSH; print(MinHashLSH(threshold=${THRESHOLD}, num_perm=112).b)")
MAX_BAND=$((NUM_BANDS - 1))
echo "Threshold=${THRESHOLD} -> ${NUM_BANDS} bands (array 0-${MAX_BAND})"

TMPSCRIPT=$(mktemp /tmp/dedup_bands_XXXXXX.sh)
cat > "${TMPSCRIPT}" <<EOF
#!/bin/bash
LOG_DIR="/mnt/weka/home/romain/logs/dedup_bands/\${SLURM_ARRAY_JOB_ID}"
mkdir -p "\${LOG_DIR}"
exec > "\${LOG_DIR}/band_\${SLURM_ARRAY_TASK_ID}.out"
exec 2> "\${LOG_DIR}/band_\${SLURM_ARRAY_TASK_ID}.err"
echo "Processing band \${SLURM_ARRAY_TASK_ID} (threshold=${THRESHOLD})"
uv run dactory dedup-bands '${SIGS}' '${DUPES}' --band "\${SLURM_ARRAY_TASK_ID}" --threshold ${THRESHOLD}
EOF

sbatch --array=0-${MAX_BAND} \
    --job-name=dedup_bands \
    --nodes=1 --ntasks=1 --cpus-per-task=2 --mem=32G \
    --partition=priority \
    "${TMPSCRIPT}"
