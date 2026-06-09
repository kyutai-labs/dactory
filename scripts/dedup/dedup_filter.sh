#!/bin/bash

# Usage: bash dedup_filter.sh INPUT_DIR OUTPUT_DIR DUPES_DIR SIGS_DIR
# Submits a Slurm array job (one task per shard) to filter duplicates in parallel.

INPUT=${1:?Usage: bash dedup_filter.sh INPUT_DIR OUTPUT_DIR DUPES_DIR SIGS_DIR}
OUTPUT=${2:?Usage: bash dedup_filter.sh INPUT_DIR OUTPUT_DIR DUPES_DIR SIGS_DIR}
DUPES=${3:?Usage: bash dedup_filter.sh INPUT_DIR OUTPUT_DIR DUPES_DIR SIGS_DIR}
SIGS=${4:?Usage: bash dedup_filter.sh INPUT_DIR OUTPUT_DIR DUPES_DIR SIGS_DIR}

NUM_SHARDS=$(ls "${SIGS}"/*.sigs.npy 2>/dev/null | wc -l)
MAX_SHARD=$((NUM_SHARDS - 1))
echo "${NUM_SHARDS} shards (array 0-${MAX_SHARD})"

TMPSCRIPT=$(mktemp /tmp/dedup_filter_XXXXXX.sh)
cat > "${TMPSCRIPT}" <<EOF
#!/bin/bash
LOG_DIR="/mnt/weka/home/romain/logs/dedup_filter/\${SLURM_ARRAY_JOB_ID}"
mkdir -p "\${LOG_DIR}"
exec > "\${LOG_DIR}/shard_\${SLURM_ARRAY_TASK_ID}.out"
exec 2> "\${LOG_DIR}/shard_\${SLURM_ARRAY_TASK_ID}.err"
echo "Filtering shard \${SLURM_ARRAY_TASK_ID}"
uv run dactory dedup-filter '${INPUT}' '${OUTPUT}' '${DUPES}' --sigs-dir '${SIGS}' --shard "\${SLURM_ARRAY_TASK_ID}"
EOF

sbatch --array=0-${MAX_SHARD} \
    --job-name=dedup_filter \
    --nodes=1 --ntasks=1 --cpus-per-task=2 --mem=16G \
    --partition=priority \
    "${TMPSCRIPT}"
