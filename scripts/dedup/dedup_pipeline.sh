#!/bin/bash

# Chains the 3-stage dedup pipeline with Slurm dependencies.
# Usage: bash dedup_pipeline.sh INPUT_DIR WORK_DIR OUTPUT_DIR [THRESHOLD]
#   INPUT_DIR  - directory of .jsonl.zstd files to dedup
#   WORK_DIR   - directory for intermediate artifacts (sigs/, dupes/)
#   OUTPUT_DIR - directory for filtered output
#   THRESHOLD  - MinHash LSH similarity threshold (default 0.75)

set -euo pipefail

INPUT=${1:?Usage: bash dedup_pipeline.sh INPUT_DIR WORK_DIR OUTPUT_DIR [THRESHOLD]}
WORK=${2:?Usage: bash dedup_pipeline.sh INPUT_DIR WORK_DIR OUTPUT_DIR [THRESHOLD]}
OUTPUT=${3:?Usage: bash dedup_pipeline.sh INPUT_DIR WORK_DIR OUTPUT_DIR [THRESHOLD]}
THRESHOLD=${4:-0.75}

SIGS="${WORK}/sigs"
DUPES="${WORK}/dupes"
mkdir -p "${SIGS}" "${DUPES}" "${OUTPUT}"

NUM_SHARDS=$(ls "${INPUT}"/*.jsonl.zstd 2>/dev/null | wc -l)
if [ "${NUM_SHARDS}" -eq 0 ]; then
    echo "No .jsonl.zstd files in ${INPUT}" >&2
    exit 1
fi
MAX_SHARD=$((NUM_SHARDS - 1))

NUM_BANDS=$(uv run python -c "from datasketch import MinHashLSH; print(MinHashLSH(threshold=${THRESHOLD}, num_perm=112).b)")
MAX_BAND=$((NUM_BANDS - 1))

echo "Input:     ${INPUT} (${NUM_SHARDS} shards)"
echo "Work:      ${WORK}"
echo "Output:    ${OUTPUT}"
echo "Threshold: ${THRESHOLD} -> ${NUM_BANDS} bands"
echo

# Stage 1: compute signatures
SIGS_SCRIPT=$(mktemp /tmp/dedup_sigs_XXXXXX.sh)
cat > "${SIGS_SCRIPT}" <<EOF
#!/bin/bash
LOG_DIR="/mnt/weka/home/romain/logs/dedup_sigs/\${SLURM_ARRAY_JOB_ID}"
mkdir -p "\${LOG_DIR}"
exec > "\${LOG_DIR}/shard_\${SLURM_ARRAY_TASK_ID}.out"
exec 2> "\${LOG_DIR}/shard_\${SLURM_ARRAY_TASK_ID}.err"
echo "Computing signatures for shard \${SLURM_ARRAY_TASK_ID}"
uv run dactory compute-sigs '${INPUT}' '${SIGS}' --shard "\${SLURM_ARRAY_TASK_ID}" -w 8
EOF

JOB1=$(sbatch --parsable \
    --array=0-${MAX_SHARD} \
    --job-name=dedup_sigs \
    --nodes=1 --ntasks=1 --cpus-per-task=9 --mem-per-cpu=4G \
    --partition=priority \
    "${SIGS_SCRIPT}")
echo "Stage 1 (compute-sigs) submitted: ${JOB1}"

# Stage 2: per-band duplicate detection (depends on stage 1)
BANDS_SCRIPT=$(mktemp /tmp/dedup_bands_XXXXXX.sh)
cat > "${BANDS_SCRIPT}" <<EOF
#!/bin/bash
LOG_DIR="/mnt/weka/home/romain/logs/dedup_bands/\${SLURM_ARRAY_JOB_ID}"
mkdir -p "\${LOG_DIR}"
exec > "\${LOG_DIR}/band_\${SLURM_ARRAY_TASK_ID}.out"
exec 2> "\${LOG_DIR}/band_\${SLURM_ARRAY_TASK_ID}.err"
echo "Processing band \${SLURM_ARRAY_TASK_ID} (threshold=${THRESHOLD})"
uv run dactory dedup-bands '${SIGS}' '${DUPES}' --band "\${SLURM_ARRAY_TASK_ID}" --threshold ${THRESHOLD}
EOF

JOB2=$(sbatch --parsable \
    --dependency=afterok:${JOB1} \
    --array=0-${MAX_BAND} \
    --job-name=dedup_bands \
    --nodes=1 --ntasks=1 --cpus-per-task=2 --mem=32G \
    --partition=priority \
    "${BANDS_SCRIPT}")
echo "Stage 2 (dedup-bands) submitted: ${JOB2} (after ${JOB1})"

# Stage 3: filter duplicates from input shards (depends on stage 2)
FILTER_SCRIPT=$(mktemp /tmp/dedup_filter_XXXXXX.sh)
cat > "${FILTER_SCRIPT}" <<EOF
#!/bin/bash
LOG_DIR="/mnt/weka/home/romain/logs/dedup_filter/\${SLURM_ARRAY_JOB_ID}"
mkdir -p "\${LOG_DIR}"
exec > "\${LOG_DIR}/shard_\${SLURM_ARRAY_TASK_ID}.out"
exec 2> "\${LOG_DIR}/shard_\${SLURM_ARRAY_TASK_ID}.err"
echo "Filtering shard \${SLURM_ARRAY_TASK_ID}"
uv run dactory dedup-filter '${INPUT}' '${OUTPUT}' '${DUPES}' --sigs-dir '${SIGS}' --shard "\${SLURM_ARRAY_TASK_ID}"
EOF

JOB3=$(sbatch --parsable \
    --dependency=afterok:${JOB2} \
    --array=0-${MAX_SHARD} \
    --job-name=dedup_filter \
    --nodes=1 --ntasks=1 --cpus-per-task=2 --mem=16G \
    --partition=priority \
    "${FILTER_SCRIPT}")
echo "Stage 3 (dedup-filter) submitted: ${JOB3} (after ${JOB2})"

echo
echo "Pipeline submitted. Track with:"
echo "  squeue -j ${JOB1},${JOB2},${JOB3}"
