#!/bin/bash
#
# Submit the 3-step cross-group MinHash dedup pipeline on a Slurm cluster.
# Each step is submitted as a Slurm array job with dependency chaining,
# so all three are queued immediately but execute in order.
#
#   Step 1: compute-sigs  (one task per shard)
#   Step 2: dedup-bands   (one task per LSH band)
#   Step 3: dedup-filter  (one task per shard)
#
# Usage:
#   bash scripts/postprocess/submit_dedup.sh INPUT_DIR OUTPUT_DIR [THRESHOLD]
#
# Example:
#   bash scripts/postprocess/submit_dedup.sh /data/crawl_v1 /data/crawl_v1_dedup 0.9

set -euo pipefail

INPUT_DIR=${1:?Usage: bash submit_dedup.sh INPUT_DIR OUTPUT_DIR [THRESHOLD]}
OUTPUT_DIR=${2:?Usage: bash submit_dedup.sh INPUT_DIR OUTPUT_DIR [THRESHOLD]}
THRESHOLD=${3:-0.9}
NUM_PERM=${NUM_PERM:-112}
PARTITION=${PARTITION:-priority}
DACTORY_DIR=${DACTORY_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}
LOG_DIR=${LOG_DIR:-${OUTPUT_DIR}/logs}

SIGS_DIR="${OUTPUT_DIR}_sigs"
DUPES_DIR="${OUTPUT_DIR}_dupes"

# Compute array ranges
NUM_SHARDS=$(ls "${INPUT_DIR}"/*.jsonl.zstd 2>/dev/null | wc -l)
if [ "$NUM_SHARDS" -eq 0 ]; then
    echo "No .jsonl.zstd files found in ${INPUT_DIR}"
    exit 1
fi
MAX_SHARD=$((NUM_SHARDS - 1))

NUM_BANDS=$(cd "$DACTORY_DIR" && uv run python -c \
    "from datasketch import MinHashLSH; print(MinHashLSH(threshold=${THRESHOLD}, num_perm=${NUM_PERM}).b)")
MAX_BAND=$((NUM_BANDS - 1))

echo "Input:     ${INPUT_DIR} (${NUM_SHARDS} shards)"
echo "Output:    ${OUTPUT_DIR}"
echo "Threshold: ${THRESHOLD} (${NUM_BANDS} bands, ${NUM_PERM} permutations)"
echo "Partition: ${PARTITION}"
echo ""

# Step 1: compute signatures
echo "=== Step 1: compute-sigs (array 0-${MAX_SHARD}) ==="
SIGS_JOB=$(sbatch --parsable \
    --array=0-${MAX_SHARD} \
    --job-name=dedup_sigs \
    --cpus-per-task=9 --mem-per-cpu=4G \
    --partition="${PARTITION}" \
    --wrap="
mkdir -p '${SIGS_DIR}' '${LOG_DIR}/compute_sigs'
exec  > '${LOG_DIR}/compute_sigs/shard_\${SLURM_ARRAY_TASK_ID}.out'
exec 2> '${LOG_DIR}/compute_sigs/shard_\${SLURM_ARRAY_TASK_ID}.err'
cd '${DACTORY_DIR}'
uv run dactory compute-sigs '${INPUT_DIR}' '${SIGS_DIR}' \
    --num-perm ${NUM_PERM} --shard \${SLURM_ARRAY_TASK_ID} -w 8
")
echo "Submitted: ${SIGS_JOB}"

# Step 2: find duplicates per LSH band
echo "=== Step 2: dedup-bands (array 0-${MAX_BAND}) ==="
BANDS_JOB=$(sbatch --parsable \
    --dependency=afterok:${SIGS_JOB} \
    --array=0-${MAX_BAND} \
    --job-name=dedup_bands \
    --cpus-per-task=2 --mem=32G \
    --partition="${PARTITION}" \
    --wrap="
mkdir -p '${DUPES_DIR}' '${LOG_DIR}/dedup_bands'
exec  > '${LOG_DIR}/dedup_bands/band_\${SLURM_ARRAY_TASK_ID}.out'
exec 2> '${LOG_DIR}/dedup_bands/band_\${SLURM_ARRAY_TASK_ID}.err'
cd '${DACTORY_DIR}'
uv run dactory dedup-bands '${SIGS_DIR}' '${DUPES_DIR}' \
    --band \${SLURM_ARRAY_TASK_ID} --threshold ${THRESHOLD} --num-perm ${NUM_PERM}
")
echo "Submitted: ${BANDS_JOB} (after ${SIGS_JOB})"

# Step 3: filter documents
echo "=== Step 3: dedup-filter (array 0-${MAX_SHARD}) ==="
FILTER_JOB=$(sbatch --parsable \
    --dependency=afterok:${BANDS_JOB} \
    --array=0-${MAX_SHARD} \
    --job-name=dedup_filter \
    --cpus-per-task=2 --mem=16G \
    --partition="${PARTITION}" \
    --wrap="
mkdir -p '${OUTPUT_DIR}' '${LOG_DIR}/dedup_filter'
exec  > '${LOG_DIR}/dedup_filter/shard_\${SLURM_ARRAY_TASK_ID}.out'
exec 2> '${LOG_DIR}/dedup_filter/shard_\${SLURM_ARRAY_TASK_ID}.err'
cd '${DACTORY_DIR}'
uv run dactory dedup-filter '${INPUT_DIR}' '${OUTPUT_DIR}' '${DUPES_DIR}' \
    --sigs-dir '${SIGS_DIR}' --shard \${SLURM_ARRAY_TASK_ID}
")
echo "Submitted: ${FILTER_JOB} (after ${BANDS_JOB})"

echo ""
echo "Pipeline: ${SIGS_JOB} -> ${BANDS_JOB} -> ${FILTER_JOB}"
echo "Output:   ${OUTPUT_DIR}"
