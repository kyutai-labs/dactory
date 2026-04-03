#!/bin/bash
#SBATCH --job-name=add_edu
#SBATCH --array=0-99
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=02:00:00

# Add edu scores to JSONL shards using a pre-trained fastText model.
#
# Usage:
#   sbatch --export=INPUT_DIR=/path/to/shards,OUTPUT_DIR=/path/to/output,MODEL=/path/to/model.bin \
#       edu/submit_add_score.sh

set -euo pipefail

: "${INPUT_DIR:?INPUT_DIR must be set (directory containing shard_XX.jsonl files)}"
: "${OUTPUT_DIR:?OUTPUT_DIR must be set (output directory for scored shards)}"
: "${MODEL:?MODEL must be set (path to edu fastText model .bin)}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

mkdir -p "$OUTPUT_DIR"

SHARD=$(printf "shard_%02d.jsonl" "$SLURM_ARRAY_TASK_ID")
INPUT="$INPUT_DIR/$SHARD"
OUTPUT="$OUTPUT_DIR/$SHARD"

if [ ! -f "$INPUT" ]; then
    echo "Input not found: $INPUT"
    exit 0
fi

if [ -f "$OUTPUT" ]; then
    echo "Output already exists: $OUTPUT, skipping."
    exit 0
fi

python "$SCRIPT_DIR/add_score.py" \
    --input "$INPUT" \
    --output "$OUTPUT" \
    --model "$MODEL"
