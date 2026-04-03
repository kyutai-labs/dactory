#!/bin/bash
#SBATCH --job-name=sample_docs
#SBATCH --array=0-99
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=01:00:00

# Sample documents from Common Crawl groups for LLM labeling.
#
# Usage:
#   sbatch --export=INPUT_DIR=/path/to/cc/output,NUM_SAMPLES=100000 \
#       sampling/submit_sampling.sh
#
# Optional env vars:
#   LANG_DIST  - language target distribution (default: EN-heavy)
#   UV_PROJECT - project dir for `uv run` (default: current dir)

set -euo pipefail

: "${INPUT_DIR:?INPUT_DIR must be set (directory containing {group}.jsonl.zstd files)}"

NUM_SAMPLES="${NUM_SAMPLES:-100000}"
LANG_DIST="${LANG_DIST:-en:0.875,fr:0.012,es:0.012,it:0.012,de:0.012,pt:0.012,others:0.065}"
UV_PROJECT="${UV_PROJECT:-.}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/workdir"
mkdir -p "$WORK_DIR/groups"

GROUP=$SLURM_ARRAY_TASK_ID
INPUT_FILE="$INPUT_DIR/${GROUP}.jsonl.zstd"

if [ ! -f "$INPUT_FILE" ]; then
    echo "Input file not found: $INPUT_FILE"
    exit 0
fi

cd "$UV_PROJECT"

uv run python "$SCRIPT_DIR/sample_documents.py" \
    --input "$INPUT_FILE" \
    --output "$WORK_DIR/groups/group_${GROUP}.jsonl" \
    --num-samples "$NUM_SAMPLES" \
    --lang-target-dist "$LANG_DIST"
