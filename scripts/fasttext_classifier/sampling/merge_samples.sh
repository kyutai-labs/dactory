#!/bin/bash
# Merge per-group samples into a single shuffled file.
# Run after all SLURM array tasks complete.
#
# Usage: bash sampling/merge_samples.sh [output_name]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/workdir"
OUTPUT_NAME="${1:-sampled_10M.jsonl}"

echo "Merging per-group samples..."
cat "$WORK_DIR"/groups/group_*.jsonl | shuf > "$WORK_DIR/$OUTPUT_NAME"

TOTAL=$(wc -l < "$WORK_DIR/$OUTPUT_NAME")
echo "Total samples: $TOTAL"
echo "Written to $WORK_DIR/$OUTPUT_NAME"
