#!/bin/bash
#SBATCH --job-name=ft_grid
#SBATCH --array=0-119
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=01:00:00

# Hyperparameter grid search for edu fastText classifier.
# Grid: 5 LRs * 4 epochs * 3 dims * 2 ngrams = 120 configurations.
#
# Usage:
#   sbatch --export=INPUT=/path/to/train.jsonl,EVAL=/path/to/eval.jsonl \
#       edu/grid_search.sh
#
# Optional env vars:
#   THRESHOLD - edu score threshold (default: 3)

set -euo pipefail

: "${INPUT:?INPUT must be set (path to training JSONL)}"
: "${EVAL:?EVAL must be set (path to eval JSONL)}"

THRESHOLD="${THRESHOLD:-3}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/workdir"
RESULTS="$WORK_DIR/grid_search_results.csv"
mkdir -p "$WORK_DIR/logs" "$WORK_DIR/grid_models"

# Grid: 5 * 4 * 3 * 2 = 120
LRS=(0.1 0.3 0.5 0.8 1.0)
EPOCHS=(3 5 10 25)
DIMS=(100 200 300)
NGRAMS=(2 3)

# Map array task ID to grid indices
idx=$SLURM_ARRAY_TASK_ID
ng_idx=$((idx % 2));       idx=$((idx / 2))
dim_idx=$((idx % 3));      idx=$((idx / 3))
epoch_idx=$((idx % 4));    idx=$((idx / 4))
lr_idx=$((idx % 5))

lr=${LRS[$lr_idx]}
epoch=${EPOCHS[$epoch_idx]}
dim=${DIMS[$dim_idx]}
ngrams=${NGRAMS[$ng_idx]}

name="lr${lr}_ep${epoch}_d${dim}_ng${ngrams}"
output="$WORK_DIR/grid_models/$name"

echo "Task $SLURM_ARRAY_TASK_ID: $name"

out=$(python "$SCRIPT_DIR/train_classifier.py" \
    --input "$INPUT" \
    --eval-file "$EVAL" \
    --output "$output" \
    --threshold "$THRESHOLD" \
    --lr "$lr" \
    --epoch "$epoch" \
    --dim "$dim" \
    --word-ngrams "$ngrams" \
    2>&1) || { echo "FAILED: $name"; exit 1; }

echo "$out"

# Parse metrics and append to shared CSV (flock for concurrent writes)
best_f1=$(echo "$out" | grep "Best F1:" | sed 's/.*Best F1: \([0-9.]*\).*/\1/')
best_thresh=$(echo "$out" | grep "Best F1:" | sed 's/.*threshold \([0-9.]*\).*/\1/')

flock "$RESULTS.lock" bash -c "
    [ -f '$RESULTS' ] || echo 'lr,epoch,dim,ngrams,best_f1,best_thresh' > '$RESULTS'
    echo '$lr,$epoch,$dim,$ngrams,$best_f1,$best_thresh' >> '$RESULTS'
"
