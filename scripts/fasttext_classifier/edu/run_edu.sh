#!/bin/bash
# End-to-end pipeline for training a FineWeb-Edu style educational classifier.
#
# Prerequisites:
#   - vLLM server running (see labeling/serve_vllm.sh)
#
# Usage:
#   bash edu/run_edu.sh <input_dir> [--api-base URL] [--model MODEL] [--threshold N]
#
# Example:
#   bash edu/run_edu.sh /path/to/cc/output --api-base http://localhost:8000/v1

set -euo pipefail

INPUT_DIR="${1:?Usage: $0 <input_dir> [--api-base URL] [--model MODEL] [--threshold N]}"
shift

API_BASE="http://localhost:8000/v1"
MODEL="Qwen/Qwen3-235B-A22B-FP8"
THRESHOLD=3
while [[ $# -gt 0 ]]; do
    case $1 in
        --api-base) API_BASE="$2"; shift 2 ;;
        --model) MODEL="$2"; shift 2 ;;
        --threshold) THRESHOLD="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/workdir"
mkdir -p "$WORK_DIR"

echo "=== Step 1: Sample 500k documents ==="
if [ -f "$WORK_DIR/sampled_500k.jsonl" ]; then
    echo "Samples already exist, skipping."
else
    python "$SCRIPT_DIR/../sampling/sample_documents.py" \
        --input-dir "$INPUT_DIR" \
        --output "$WORK_DIR/sampled_500k.jsonl" \
        --num-samples 500000 \
        --groups 0,1,2,3,4
fi

echo ""
echo "=== Step 2: Label with LLM (educational scoring) ==="
python "$SCRIPT_DIR/../labeling/label_with_llm.py" --edu \
    --input "$WORK_DIR/sampled_500k.jsonl" \
    --output "$WORK_DIR/labeled_edu_500k.jsonl" \
    --api-base "$API_BASE" \
    --model "$MODEL"

echo ""
echo "=== Step 3: Train fastText edu classifier ==="
python "$SCRIPT_DIR/train_classifier.py" \
    --input "$WORK_DIR/labeled_edu_500k.jsonl" \
    --output "$WORK_DIR/edu_model" \
    --threshold "$THRESHOLD"

echo ""
echo "=== Done ==="
echo "Model saved to: $WORK_DIR/edu_model.bin"
echo ""
echo "To use with dactory:"
echo "  dactory create ./output --edu-classifier $WORK_DIR/edu_model.bin"
