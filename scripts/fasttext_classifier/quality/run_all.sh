#!/bin/bash
# End-to-end pipeline for training a quality classifier.
#
# Prerequisites:
#   - vLLM server running (see labeling/serve_vllm.sh)
#
# Usage:
#   bash quality/run_all.sh <input_dir> [--api-base URL] [--model MODEL]
#
# Example:
#   bash quality/run_all.sh /path/to/cc/output --api-base http://localhost:8000/v1

set -euo pipefail

INPUT_DIR="${1:?Usage: $0 <input_dir> [--api-base URL] [--model MODEL]}"
shift

API_BASE="http://localhost:8000/v1"
MODEL="Qwen/Qwen3-235B-A22B-FP8"
while [[ $# -gt 0 ]]; do
    case $1 in
        --api-base) API_BASE="$2"; shift 2 ;;
        --model) MODEL="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/workdir"
mkdir -p "$WORK_DIR"

echo "=== Step 1: Sample 500k documents ==="
python "$SCRIPT_DIR/../sampling/sample_documents.py" \
    --input-dir "$INPUT_DIR" \
    --output "$WORK_DIR/sampled_500k.jsonl" \
    --num-samples 500000 \
    --groups 0,1,2,3,4

echo ""
echo "=== Step 2: Label with LLM ==="
python "$SCRIPT_DIR/../labeling/label_with_llm.py" \
    --input "$WORK_DIR/sampled_500k.jsonl" \
    --output "$WORK_DIR/labeled_500k.jsonl" \
    --api-base "$API_BASE" \
    --model "$MODEL"

echo ""
echo "=== Step 3: Train fastText classifier ==="
python "$SCRIPT_DIR/train_classifier.py" \
    --input "$WORK_DIR/labeled_500k.jsonl" \
    --output "$WORK_DIR/quality_model" \
    --min-confidence 0.7

echo ""
echo "=== Done ==="
echo "Model saved to: $WORK_DIR/quality_model.bin"
echo ""
echo "To use with dactory:"
echo "  dactory create ./output --quality-classifier $WORK_DIR/quality_model.bin --max-dclm-low-score 0.3"
