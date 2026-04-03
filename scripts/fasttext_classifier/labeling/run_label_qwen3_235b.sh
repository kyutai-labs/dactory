#!/bin/bash
# Label documents with Qwen3-235B using distributed vLLM endpoints.
#
# Usage:
#   bash labeling/run_label_qwen3_235b.sh <input> <output>
#
# Reads endpoints from workdir/vllm_endpoints.txt (written by submit_serve_qwen3_235b.sh).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/workdir"

INPUT="${1:?Usage: $0 <input.jsonl> <output.jsonl>}"
OUTPUT="${2:?Usage: $0 <input.jsonl> <output.jsonl>}"
MODEL="${MODEL:-Qwen/Qwen3-235B-A22B-FP8}"
MAX_CONCURRENT="${MAX_CONCURRENT:-512}"
ENDPOINTS=$(cat "$WORK_DIR/vllm_endpoints.txt")

python "$SCRIPT_DIR/label_with_llm.py" --edu \
    --input "$INPUT" \
    --output "$OUTPUT" \
    --api-base "$ENDPOINTS" \
    --model "$MODEL" \
    --max-concurrent "$MAX_CONCURRENT"
