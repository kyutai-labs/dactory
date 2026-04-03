#!/bin/bash
# Serve a model locally on 8 GPUs for quality/edu classification.
#
# Usage:
#   bash labeling/serve_vllm.sh [model]
#
# The server exposes an OpenAI-compatible API at http://localhost:8000/v1

set -euo pipefail

MODEL="${1:-Qwen/Qwen3-235B-A22B-FP8}"

# Use local disk for Triton/torch caches to avoid race conditions on shared FS
export TRITON_CACHE_DIR="/tmp/triton_cache_$$"
export TORCHINDUCTOR_CACHE_DIR="/tmp/torchinductor_$$"

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$SCRIPT_DIR/.venv"

"$VENV/bin/vllm" serve "$MODEL" \
    --tensor-parallel-size 8 \
    --enable-expert-parallel \
    --enable-prefix-caching \
    --max-model-len 8192 \
    --max-num-seqs 64 \
    --gpu-memory-utilization 0.90 \
    --port 8000
