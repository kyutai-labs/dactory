#!/bin/bash
#SBATCH --job-name=serve_llm
#SBATCH --nodes=12
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-node=8
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --partition=priority
#SBATCH --exclusive

# Serve Meta-Llama-3-70B-Instruct on N nodes (one vLLM instance per node).
# After launch, use the endpoints file with label_with_llm.py:
#
#   ENDPOINTS=$(cat workdir/vllm_endpoints.txt)
#   python labeling/label_with_llm.py --edu \
#       --input workdir/sampled_1M.jsonl \
#       --output workdir/labeled_1M.jsonl \
#       --api-base "$ENDPOINTS" \
#       --model meta-llama/Meta-Llama-3-70B-Instruct
#
# Usage:
#   sbatch labeling/submit_serve_llm.sh
#   sbatch --export=MODEL=other/model labeling/submit_serve_llm.sh

set -euo pipefail

MODEL="${MODEL:-meta-llama/Meta-Llama-3-70B-Instruct}"
PORT="${PORT:-8000}"

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK_DIR="$SCRIPT_DIR/workdir"
VENV="$SCRIPT_DIR/.venv"
mkdir -p "$WORK_DIR/logs"

# Use local disk for Triton/torch caches to avoid race conditions on shared FS
export TRITON_CACHE_DIR="/tmp/triton_cache_$$"
export TORCHINDUCTOR_CACHE_DIR="/tmp/torchinductor_$$"

# Build comma-separated endpoint list for label_with_llm.py
ENDPOINTS_FILE="$WORK_DIR/vllm_endpoints.txt"
NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
API_BASES=""
for node in $NODES; do
    if [ -n "$API_BASES" ]; then
        API_BASES="${API_BASES},"
    fi
    API_BASES="${API_BASES}http://${node}:${PORT}/v1"
done
echo "$API_BASES" > "$ENDPOINTS_FILE"
echo "Endpoints: $API_BASES"
echo "Endpoints file: $ENDPOINTS_FILE"

# Launch one vLLM server per node
srun "$VENV/bin/vllm" serve "$MODEL" \
    --tensor-parallel-size 8 \
    --enable-prefix-caching \
    --max-model-len 4096 \
    --max-num-seqs 256 \
    --gpu-memory-utilization 0.90 \
    --port "$PORT"
