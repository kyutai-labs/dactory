#!/bin/bash
#SBATCH --job-name=serve_qwen3
#SBATCH --nodes=10
#SBATCH --ntasks-per-node=8
#SBATCH --gpus-per-task=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=256G
#SBATCH --partition=priority
#SBATCH --exclusive

# Serve Qwen/Qwen3-32B on N nodes, one vLLM instance per GPU (8 per node).
# After launch, use the endpoints file with label_with_llm.py:
#
#   ENDPOINTS=$(cat workdir/vllm_endpoints_qwen3.txt)
#   python labeling/label_with_llm.py --edu \
#       --input workdir/sampled_1M.jsonl \
#       --output workdir/labeled_1M.jsonl \
#       --api-base "$ENDPOINTS" \
#       --model Qwen/Qwen3-32B
#
# Usage:
#   sbatch labeling/submit_serve_qwen3_32b.sh
#   sbatch --export=MODEL=other/model labeling/submit_serve_qwen3_32b.sh

set -euo pipefail

MODEL="${MODEL:-Qwen/Qwen3-32B}"
BASE_PORT="${BASE_PORT:-8000}"

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK_DIR="$SCRIPT_DIR/workdir"
VENV="$SCRIPT_DIR/.venv"
mkdir -p "$WORK_DIR/logs"

# Use local disk for Triton/torch caches to avoid race conditions on shared FS
export TRITON_CACHE_DIR="/tmp/triton_cache_$$"
export TORCHINDUCTOR_CACHE_DIR="/tmp/torchinductor_$$"

# Build comma-separated endpoint list: each node exposes 8 instances on ports BASE_PORT..BASE_PORT+7
ENDPOINTS_FILE="$WORK_DIR/vllm_endpoints_qwen3.txt"
NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
API_BASES=""
for node in $NODES; do
    for gpu_id in $(seq 0 7); do
        port=$((BASE_PORT + gpu_id))
        if [ -n "$API_BASES" ]; then
            API_BASES="${API_BASES},"
        fi
        API_BASES="${API_BASES}http://${node}:${port}/v1"
    done
done
echo "$API_BASES" > "$ENDPOINTS_FILE"
echo "Endpoints: $API_BASES"
echo "Endpoints file: $ENDPOINTS_FILE"

# Launch one vLLM server per GPU.
# SLURM sets CUDA_VISIBLE_DEVICES to the task's assigned GPU automatically (--gpus-per-task=1).
# SLURM_LOCALID (0-7) is used to assign unique ports within each node.
export BASE_PORT VENV MODEL
srun bash -c '
    PORT=$((BASE_PORT + SLURM_LOCALID))
    # Each task needs a unique MASTER_PORT for torch distributed rendezvous
    export MASTER_PORT=$((29500 + SLURM_LOCALID))
    # Per-task local compile cache to avoid shared-FS race conditions
    COMPILE_CACHE="/tmp/vllm_compile_${SLURM_JOB_ID}_${SLURM_LOCALID}"
    exec "$VENV/bin/vllm" serve "$MODEL" \
        --tensor-parallel-size 1 \
        --enable-prefix-caching \
        --max-model-len 4096 \
        --max-num-seqs 64 \
        --gpu-memory-utilization 0.90 \
        --compilation-config "{\"cache_dir\": \"$COMPILE_CACHE\"}" \
        --port "$PORT"
'
