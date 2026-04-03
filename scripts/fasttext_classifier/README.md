# FastText Classifier

FastText classifiers that distinguish high-quality web documents from low-quality ones, trained for use in the dactory pipeline.

## Educational Classifier (edu_model)

Binary classifier predicting `edu_high` vs `edu_low` based on educational value, following the [FineWeb-Edu](https://arxiv.org/abs/2406.17557) approach.

### Training data

10M documents sampled from Common Crawl (CC-MAIN-2025-43), labeled by Qwen3-235B-A22B-FP8 on a 0-5 educational score scale using the FineWeb-Edu annotation prompt.

**Score distribution (10M samples):**

| Score | Count | % |
|-------|------:|---:|
| 0 | 4,461,098 | 44.6% |
| 1 | 3,790,113 | 37.9% |
| 2 | 1,124,588 | 11.2% |
| 3 | 534,241 | 5.3% |
| 4 | 81,856 | 0.8% |
| 5 | 865 | 0.01% |

Binary threshold: score >= 3 → `edu_high` (616,962 docs, 6.2%).

### Training procedure

1. **LLM labeling**: 10M documents labeled with Qwen3-235B-A22B-FP8 served via vLLM. Each document receives a 0-5 educational score.
2. **Balanced training set**: All `edu_high` samples kept, `edu_low` downsampled to match (1:1 ratio).
3. **Hyperparameter search**: Grid search over 120 configurations (lr, epochs, dim, word-ngrams) evaluated on a held-out 10% split at natural distribution (~6% edu_high). Best F1 used to select hyperparameters.
4. **Final model**: Retrained on all 10M samples (balanced) using the best hyperparameters. No validation holdout for the production model — all data used for maximum coverage.

### Model details

- Architecture: fastText supervised
- Embedding dim: 300
- Word n-grams: 2
- Learning rate: 0.3
- Epochs: 5
- Loss: hierarchical softmax
- Labels: `edu_high`, `edu_low`

### Evaluation

Evaluated on a held-out 10% split (999,277 docs) at natural distribution before final retraining.

**Threshold sweep (edu_high):**

| Threshold | Precision | Recall | F1 |
|-----------|-----------|--------|----|
| 0.3 | 0.535 | 0.572 | 0.553 |
| 0.5 | 0.641 | 0.435 | 0.518 |
| 0.7 | 0.735 | 0.309 | 0.435 |
| 0.9 | 0.840 | 0.156 | 0.264 |

Best F1: 0.553 at threshold 0.3. For high-precision filtering, use threshold 0.7+ (P=0.735, R=0.309).

**Error analysis:**
- False positives are concentrated at score 2 (72%) — borderline content that is informational but not pedagogical.
- False negatives are concentrated at score 3 (95%) — educational content with commercial or promotional tone.
- Some LLM labeling errors observed (e.g., XML feeds labeled as score 5, educational articles labeled as score 1).

### Normalization

Text is normalized before prediction to match the training format:
- Newlines replaced with spaces
- Carriage returns replaced with spaces
- Multiple whitespace collapsed to single spaces

This normalization is applied in `python/dactory/scoring.py:QualityClassifier.get_quality_score()`.

## DCLM Quality Classifier (quality_model)

Binary classifier predicting `HIGH` vs `LOW` quality based on agreement between curated dataset membership and LLM annotation.

### Training data

~513k documents where two independent signals agree on the quality label.

**Signal 1: Curated datasets vs raw Common Crawl**

Following the DCLM approach, documents from established curated datasets are positive examples, raw unfiltered Common Crawl are negatives.

- Positives (~20k per language, 24 EU languages): FineWeb-2, FineWeb, DCLM-baseline
- Negatives (~20k per language): Raw CC-MAIN-2024-10, language detected with fastText lid.176 (confidence > 0.8)

**Signal 2: LLM annotation**

~990k raw Common Crawl documents labeled by Qwen3-235B-A22B-FP8 via vLLM as HIGH/LOW.

**Combining signals:** Kept only 513k documents where both signals agree (272k HIGH, 241k LOW). Final classifier trained on this intersection reaches P@1 = 0.951.

### Model details

- Architecture: fastText supervised
- Embedding dim: 100
- Word n-grams: 2
- Epochs: 25
- Loss: hierarchical softmax

### Evaluation

| Metric | HIGH | LOW |
|--------|------|-----|
| Precision | 0.944 | 0.962 |
| Recall | 0.967 | 0.935 |
| F1 | 0.955 | 0.948 |

Overall accuracy: 0.952

## Directory structure

```
fasttext_classifier/
├── sampling/
│   ├── sample_documents.py          # Sample documents from processed Common Crawl output
│   ├── merge_samples.sh             # Merge per-group samples into one shuffled file
│   └── submit_sampling.sh           # SLURM array job: sample 100k docs per CC group
├── labeling/
│   ├── label_with_llm.py            # Label documents with an LLM served via vLLM (quality + edu modes)
│   ├── serve_vllm.sh                # Launch local vLLM server (single node, 8 GPUs)
│   ├── submit_serve_llm.sh          # SLURM: serve Llama-70B on 12 nodes
│   ├── submit_serve_qwen3_32b.sh    # SLURM: serve Qwen3-32B (80 instances, 10 nodes)
│   ├── submit_serve_qwen3_235b.sh   # SLURM: serve Qwen3-235B (12 nodes)
│   ├── run_label_qwen3_32b.sh       # Label with Qwen3-32B endpoints
│   └── run_label_qwen3_235b.sh      # Label with Qwen3-235B endpoints
├── quality/
│   ├── run_all.sh                   # End-to-end quality classifier pipeline
│   ├── train_classifier.py          # Train fastText quality classifier (HIGH/LOW)
│   └── sample_hq_datasets.py        # Sample positives from curated datasets + negatives from raw CC
└── edu/
    ├── run_edu.sh                   # End-to-end edu classifier pipeline
    ├── train_classifier.py          # Train fastText edu classifier (edu_high/edu_low)
    ├── train_embed_classifier.py    # Fine-tune Qwen3-Embedding for edu classification (DDP)
    ├── grid_search.sh               # SLURM grid search over fastText hyperparameters
    ├── add_score.py                 # Add edu scores to existing JSONL shards
    └── submit_add_score.sh          # SLURM array job: distributed edu scoring
```

## Pipeline overview

Both classifiers follow the same high-level pipeline:

1. **Sample** documents from Common Crawl
2. **Serve** an LLM via vLLM
3. **Label** documents with the LLM
4. **Train** a fastText classifier on the labels

### Quality classifier

The quality classifier combines two signals: curated dataset membership (DCLM approach) and LLM annotation. Only documents where both signals agree are kept for training.

```bash
# 1. Sample positives from curated datasets + negatives from raw CC
python quality/sample_hq_datasets.py \
    --output workdir/hq_training_data.jsonl \
    --num-samples-per-lang 20000

# 2. Serve LLM and label raw CC documents
bash labeling/serve_vllm.sh  # optional: pass model name as argument
python labeling/label_with_llm.py \
    --input workdir/sampled_500k.jsonl \
    --output workdir/labeled_500k.jsonl \
    --api-base http://localhost:8000/v1 \
    --model Qwen/Qwen3-235B-A22B-FP8

# 3. Train on the intersection of both signals
python quality/train_classifier.py \
    --input workdir/labeled_500k.jsonl \
    --output workdir/quality_model \
    --min-confidence 0.7

# Or run the full pipeline at once:
bash quality/run_all.sh /path/to/cc/output
```

### Educational classifier

The edu classifier uses LLM-assigned 0-5 educational scores (FineWeb-Edu style), binarized at a threshold.

```bash
# 1. Sample documents from CC (at scale, using SLURM)
sbatch --export=INPUT_DIR=/path/to/cc/output \
    sampling/submit_sampling.sh                  # 100 groups × 100k docs
bash sampling/merge_samples.sh                   # merge into sampled_10M.jsonl

# 2. Serve LLM and label (at scale, using SLURM)
sbatch labeling/submit_serve_qwen3_235b.sh
bash labeling/run_label_qwen3_235b.sh \
    workdir/sampled_10M.jsonl workdir/labeled_edu_10M.jsonl

# 3. Train classifier (score >= 3 → edu_high)
python edu/train_classifier.py \
    --input workdir/labeled_edu_10M.jsonl \
    --output workdir/edu_model \
    --threshold 3

# 4. (Optional) Hyperparameter grid search
sbatch --export=INPUT=workdir/train.jsonl,EVAL=workdir/eval.jsonl \
    edu/grid_search.sh

# 5. Apply edu scores to existing shards
sbatch --export=INPUT_DIR=/path/to/shards,OUTPUT_DIR=/path/to/output,MODEL=workdir/edu_model.bin \
    edu/submit_add_score.sh

# Or run a smaller end-to-end pipeline:
bash edu/run_edu.sh /path/to/cc/output
```

## Using the trained models

```python
import fasttext

# Normalize text (must match training format)
text = document_text.replace("\n", " ").replace("\r", " ")
text = " ".join(text.split())

# Educational classifier
model = fasttext.load_model("edu_model.bin")
labels, scores = model.predict(text, k=1)
label = labels[0].replace("__label__", "")  # "edu_high" or "edu_low"

# Quality classifier
model = fasttext.load_model("quality_model.bin")
labels, scores = model.predict(text, k=1)
label = labels[0].replace("__label__", "")  # "HIGH" or "LOW"
```

Both classifiers are integrated in the dactory pipeline via CLI flags:

```bash
# Quality classifier
dactory create output/ --quality-classifier quality_model.bin --max-dclm-low-score 0.3

# Edu classifier (scoring only, no filtering)
dactory create output/ --edu-classifier edu_model.bin
```
