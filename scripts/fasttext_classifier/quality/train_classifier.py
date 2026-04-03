"""
Train a fastText quality classifier from LLM-labeled documents.

Expects a JSONL input with "text" and "label" (HIGH/LOW) fields,
plus an optional "confidence" field for filtering.

Usage:
    python train_classifier.py \
        --input labeled_500k.jsonl \
        --output quality_model
"""

import argparse
import json
import random
import tempfile
from collections import Counter
from pathlib import Path

import numpy as np

# Fix numpy 2.x incompatibility with fasttext
_orig_array = np.array


def _patched_array(*args, **kwargs):
    kwargs.pop("copy", None)
    return _orig_array(*args, **kwargs)


np.array = _patched_array


def to_fasttext_line(text: str, label: str) -> str:
    """Convert a document to fastText format: __label__X <text on one line>"""
    clean = text.replace("\n", " ").replace("\r", " ")
    clean = " ".join(clean.split())
    return f"__label__{label} {clean}"


def load_docs(path: str, min_confidence: float) -> list[dict]:
    """Load documents labeled with binary HIGH/LOW quality."""
    docs = []
    skipped_errors = 0
    skipped_confidence = 0
    with open(path) as f:
        for line in f:
            doc = json.loads(line)
            if doc.get("label") == "ERROR":
                skipped_errors += 1
                continue
            if doc.get("confidence") is not None and doc["confidence"] != "null":
                conf = float(doc["confidence"])
                certainty = conf if doc["label"] == "HIGH" else (1.0 - conf)
                if certainty < min_confidence:
                    skipped_confidence += 1
                    continue
            docs.append(doc)

    print(f"Loaded {len(docs):,} labeled documents")
    print(f"  Skipped {skipped_errors:,} errors, {skipped_confidence:,} low-confidence")

    high = sum(1 for d in docs if d["label"] == "HIGH")
    low = len(docs) - high
    print(f"  HIGH: {high:,} ({high / len(docs) * 100:.1f}%)")
    print(f"  LOW:  {low:,} ({low / len(docs) * 100:.1f}%)")
    return docs


def eval_at_threshold(preds, threshold):
    tp = fp = fn = tn = 0
    for high_prob, true_label in preds:
        pred_high = high_prob >= threshold
        true_high = true_label == "HIGH"
        if pred_high and true_high:
            tp += 1
        elif pred_high and not true_high:
            fp += 1
        elif not pred_high and true_high:
            fn += 1
        else:
            tn += 1
    p = tp / (tp + fp) if (tp + fp) > 0 else 0
    r = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0
    acc = (tp + tn) / (tp + fp + fn + tn)
    return {"precision": p, "recall": r, "f1": f1, "accuracy": acc, "tp": tp, "fp": fp, "fn": fn}


def main():
    parser = argparse.ArgumentParser(description="Train a fastText quality classifier")
    parser.add_argument("--input", required=True, help="Labeled JSONL from LLM")
    parser.add_argument("--output", required=True, help="Output model name (without .bin)")
    parser.add_argument("--eval-split", type=float, default=0.1)
    parser.add_argument("--eval-file", type=str, default=None, help="Separate eval JSONL file")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--lr", type=float, default=0.3)
    parser.add_argument("--epoch", type=int, default=5)
    parser.add_argument("--word-ngrams", type=int, default=2)
    parser.add_argument("--dim", type=int, default=300)
    parser.add_argument("--min-confidence", type=float, default=0.0, help="Skip low-confidence docs")
    parser.add_argument("--downsample-ratio", type=float, default=None, help="Downsample majority class")
    parser.add_argument("--language", type=str, default=None, help="Filter to a single language")
    parser.add_argument("--loss", type=str, default="hs", help="Loss function: hs, softmax, ova")
    args = parser.parse_args()

    random.seed(args.seed)

    docs = load_docs(args.input, args.min_confidence)

    if args.language:
        before = len(docs)
        docs = [d for d in docs if d.get("language") == args.language]
        print(f"Filtered to language '{args.language}': {before:,} -> {len(docs):,}")

    random.shuffle(docs)
    if args.eval_file:
        train_docs = docs
        eval_docs = load_docs(args.eval_file, args.min_confidence)
    elif args.eval_split > 0:
        split_idx = int(len(docs) * (1 - args.eval_split))
        train_docs = docs[:split_idx]
        eval_docs = docs[split_idx:]
    else:
        train_docs = docs
        eval_docs = []

    # Downsample majority class in training set
    if args.downsample_ratio is not None:
        labels = set(d["label"] for d in train_docs)
        by_label = {l: [d for d in train_docs if d["label"] == l] for l in labels}
        minority_size = min(len(v) for v in by_label.values())
        majority_label = max(by_label, key=lambda l: len(by_label[l]))
        target_majority = int(minority_size * args.downsample_ratio)
        print(
            f"\nDownsampling '{majority_label}': {len(by_label[majority_label]):,} -> {target_majority:,} "
            f"(ratio {args.downsample_ratio}:1 vs minority {minority_size:,})"
        )
        by_label[majority_label] = random.sample(by_label[majority_label], target_majority)
        train_docs = [d for docs in by_label.values() for d in docs]
        random.shuffle(train_docs)

    print(f"Train: {len(train_docs):,}, Eval: {len(eval_docs):,}")

    with tempfile.TemporaryDirectory() as tmpdir:
        train_path = Path(tmpdir) / "train.txt"
        eval_path = Path(tmpdir) / "eval.txt"

        with open(train_path, "w") as f:
            for doc in train_docs:
                f.write(to_fasttext_line(doc["text"], doc["label"]) + "\n")
        with open(eval_path, "w") as f:
            for doc in eval_docs:
                f.write(to_fasttext_line(doc["text"], doc["label"]) + "\n")

        import fasttext

        print(f"\nTraining fastText model...")
        model = fasttext.train_supervised(
            input=str(train_path),
            lr=args.lr,
            epoch=args.epoch,
            wordNgrams=args.word_ngrams,
            minCount=5,
            dim=args.dim,
            loss=args.loss,
            thread=64,
        )
        model.save_model(f"{args.output}.bin")

        if not eval_docs:
            print(f"\nNo eval set -- skipping evaluation.")
            print(f"\nModel saved to {args.output}.bin")
            return

        print(f"\nEvaluating on {len(eval_docs):,} held-out documents...")
        result = model.test(str(eval_path))
        print(f"  N: {result[0]}, P@1: {result[1]:.3f}, R@1: {result[2]:.3f}")

        # Per-class evaluation
        predictions = []
        for doc in eval_docs:
            text = doc["text"].replace("\n", " ")
            labels, probs = model.predict(text, k=2)
            prob_map = {l.replace("__label__", ""): p for l, p in zip(labels, probs)}
            predictions.append((prob_map.get("HIGH", 0.0), doc["label"]))

        print(f"\nDefault threshold (0.5):")
        m = eval_at_threshold(predictions, 0.5)
        print(f"  HIGH: P={m['precision']:.3f} R={m['recall']:.3f} F1={m['f1']:.3f}")
        print(f"  Accuracy: {m['accuracy']:.3f}")

        # Sweep confidence thresholds
        print(f"\nConfidence threshold sweep (HIGH):")
        print(f"  {'thresh':>6s}  {'P':>6s}  {'R':>6s}  {'F1':>6s}  {'Acc':>6s}  {'TP':>8s}  {'FP':>8s}  {'FN':>8s}")
        best_f1 = 0
        best_thresh = 0.5
        for t in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
            m = eval_at_threshold(predictions, t)
            if m["f1"] > best_f1:
                best_f1 = m["f1"]
                best_thresh = t
            print(
                f"  {t:>6.1f}  {m['precision']:>6.3f}  {m['recall']:>6.3f}  {m['f1']:>6.3f}  {m['accuracy']:>6.3f}  {m['tp']:>8,}  {m['fp']:>8,}  {m['fn']:>8,}"
            )
        print(f"\n  Best F1: {best_f1:.3f} at threshold {best_thresh}")

        # Plot P/R/F1 curve
        thresholds = [i / 100 for i in range(5, 100, 5)]
        precisions, recalls, f1s = [], [], []
        for t in thresholds:
            m = eval_at_threshold(predictions, t)
            precisions.append(m["precision"])
            recalls.append(m["recall"])
            f1s.append(m["f1"])

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.plot(thresholds, precisions, label="Precision", marker=".")
        ax.plot(thresholds, recalls, label="Recall", marker=".")
        ax.plot(thresholds, f1s, label="F1", marker=".", linewidth=2)
        ax.axvline(x=best_thresh, color="gray", linestyle="--", alpha=0.5, label=f"Best F1 @ {best_thresh}")
        ax.set_xlabel("Confidence threshold")
        ax.set_ylabel("Score")
        ax.set_title("P/R/F1 vs confidence threshold (HIGH)")
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        plot_path = f"{args.output}_threshold_curve.png"
        fig.savefig(plot_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"\n  Threshold curve saved to {plot_path}")

    print(f"\nModel saved to {args.output}.bin")


if __name__ == "__main__":
    main()
