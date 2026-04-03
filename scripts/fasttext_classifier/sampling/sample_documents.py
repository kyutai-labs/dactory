"""
Sample documents from a single CC group for LLM quality labeling.
Uses yomikomi for fast multithreaded reading.
Optionally resamples languages to match a target distribution.

Designed to run as a SLURM array job (one task per group).

Usage:
    python sample_documents.py \
        --input /mnt/weka/datasets/crawl_3/CC-MAIN-2025-43/0.jsonl.zstd \
        --output workdir/group_0.jsonl \
        --num-samples 10000
"""

import argparse
import json
import random
from collections import defaultdict

import yomikomi as yk


FIELDS = ["text", "language", "url"]


class LanguageSampler:
    """Resamples documents to match a target language distribution.
    Same logic as klimt.data.pretrain.LanguageSampler."""

    def __init__(self, target_dist: dict[str, float], ema_alpha=0.001, warmup=1000):
        self.target_dist = target_dist
        self.ema_alpha = ema_alpha
        self.warmup = warmup
        self.samples_seen = 0
        self.obs_ema = defaultdict(float)

    def _get_category(self, lang: str) -> str:
        return lang if lang in self.target_dist else "others"

    def _update_obs_ema(self, cat: str):
        for c in self.obs_ema:
            self.obs_ema[c] *= 1 - self.ema_alpha
        self.obs_ema[cat] += self.ema_alpha

    def __call__(self, example: dict) -> bool:
        lang = bytes(example["language"]).decode("utf-8")
        cat = self._get_category(lang)
        self.samples_seen += 1
        self._update_obs_ema(cat)

        if self.samples_seen < self.warmup:
            return True

        raw = {
            c: self.target_dist[c] / max(self.obs_ema[c], 1e-6)
            for c in self.target_dist
        }
        max_ratio = max(raw.values())
        prob = raw[cat] / max_ratio
        return random.random() < prob


def decode_example(example: dict) -> dict:
    return {
        "text": bytes(example["text"]).decode("utf-8"),
        "language": bytes(example["language"]).decode("utf-8"),
        "url": bytes(example["url"]).decode("utf-8"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to a single .jsonl.zstd group file")
    parser.add_argument("--output", required=True)
    parser.add_argument("--num-samples", type=int, default=10_000)
    parser.add_argument("--max-text-length", type=int, default=8000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--lang-target-dist",
        default=None,
        help="Target language distribution, e.g. 'en:0.875,fr:0.012,others:0.065'",
    )
    args = parser.parse_args()

    random.seed(args.seed)

    lang_sampler = None
    if args.lang_target_dist:
        target_dist = {
            k: float(v)
            for k, v in [x.split(":") for x in args.lang_target_dist.split(",")]
        }
        lang_sampler = LanguageSampler(target_dist)
        print(f"Language resampling: {target_dist}")

    print(f"Sampling {args.num_samples:,} docs from {args.input}...")

    dataset = yk.jsonl(args.input, field=FIELDS, include_if_missing=True)
    if lang_sampler:
        dataset = dataset.filter(lang_sampler)
    dataset = dataset.prefetch(num_threads=8, buffer_size=10000)

    # Reservoir sampling
    sampled = []
    total = 0
    for example in dataset:
        total += 1
        if len(sampled) < args.num_samples:
            sampled.append(decode_example(example))
        else:
            j = random.randint(0, total - 1)
            if j < args.num_samples:
                sampled[j] = decode_example(example)

    print(f"  {total:,} docs seen, {len(sampled):,} sampled")

    # Truncate text for LLM labeling
    for doc in sampled:
        doc["full_text_length"] = len(doc["text"])
        doc["text"] = doc["text"][: args.max_text_length]

    random.shuffle(sampled)

    # Print language distribution
    lang_counts = {}
    for doc in sampled:
        lang_counts[doc["language"]] = lang_counts.get(doc["language"], 0) + 1
    print("Language distribution:")
    for lang, count in sorted(lang_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"  {lang}: {count:,} ({count / len(sampled) * 100:.1f}%)")

    with open(args.output, "w") as f:
        for doc in sampled:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print(f"Written to {args.output}")


if __name__ == "__main__":
    main()
