"""
Build training data for a quality classifier by combining:
  - Positive (HIGH): curated datasets (FineWeb2, DCLM-baseline)
  - Negative (LOW): raw unfiltered Common Crawl via dactory's WARC extraction

Both sides are balanced across languages to prevent the classifier
from learning language detection instead of quality.

Usage:
    python sample_hq_datasets.py \
        --output workdir/hq_training_data.jsonl \
        --samples-per-lang 20000

Requires: datasets, fasttext, fastwarc, resiliparse, requests
(all available in the dactory venv: run with `uv run python`)
"""

import argparse
import gzip
import io
import json
import random
import time
from collections import defaultdict

# Fix numpy 2.x incompatibility with fasttext
import numpy as np

_orig_array = np.array


def _patched_array(*args, **kwargs):
    kwargs.pop("copy", None)
    return _orig_array(*args, **kwargs)


np.array = _patched_array

import fasttext
import requests
from datasets import load_dataset
from fastwarc.warc import ArchiveIterator
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.encoding import detect_encoding

# Map dactory language codes to FineWeb2 subset names
LANG_TO_FW2 = {
    "bg": "bul_Cyrl",
    "cs": "ces_Latn",
    "da": "dan_Latn",
    "de": "deu_Latn",
    "el": "ell_Grek",
    "en": "eng_Latn",
    "es": "spa_Latn",
    "et": "est_Latn",
    "fi": "fin_Latn",
    "fr": "fra_Latn",
    "ga": "gle_Latn",
    "hr": "hrv_Latn",
    "hu": "hun_Latn",
    "it": "ita_Latn",
    "lt": "lit_Latn",
    "lv": "lav_Latn",
    "mt": "mlt_Latn",
    "nl": "nld_Latn",
    "pl": "pol_Latn",
    "pt": "por_Latn",
    "ro": "ron_Latn",
    "sk": "slk_Latn",
    "sl": "slv_Latn",
    "sv": "swe_Latn",
}

LID_MODEL_URL = (
    "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
)


def reservoir_sample(iterator, n, desc=""):
    """Reservoir sampling from an iterator."""
    reservoir = []
    count = 0
    t0 = time.time()
    for item in iterator:
        count += 1
        if count <= n:
            reservoir.append(item)
        else:
            j = random.randint(0, count - 1)
            if j < n:
                reservoir[j] = item
        if count % 10000 == 0:
            elapsed = time.time() - t0
            print(
                f"  [{desc}] {count:,} scanned, {len(reservoir):,} sampled ({elapsed:.0f}s)",
                flush=True,
            )
        if count >= n * 10:
            break
    print(
        f"  [{desc}] Done: {count:,} scanned -> {len(reservoir):,} sampled",
        flush=True,
    )
    return reservoir


def get_warc_paths(corpus: str) -> list[str]:
    """Fetch all WARC paths for a CC corpus."""
    url = f"https://data.commoncrawl.org/crawl-data/{corpus}/warc.paths.gz"
    resp = requests.get(url)
    resp.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as f:
        return f.read().decode("utf-8").splitlines()


def extract_text_from_warc(warc_url: str, min_length: int, max_length: int):
    """Download a WARC and yield (text, url) tuples — raw, no quality filtering."""
    try:
        response = requests.get(warc_url, stream=True, timeout=(30, 120))
        response.raise_for_status()
    except Exception as e:
        print(f"    Failed to download {warc_url}: {e}", flush=True)
        return

    for record in ArchiveIterator(response.raw):
        if record.headers.get("WARC-Type") != "response":
            continue
        try:
            html = record.reader.read()
            try:
                html_decoded = html.decode("utf-8")
            except UnicodeDecodeError:
                html_decoded = html.decode(detect_encoding(html), errors="ignore")
            text = extract_plain_text(html_decoded, main_content=True)
            if min_length <= len(text) <= max_length:
                yield {
                    "text": text,
                    "url": record.headers.get("WARC-Target-URI", ""),
                }
        except Exception:
            continue


def sample_raw_cc(
    corpus: str,
    lid_model,
    languages: set[str],
    samples_per_lang: int,
    min_length: int,
    max_length: int,
    max_warcs: int = 50,
):
    """Sample raw CC documents balanced across languages using reservoir sampling."""
    warc_paths = get_warc_paths(corpus)
    random.shuffle(warc_paths)
    warc_paths = warc_paths[:max_warcs]

    # Per-language reservoirs
    reservoirs = {lang: [] for lang in languages}
    counts = {lang: 0 for lang in languages}
    all_done = False

    for i, warc_path in enumerate(warc_paths):
        if all_done:
            break
        warc_url = f"https://data.commoncrawl.org/{warc_path}"
        print(f"  WARC {i + 1}/{len(warc_paths)}: {warc_path}", flush=True)

        for doc in extract_text_from_warc(warc_url, min_length, max_length):
            # Detect language
            pred = lid_model.predict(doc["text"].replace("\n", " "))
            lang = pred[0][0].removeprefix("__label__")
            conf = pred[1][0]
            if lang not in languages or conf < 0.8:
                continue

            counts[lang] += 1
            n = counts[lang]
            if n <= samples_per_lang:
                reservoirs[lang].append(doc)
            else:
                j = random.randint(0, n - 1)
                if j < samples_per_lang:
                    reservoirs[lang][j] = doc

        # Status
        status = ", ".join(
            f"{l}={len(reservoirs[l])}" for l in sorted(languages) if reservoirs[l]
        )
        print(f"    Reservoirs: {status}", flush=True)

        # Check if all languages have enough
        if all(len(reservoirs[l]) >= samples_per_lang for l in languages):
            print("  All languages have enough samples!", flush=True)
            all_done = True

    return reservoirs


def sample_positive_fineweb2(lang, fw2_name, n, min_length, max_length):
    """Sample n documents from FineWeb2 for a given language."""
    print(f"  Sampling {n:,} from FineWeb2/{fw2_name}...", flush=True)
    ds = load_dataset(
        "HuggingFaceFW/fineweb-2",
        name=fw2_name,
        split="train",
        streaming=True,
    )
    filtered = (
        item
        for item in ds
        if item.get("text") and min_length <= len(item["text"]) <= max_length
    )
    return reservoir_sample(filtered, n, desc=f"FW2-{lang}")


def sample_positive_dclm(n, min_length, max_length):
    """Sample n documents from DCLM-baseline (English only)."""
    print(f"  Sampling {n:,} from DCLM-baseline...", flush=True)
    ds = load_dataset(
        "mlfoundations/dclm-baseline-1.0",
        split="train",
        streaming=True,
    )
    filtered = (
        item
        for item in ds
        if item.get("text") and min_length <= len(item["text"]) <= max_length
    )
    return reservoir_sample(filtered, n, desc="DCLM-en")


def load_lid_model():
    """Load fastText language identification model."""
    import tempfile
    from pathlib import Path

    cache_path = Path(tempfile.gettempdir()) / "lid.176.bin"
    if not cache_path.exists():
        print("Downloading language detection model...", flush=True)
        resp = requests.get(LID_MODEL_URL)
        resp.raise_for_status()
        cache_path.write_bytes(resp.content)
    return fasttext.load_model(str(cache_path))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--samples-per-lang",
        type=int,
        default=20000,
        help="Positive AND negative samples per language",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--min-length", type=int, default=200)
    parser.add_argument("--max-length", type=int, default=10000)
    parser.add_argument(
        "--languages",
        nargs="+",
        default=list(LANG_TO_FW2.keys()),
    )
    parser.add_argument(
        "--cc-corpus",
        default="CC-MAIN-2024-10",
        help="Common Crawl corpus for negative samples",
    )
    parser.add_argument(
        "--max-warcs",
        type=int,
        default=100,
        help="Max WARC files to scan for negative samples",
    )
    args = parser.parse_args()

    random.seed(args.seed)
    all_docs = []

    # =============================================
    # POSITIVE SAMPLES: FineWeb2 + DCLM
    # =============================================
    print("\n" + "=" * 60)
    print("SAMPLING POSITIVES (curated datasets)")
    print("=" * 60)

    for lang in args.languages:
        fw2_name = LANG_TO_FW2.get(lang)
        if not fw2_name:
            continue

        if lang == "en":
            n_fw2 = args.samples_per_lang // 2
            n_dclm = args.samples_per_lang - n_fw2
        else:
            n_fw2 = args.samples_per_lang
            n_dclm = 0

        print(f"\n--- {lang} ({fw2_name}) ---")

        # FineWeb2
        try:
            samples = sample_positive_fineweb2(
                lang, fw2_name, n_fw2, args.min_length, args.max_length
            )
            for item in samples:
                all_docs.append(
                    {
                        "text": item["text"],
                        "label": "HIGH",
                        "source": "fineweb2",
                        "language": lang,
                    }
                )
            print(f"    -> {len(samples):,} FineWeb2 docs")
        except Exception as e:
            print(f"    ERROR FineWeb2/{fw2_name}: {e}")

        # DCLM (English only)
        if n_dclm > 0:
            try:
                samples = sample_positive_dclm(
                    n_dclm, args.min_length, args.max_length
                )
                for item in samples:
                    all_docs.append(
                        {
                            "text": item["text"],
                            "label": "HIGH",
                            "source": "dclm",
                            "language": "en",
                        }
                    )
                print(f"    -> {len(samples):,} DCLM docs")
            except Exception as e:
                print(f"    ERROR DCLM: {e}")

    # =============================================
    # NEGATIVE SAMPLES: Raw unfiltered CC
    # =============================================
    print("\n" + "=" * 60)
    print("SAMPLING NEGATIVES (raw Common Crawl)")
    print("=" * 60)

    lid_model = load_lid_model()
    cc_reservoirs = sample_raw_cc(
        corpus=args.cc_corpus,
        lid_model=lid_model,
        languages=set(args.languages),
        samples_per_lang=args.samples_per_lang,
        min_length=args.min_length,
        max_length=args.max_length,
        max_warcs=args.max_warcs,
    )

    for lang, docs in cc_reservoirs.items():
        for doc in docs:
            all_docs.append(
                {
                    "text": doc["text"],
                    "label": "LOW",
                    "source": "cc_raw",
                    "language": lang,
                }
            )
        if docs:
            print(f"  {lang}: {len(docs):,} raw CC docs")

    # =============================================
    # SUMMARY & WRITE
    # =============================================
    random.shuffle(all_docs)
    high = sum(1 for d in all_docs if d["label"] == "HIGH")
    low = len(all_docs) - high

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {len(all_docs):,} docs")
    print(f"  HIGH: {high:,} ({high / max(len(all_docs), 1) * 100:.1f}%)")
    print(f"  LOW:  {low:,} ({low / max(len(all_docs), 1) * 100:.1f}%)")

    by_source = defaultdict(int)
    for d in all_docs:
        by_source[f"{d['source']}/{d['label']}"] += 1
    for src, cnt in sorted(by_source.items()):
        print(f"  {src}: {cnt:,}")

    print(f"\nPer-language:")
    by_lang = defaultdict(lambda: {"HIGH": 0, "LOW": 0})
    for d in all_docs:
        by_lang[d["language"]][d["label"]] += 1
    for lang in sorted(by_lang):
        h, lo = by_lang[lang]["HIGH"], by_lang[lang]["LOW"]
        print(f"  {lang}: HIGH={h:,} LOW={lo:,}")

    with open(args.output, "w") as f:
        for doc in all_docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    main()
