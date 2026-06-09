"""Post-hoc filtering of existing JSONL shards.

Reads .jsonl.zstd files produced by `dactory create`, computes missing
metrics (gopher, c4, quality, edu), applies filter thresholds, and writes
filtered output to a new directory.
"""

import io
import json
from pathlib import Path

import zstandard as zstd

from dactory.c4 import C4Config, passes_c4_filters
from dactory.gopher import GopherConfig, passes_gopher_filters
from dactory.scoring import QualityClassifier
from dactory.zstd_writer import zstd_writer


def refilter_shard(
    input_path: Path,
    output_path: Path,
    *,
    enable_gopher_filters: bool = False,
    enable_c4_filters: bool = False,
    quality_classifier: QualityClassifier | None = None,
    min_quality_score: float = 0.5,
    edu_classifier: QualityClassifier | None = None,
) -> tuple[int, int]:
    """Refilter a single .jsonl.zstd shard.

    Returns (n_in, n_out).
    """
    tmp_path = output_path.parent / (output_path.name + ".tmp")
    n_in = 0
    n_out = 0

    dctx = zstd.ZstdDecompressor()

    with open(input_path, "rb") as fin:
        with dctx.stream_reader(fin) as reader:
            text_in = io.TextIOWrapper(reader, encoding="utf-8")
            with zstd_writer(tmp_path) as writer:
                for line in text_in:
                    n_in += 1
                    doc = json.loads(line)
                    text = doc["text"]

                    # C4 filters
                    if enable_c4_filters:
                        passes, c4_metrics = passes_c4_filters(text, C4Config())
                        doc["c4_metrics"] = {k: round(v, 3) for k, v in c4_metrics.items()}
                        if not passes:
                            continue

                    # Gopher filters
                    if enable_gopher_filters:
                        language = doc.get("language", "en")
                        passes, gopher_metrics = passes_gopher_filters(
                            text, language, GopherConfig()
                        )
                        doc["gopher_metrics"] = {
                            k: round(v, 3) for k, v in gopher_metrics.items()
                        }
                        if not passes:
                            continue

                    # Quality classifier
                    if quality_classifier is not None:
                        quality_scores = quality_classifier.get_quality_score(text)
                        quality_high = quality_scores.get("HIGH", 0.0)
                        doc["scores"]["quality"] = round(quality_high, 3)
                        if quality_high < min_quality_score:
                            continue

                    # Edu classifier (scoring only, no filtering)
                    if edu_classifier is not None:
                        edu_scores = edu_classifier.get_quality_score(text)
                        doc["scores"]["edu"] = round(edu_scores.get("edu_high", 0.0), 3)

                    out_line = json.dumps(doc, ensure_ascii=False) + "\n"
                    writer.write(out_line.encode("utf-8"))
                    n_out += 1

                    if n_in % 500_000 == 0:
                        pct = n_out / n_in * 100 if n_in else 0
                        print(f"  {n_in:,} read, {n_out:,} kept ({pct:.1f}%)", flush=True)

    tmp_path.rename(output_path)
    return n_in, n_out


def refilter_directory(
    input_dir: Path,
    output_dir: Path,
    *,
    shard: int | None = None,
    enable_gopher_filters: bool = False,
    enable_c4_filters: bool = False,
    quality_classifier: QualityClassifier | None = None,
    min_quality_score: float = 0.5,
    edu_classifier: QualityClassifier | None = None,
) -> None:
    """Refilter shards in a directory."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if shard is not None:
        files = [input_dir / f"{shard}.jsonl.zstd"]
        if not files[0].exists():
            print(f"Input not found: {files[0]}")
            return
    else:
        files = sorted(input_dir.glob("*.jsonl.zstd"))
        if not files:
            print(f"No .jsonl.zstd files found in {input_dir}")
            return

    total_in = 0
    total_out = 0

    for input_path in files:
        output_path = output_dir / input_path.name
        if output_path.exists():
            print(f"Output already exists: {output_path}, skipping.")
            continue

        print(f"Processing {input_path.name}...", flush=True)
        n_in, n_out = refilter_shard(
            input_path,
            output_path,
            enable_gopher_filters=enable_gopher_filters,
            enable_c4_filters=enable_c4_filters,
            quality_classifier=quality_classifier,
            min_quality_score=min_quality_score,
            edu_classifier=edu_classifier,
        )
        total_in += n_in
        total_out += n_out
        drop_pct = (1 - n_out / n_in) * 100 if n_in else 0
        print(f"  {input_path.name}: {n_in:,} -> {n_out:,} ({drop_pct:.1f}% dropped)")

    if total_in:
        drop_pct = (1 - total_out / total_in) * 100
        print(f"\nTotal: {total_in:,} -> {total_out:,} ({drop_pct:.1f}% dropped)")
