"""Add edu score to existing JSONL shards using a fastText edu classifier."""

import argparse
import json
from pathlib import Path

import numpy as np

_orig_array = np.array


def _patched_array(*args, **kwargs):
    kwargs.pop("copy", None)
    return _orig_array(*args, **kwargs)


np.array = _patched_array

import fasttext


def normalize_text(text: str) -> str:
    clean = text.replace("\n", " ").replace("\r", " ")
    return " ".join(clean.split())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input JSONL file")
    parser.add_argument("--output", required=True, help="Output JSONL file")
    parser.add_argument("--model", required=True, help="Path to edu fastText model")
    args = parser.parse_args()

    model = fasttext.load_model(args.model)

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to temp file then rename for atomicity
    tmp_path = output_path.with_suffix(".tmp")
    n = 0
    with open(input_path) as fin, open(tmp_path, "w") as fout:
        for line in fin:
            doc = json.loads(line)
            text = normalize_text(doc["text"])
            labels, probs = model.predict(text, k=2)
            prob_map = {l.replace("__label__", ""): float(p) for l, p in zip(labels, probs)}
            doc["scores"]["edu"] = round(prob_map.get("edu_high", 0.0), 3)
            fout.write(json.dumps(doc, ensure_ascii=False) + "\n")
            n += 1
            if n % 500_000 == 0:
                print(f"  Processed {n:,} documents", flush=True)

    tmp_path.rename(output_path)
    print(f"Done: {n:,} documents -> {output_path}")


if __name__ == "__main__":
    main()
