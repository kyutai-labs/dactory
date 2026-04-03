"""
Label documents using a vLLM-served model.

Supports two modes:
  - Quality mode (default): Binary HIGH/LOW classification
  - Edu mode (--edu): Educational value scoring on a 0-5 scale (FineWeb-Edu style)

Usage:
    # Quality mode
    python label_with_llm.py \
        --input sampled_500k.jsonl \
        --output labeled_500k.jsonl

    # Edu mode (FineWeb-Edu style)
    python label_with_llm.py --edu \
        --input sampled_500k.jsonl \
        --output labeled_edu_500k.jsonl
"""

import argparse
import asyncio
import json
import math
import re
import time
from pathlib import Path

from openai import AsyncOpenAI

QUALITY_SYSTEM_PROMPT = """\
You are a data quality classifier for language model training data.
Classify the following web document as HIGH or LOW quality.

HIGH quality: well-written, informative, educational, coherent prose, \
original content, useful knowledge, well-structured. \
Mathematical content, scientific writing, and code (tutorials, documentation, \
technical explanations, well-commented source code) are considered high quality.

LOW quality: SEO spam, boilerplate, auto-generated content, incoherent text, \
ads, cookie notices, low-effort content, listicles with no substance, \
scraped/duplicated content, gibberish, navigation menus, product listings \
with no editorial content.

Respond with a single word: HIGH or LOW"""

# FineWeb-Edu style prompt (https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu-llama3-annotations)
EDU_PROMPT_TEMPLATE = """\
Below is an extract from a web page. Evaluate whether the page has a high \
educational value and could be useful in an educational setting for teaching \
from primary school to grade school levels using the additive 5-point scoring \
system described below. Points are accumulated based on the satisfaction of each \
criterion:

- Add 1 point if the extract provides some basic information relevant to \
educational topics, even if it includes some irrelevant or non-academic content \
like advertisements and promotional material.

- Add another point if the extract addresses certain elements pertinent to \
education but does not align closely with educational standards. It might mix \
educational content with non-educational material, offering a superficial \
overview of potentially useful topics, or presenting information in a \
disorganized manner and incoherent writing style.

- Award a third point if the extract is appropriate for educational use and \
introduces key concepts relevant to school curricula. It is coherent though it \
may not be comprehensive or could include some extraneous information. It may \
resemble an introductory section of a textbook or a basic tutorial that is \
suitable for learning but has notable limitations like treating concepts that \
are too complex for grade school students.

- Grant a fourth point if the extract highly relevant and beneficial for \
educational purposes for a level not higher than grade school, exhibiting a \
clear and consistent writing style. It could be similar to a chapter from a \
textbook or a tutorial, offering substantial educational content, including \
exercises and solutions, with minimal irrelevant information, and the concepts \
aren't too advanced for grade school students. The content is coherent, focused, \
and valuable for structured learning.

- Bestow a fifth point if the extract is outstanding in its educational value, \
perfectly suited for teaching either at primary school or grade school. It \
follows detailed reasoning, the writing style is easy to follow and offers \
profound and thorough insights into the subject matter, devoid of any \
non-educational or complex content.

The extract:
{text}

After examining the extract:
- Briefly justify your total score, up to 100 words.
- Conclude with the score using the format: "Educational score: <total points>\""""


def parse_edu_score(content: str) -> int | None:
    """Extract educational score (0-5) from LLM response."""
    match = re.search(r"[Ee]ducational\s+[Ss]core:\s*(\d)", content)
    if match:
        score = int(match.group(1))
        return min(score, 5)
    # Fallback: look for a lone digit at the end
    match = re.search(r"(\d)\s*$", content.strip())
    if match:
        score = int(match.group(1))
        if score <= 5:
            return score
    return None


async def classify_document_quality(
    client: AsyncOpenAI,
    model: str,
    doc: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    async with semaphore:
        text = doc["text"]
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": QUALITY_SYSTEM_PROMPT},
                    {"role": "user", "content": text},
                ],
                max_tokens=1,
                temperature=0,
                logprobs=True,
                top_logprobs=5,
                extra_body={"chat_template_kwargs": {"enable_thinking": False}},
            )
            choice = response.choices[0]
            token = choice.message.content.strip().upper()

            # Extract logprobs for confidence
            confidence = None
            if choice.logprobs and choice.logprobs.content:
                top = choice.logprobs.content[0].top_logprobs
                probs = {}
                for entry in top:
                    key = entry.token.strip().upper()
                    if key in ("HIGH", "LOW"):
                        probs[key] = entry.logprob
                if "HIGH" in probs and "LOW" in probs:
                    p_high = math.exp(probs["HIGH"])
                    p_low = math.exp(probs["LOW"])
                    confidence = p_high / (p_high + p_low)
                elif "HIGH" in probs:
                    confidence = 1.0
                elif "LOW" in probs:
                    confidence = 0.0

            label = "HIGH" if token.startswith("HIGH") else "LOW"
            return {
                **doc,
                "label": label,
                "confidence": confidence,
                "raw_token": token,
            }
        except Exception as e:
            return {**doc, "label": "ERROR", "confidence": None, "error": str(e)}


async def classify_document_edu(
    client: AsyncOpenAI,
    model: str,
    doc: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    async with semaphore:
        text = doc["text"]
        prompt = EDU_PROMPT_TEMPLATE.format(text=text)
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
                temperature=0,
                extra_body={"chat_template_kwargs": {"enable_thinking": False}},
            )
            content = response.choices[0].message.content.strip()
            score = parse_edu_score(content)

            if score is None:
                return {
                    **doc,
                    "label": "ERROR",
                    "score": None,
                    "error": f"Could not parse score from: {content[:200]}",
                }

            return {
                **doc,
                "label": "edu_high" if score >= 3 else "edu_low",
                "score": score,
                "raw_response": content,
            }
        except Exception as e:
            return {**doc, "label": "ERROR", "score": None, "error": str(e)}


class MultiClient:
    """Round-robin across multiple OpenAI-compatible API endpoints."""

    def __init__(self, base_urls: list[str]):
        self.clients = [AsyncOpenAI(base_url=url, api_key="not-needed") for url in base_urls]
        self._counter = 0

    def next(self) -> AsyncOpenAI:
        client = self.clients[self._counter % len(self.clients)]
        self._counter += 1
        return client


async def main_async(args):
    api_bases = [url.strip() for url in args.api_base.split(",")]
    multi_client = MultiClient(api_bases)
    print(f"API endpoints: {api_bases}")

    classify_fn = classify_document_edu if args.edu else classify_document_quality
    mode_name = "edu (0-5 educational scoring)" if args.edu else "quality (HIGH/LOW)"
    print(f"Mode: {mode_name}")

    # Load input documents
    docs = []
    with open(args.input) as f:
        for line in f:
            docs.append(json.loads(line))
    print(f"Loaded {len(docs):,} documents")

    # Skip already labeled documents if output exists (for resume)
    output_path = Path(args.output)
    already_done = 0
    if output_path.exists():
        with open(output_path) as f:
            already_done = sum(1 for _ in f)
        print(f"Resuming from {already_done:,} already labeled")
        docs = docs[already_done:]

    if not docs:
        print("All documents already labeled!")
        return

    semaphore = asyncio.Semaphore(args.max_concurrent)

    t0 = time.time()
    completed = 0
    errors = 0

    # Process in batches to write incrementally
    batch_size = 1000
    with open(args.output, "a") as out_f:
        for batch_start in range(0, len(docs), batch_size):
            batch = docs[batch_start : batch_start + batch_size]
            tasks = [
                classify_fn(multi_client.next(), args.model, doc, semaphore)
                for doc in batch
            ]
            results = await asyncio.gather(*tasks)

            for result in results:
                out_f.write(json.dumps(result, ensure_ascii=False) + "\n")
                if result.get("label") == "ERROR":
                    errors += 1

            completed += len(results)
            elapsed = time.time() - t0
            rate = completed / elapsed if elapsed > 0 else 0
            total = len(docs)
            eta = (total - completed) / rate if rate > 0 else 0
            print(
                f"  {completed + already_done:,}/{total + already_done:,} "
                f"({rate:.1f} docs/sec, ETA {eta / 60:.1f}min, {errors} errors)"
            )
            out_f.flush()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed / 60:.1f} minutes ({completed / elapsed:.1f} docs/sec)")
    print(f"Errors: {errors:,}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--api-base", default="http://localhost:8000/v1")
    parser.add_argument("--model", default="Qwen/Qwen3-235B-A22B-FP8")
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=256,
        help="Max concurrent requests to vLLM",
    )
    parser.add_argument(
        "--edu",
        action="store_true",
        help="Use FineWeb-Edu style educational scoring (0-5) instead of binary HIGH/LOW",
    )
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
