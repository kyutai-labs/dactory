import io
from collections import Counter
from pathlib import Path
from typing import Annotated

import fasttext
import pydantic
import typer
import zstandard as zstd
from typer import Argument, Option

import dactory.create
from dactory.language_detector import (
    get_all_languages_available,
    load_language_detection_model,
)
from dactory.profiling import profile
from dactory.scoring import get_edu_classifier, get_quality_classifier, get_scoring_models
from dactory.warc_groups import get_warc_groups

from .document import Document
from .download_models import HF_PREFIX

KYUTAI_HF_REPOSITORY = HF_PREFIX + "kyutai/dactory-models"

DEFAULT_LANGUAGE_DETECTOR_MODEL = (
    "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
)

# fmt: off
DEFAULT_LANGUAGES = [
    "bg", "cs", "da", "de", "el", "en",
    "es", "et", "fi", "fr", "ga", "hr", 
    "hu", "it", "lt", "lv", "mt", "nl",
    "pl", "pt", "ro", "sk", "sl", "sv",
]
# fmt: on


app = typer.Typer()


@app.command("compute-sigs")
def compute_sigs(
    input_dir: Annotated[
        Path, Argument(help="Directory containing .jsonl or .jsonl.zstd files.")
    ],
    sigs_dir: Annotated[Path, Argument(help="Directory to write signature .npy files.")],
    num_perm: Annotated[int, Option(help="Number of MinHash permutations.")] = 112,
    shard: Annotated[
        int | None, Option(help="Process only this shard index (for Slurm array jobs).")
    ] = None,
    workers: Annotated[int, Option("--workers", "-w", help="Number of processes.")] = 4,
):
    """Pre-compute MinHash signatures. Use --shard with SLURM_ARRAY_TASK_ID for parallel jobs."""
    from dactory.dedup import compute_signatures

    compute_signatures(input_dir, sigs_dir, num_perm=num_perm, shard=shard, workers=workers)


@app.command()
def dedup(
    input_dir: Annotated[
        Path, Argument(help="Directory containing .jsonl.zstd files from `dactory create`.")
    ],
    output_dir: Annotated[Path, Argument(help="Directory to write deduplicated files.")],
    threshold: Annotated[float, Option(help="MinHash LSH similarity threshold.")] = 0.75,
    num_perm: Annotated[int, Option(help="Number of MinHash permutations.")] = 112,
    workers: Annotated[
        int,
        Option(
            "--workers", "-w", help="Number of processes for parallel signature computation."
        ),
    ] = 4,
    signatures: Annotated[
        Path | None, Option(help="Directory of pre-computed signatures (from compute-sigs).")
    ] = None,
):
    """Deduplicate documents across all groups using MinHash LSH."""
    from dactory.dedup import dedup_snapshot

    dedup_snapshot(
        input_dir,
        output_dir,
        threshold=threshold,
        num_perm=num_perm,
        workers=workers,
        signatures_dir=signatures,
    )


@app.command("dedup-info")
def dedup_info(
    threshold: Annotated[float, Option(help="MinHash LSH similarity threshold.")] = 0.75,
    num_perm: Annotated[int, Option(help="Number of MinHash permutations.")] = 112,
):
    """Print LSH band configuration for a given threshold."""
    from datasketch import MinHashLSH

    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    print(f"Threshold: {threshold}, Num perm: {num_perm}")
    print(f"Bands: {lsh.b}, Rows per band: {lsh.r}")
    print(f"Slurm array: --array=0-{lsh.b - 1}")


@app.command("dedup-bands")
def dedup_bands(
    sigs_dir: Annotated[Path, Argument(help="Directory containing .sigs.npy files.")],
    dupes_dir: Annotated[Path, Argument(help="Directory to write per-band duplicate files.")],
    band: Annotated[int, Option(help="Band index to process (use with SLURM_ARRAY_TASK_ID).")],
    threshold: Annotated[float, Option(help="MinHash LSH similarity threshold.")] = 0.75,
    num_perm: Annotated[int, Option(help="Number of MinHash permutations.")] = 112,
):
    """Find duplicates for a single LSH band. Use --band with SLURM_ARRAY_TASK_ID for parallel jobs."""
    from dactory.dedup import find_band_duplicates

    find_band_duplicates(
        sigs_dir, dupes_dir, band=band, threshold=threshold, num_perm=num_perm
    )


@app.command("dedup-filter")
def dedup_filter(
    input_dir: Annotated[Path, Argument(help="Directory containing .jsonl.zstd files.")],
    output_dir: Annotated[Path, Argument(help="Directory to write deduplicated files.")],
    dupes_dir: Annotated[
        Path, Argument(help="Directory containing per-band .dupes.npy files.")
    ],
    sigs_dir: Annotated[
        Path | None, Option(help="Directory of .sigs.npy files (required with --shard).")
    ] = None,
    shard: Annotated[
        int | None, Option(help="Process only this shard (for Slurm array jobs).")
    ] = None,
):
    """Filter documents using pre-computed per-band duplicate sets."""
    from dactory.dedup import filter_duplicates

    filter_duplicates(input_dir, output_dir, dupes_dir, sigs_dir=sigs_dir, shard=shard)


@app.command()
def stats():
    file_to_load = Path("/lustre/scwpod02/client/kyutai/gabriel/dactory_test/0.jsonl.zstd")
    documents = []
    with file_to_load.open("rb") as in_f:
        with zstd.ZstdDecompressor().stream_reader(in_f) as in_f_decompressed:
            in_f_decompressed_text = io.TextIOWrapper(in_f_decompressed, encoding="utf-8")
            for line in in_f_decompressed_text:
                documents.append(Document.model_validate_json(line))

    print(f"Number of documents: {len(documents)}")
    # we get the percentage of documents in each language
    counter = Counter()
    for doc in documents:
        counter[doc.language] += 1
    total = sum(counter.values())
    for lang, count in counter.items():
        print(f"{lang}: {count / total * 100:.2f}%")


@app.command()
def list_languages(
    lang_detection_model: Annotated[
        str, Option(help="Path or url of the language detector model")
    ] = DEFAULT_LANGUAGE_DETECTOR_MODEL,
):
    """List all the languages available in the language detection model."""
    model = fasttext.load_model(lang_detection_model)
    languages = get_all_languages_available(model)
    print("Available languages: " + ",".join(languages))


@app.command("create")
class CreateArgs(pydantic.BaseModel):
    """Downloads the CommonCrawl corpus and filters the documents.
    If will save the documents in the destination directory in the format <group>.jsonl.zstd.

    If a file <group>.jsonl.zstd is complete, it will be skipped.
    The files are compressed with zstd. You can read them with `zstd -cd my_file.zstd`

    While the code uses multiprocessing within a group, the whole dataset can be created even faster by using
    xargs or slurm to run multiple groups in parallel. Use --group <group-idx> to download only one group.

    Path of files can have two different formats:
    - hf://org/repo-name/filename for huggingface files
    - https://something.com/some/file
    - /path/to/file for local files

    You can list all the languages available in the language detection model with `dactory list-languages`.
    """

    destination_directory: Annotated[
        Path,
        Argument(
            help="Directory to save the downloaded files. They will be saved at DESTINATION_DIRECTORY/<group>.jsonl.zstd"
        ),
    ]
    corpus: Annotated[str, Option("--corpus", "-c", help="The CommonCrawl corpus")] = (
        "CC-MAIN-2024-51"
    )
    load_models_early: Annotated[
        bool,
        Option(help="Load scoring models before downloading, disable for faster iteration."),
    ] = True
    workers: Annotated[
        int,
        Option(
            "--workers", "-w", help="Number of processes to download and filter the documents."
        ),
    ] = 8
    groups: Annotated[
        str,
        Option(
            "--groups",
            "-g",
            help="The groups to download and filter in the corpus. Examples: `ALL`, `28`, `10-50`, or `1,8,13`",
        ),
    ] = "ALL"
    # Filters
    min_length: Annotated[
        int,
        Option(
            help="Filter text smaller than the number of characters given. Use 0 for no filter."
        ),
    ] = 500
    lang_detection_model: Annotated[
        str, Option(help="Path or url to the language detection model.")
    ] = DEFAULT_LANGUAGE_DETECTOR_MODEL
    languages: Annotated[
        str,
        Option(
            help=(
                "A comma delimited list of languages to download. Example: `en,fr`. "
                "Use `dactory list-languages` for the full list available."
            )
        ),
    ] = ",".join(DEFAULT_LANGUAGES)
    bloom_filter: Annotated[str, Option(help="Path or url of the bloom filter model.")] = (
        f"{KYUTAI_HF_REPOSITORY}/bloom_v2.bin"
    )
    min_bloom_threshold: Annotated[
        float, Option(help="Keep only paragraphs above the bloom threshold.")
    ] = 0.2
    scoring_models: Annotated[
        str, Option(help="Path or url of the directory containing the scoring models.")
    ] = f"{KYUTAI_HF_REPOSITORY}/"
    max_rand_score: Annotated[
        float, Option(help="Filter any text that has a score for `rand` above the threshold.")
    ] = 0.9
    enable_gopher_filters: Annotated[
        bool, Option(help="Enable Gopher-style heuristic filters.")
    ] = False
    enable_c4_filters: Annotated[bool, Option(help="Enable C4-inspired content filters.")] = (
        False
    )
    enable_minhash_dedup: Annotated[
        bool, Option(help="Enable MinHash document-level deduplication.")
    ] = False
    minhash_threshold: Annotated[float, Option(help="MinHash LSH similarity threshold.")] = (
        0.75
    )
    minhash_num_perm: Annotated[int, Option(help="Number of MinHash permutations.")] = 112
    quality_classifier: Annotated[
        str, Option(help="Path/URL to DCLM fastText quality model, or 'none'.")
    ] = "none"
    edu_classifier: Annotated[
        str, Option(help="Path/URL to edu fastText quality model, or 'none'.")
    ] = "none"
    min_quality_score: Annotated[
        float, Option(help="Filter docs with quality score below this threshold.")
    ] = 0.5
    quiet: Annotated[bool, Option("--quiet", "-q", help="Do not show progress bars.")] = False

    def __init__(self, **cli_args) -> None:
        super().__init__(**cli_args)
        loaded_args = parse_args_and_load_models(self)
        dactory.create.create_dataset(loaded_args)


def get_languages(user_args: CreateArgs, lang_detection_model) -> list[str]:
    """Get the languages to download."""
    if user_args.languages == "ALL":
        languages = get_all_languages_available(lang_detection_model)
    else:
        languages = user_args.languages.split(",")
    return languages


def parse_groups_to_do(command_line_arg: str, number_of_warcs: int) -> list[int]:
    if command_line_arg == "ALL":
        return list(range(number_of_warcs))
    else:
        if "," in command_line_arg:
            return [int(x) for x in command_line_arg.split(",")]
        elif "-" in command_line_arg:
            start, end = command_line_arg.split("-")
            return list(range(int(start), int(end)))
        else:
            try:
                return [int(command_line_arg)]
            except ValueError:
                raise ValueError(
                    f"Invalid group format: {command_line_arg}. Examples: `ALL`, `28`, `10-50`, or `1,8,13`"
                )


def parse_args_and_load_models(user_args: CreateArgs) -> dactory.create.LoadedArgs:
    """Parse the command line arguments and load the models."""
    lang_detection_model = load_language_detection_model(user_args.lang_detection_model)
    languages = get_languages(user_args, lang_detection_model)

    user_args.destination_directory.mkdir(parents=True, exist_ok=True)
    warc_paths = get_warc_groups(user_args.corpus)
    groups = parse_groups_to_do(user_args.groups, len(warc_paths))

    return dactory.create.LoadedArgs(
        destination_directory=user_args.destination_directory,
        corpus=user_args.corpus,
        workers=user_args.workers,
        groups=groups,
        warc_paths=warc_paths,
        min_length=user_args.min_length,
        lang_detection_model=lang_detection_model,
        languages=languages,
        bloom_filter=user_args.bloom_filter,
        min_bloom_threshold=user_args.min_bloom_threshold,
        scoring_models=get_scoring_models(
            user_args.scoring_models, languages, user_args.load_models_early
        ),
        max_rand_score=user_args.max_rand_score,
        enable_gopher_filters=user_args.enable_gopher_filters,
        enable_c4_filters=user_args.enable_c4_filters,
        enable_minhash_dedup=user_args.enable_minhash_dedup,
        minhash_threshold=user_args.minhash_threshold,
        minhash_num_perm=user_args.minhash_num_perm,
        quality_classifier=get_quality_classifier(user_args.quality_classifier),
        min_quality_score=user_args.min_quality_score,
        edu_classifier=get_edu_classifier(user_args.edu_classifier),
        quiet=user_args.quiet,
    )


def main():
    try:
        app()
    finally:
        if profile.functions:
            profile.print_stats()


if __name__ == "__main__":
    main()
