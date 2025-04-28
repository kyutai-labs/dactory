from pathlib import Path
from typing import Annotated

import fasttext
import pydantic
import typer
from typer import Argument, Option

import dactory.create
from dactory.language_detector import (
    get_all_languages_available,
    load_language_detection_model,
)
from dactory.profiling import profile
from dactory.scoring import get_scoring_models
from dactory.warc_groups import get_warc_groups

DEFAULT_LANGUAGE_DETECTOR_MODEL = (
    "/lustre/scwpod02/client/kyutai-interns/helium/dactory/lid.176.bin"
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
        bool, Option(help="Load scoring models before downloading, only useful for profiling.")
    ] = False
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
        "/lustre/scwpod02/client/kyutai-interns/helium/dactory/bloom_v2.bin"
    )
    min_bloom_threshold: Annotated[
        float, Option(help="Keep only paragraphs above the bloom threshold.")
    ] = 0.2
    scoring_models: Annotated[
        str, Option(help="Path or url of the directory containing the scoring models.")
    ] = "/lustre/scwpod02/client/kyutai-interns/helium/dactory/"
    max_rand_score: Annotated[
        float, Option(help="Filter any text that has a score for `rand` above the threshold.")
    ] = 0.9
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
