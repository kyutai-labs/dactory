import multiprocessing
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import requests
from fasttext.FastText import _FastText as FastTextModel
from fastwarc.warc import ArchiveIterator, WarcRecord
from requests.exceptions import RequestException
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.encoding import detect_encoding
from retry import retry
from tqdm import tqdm

from dactory import compute_long_words, compute_repetitions_rolling, dedup_document
from dactory.bloom_filter import load_bloom_filter
from dactory.scoring import ScoringModels
from dactory.zstd_writer import zstd_writer

from .document import Document
from .rewinding import GroupProgress, rewind_old_file

NO_MORE_INPUT = "NO_MORE_INPUT"


class UnwantedWarcRecord(Exception):
    pass


@dataclass
class WarcResults:
    warc_url: str
    success: bool
    total_records: int
    processed_records: int
    failed_records: int


@dataclass
class LoadedArgs:
    """Same as CreateArgs, but after loading and parsing the arguments."""

    destination_directory: Path
    corpus: str
    workers: int
    groups: list[int]
    warc_paths: list[list[str]]
    min_length: int
    lang_detection_model: FastTextModel | None
    languages: list[str]
    bloom_filter: str
    min_bloom_threshold: float
    scoring_models: ScoringModels | None
    max_rand_score: float
    quiet: bool


def get_record_dict(
    args: LoadedArgs, record: WarcRecord, group_idx: int, warc_file: str, record_idx: int
) -> Document:
    if record.headers["WARC-Type"] != "response":
        raise UnwantedWarcRecord("Not a response record")
    html = record.reader.read()
    try:
        # Most of the time, the encoding is utf-8, so we try it first then fallback to detect the encoding
        # if it fails, it's faster than to check the encoding every time.
        html_decoded = html.decode("utf-8")
    except UnicodeDecodeError:
        html_decoded = html.decode(detect_encoding(html), errors="ignore")
    text = extract_plain_text(html_decoded, main_content=True)
    if len(text) <= args.min_length:
        raise UnwantedWarcRecord("Text too short")
    lid = args.lang_detection_model.predict(text.replace("\n", " "))
    lid = (lid[0][0].removeprefix("__label__"), lid[1][0])
    if lid[0] == "hr":
        lid = (lid[0], 2 * lid[1])
    if lid[0] not in args.languages:
        raise UnwantedWarcRecord("Language not in the list")
    if lid[1] < 0.8:
        raise UnwantedWarcRecord("Confidence too low")
    return Document(
        text=text,
        date=record.headers["WARC-Date"],
        url=record.headers["WARC-Target-URI"],
        language=lid[0],
        language_score=round(lid[1], 3),
        warc_id=record.headers["WARC-Record-ID"],
        scores={},
        group_idx=group_idx,
        warc_file=warc_file,
        record_idx=record_idx,
        repetitions=None,  # will be filled later
        long_words=None,  # will be filled later
    )


@retry(RequestException, tries=3, delay=5)
def get_response(warc_url: str) -> requests.Response:
    # connect timeout: 30s, read timeout: 60s
    response = requests.get(warc_url, stream=True, timeout=(30, 60))
    response.raise_for_status()
    return response


def document_generator(
    args: LoadedArgs, warc_url: str, group_idx: int, work_already_done: GroupProgress
) -> Iterator[Document | WarcResults]:
    previous_work = work_already_done[warc_url]
    total_records = 0
    failed_records = 0
    processed_records = 0

    if previous_work.done:
        yield WarcResults(
            warc_url=warc_url,
            success=True,
            total_records=0,
            processed_records=0,
            failed_records=0,
        )
        return

    try:
        response = get_response(warc_url)
        for record_idx, record in enumerate(ArchiveIterator(response.raw)):
            total_records += 1
            if record_idx <= previous_work.last_record_seen:
                processed_records += 1
                continue
            try:
                yield get_record_dict(args, record, group_idx, warc_url, record_idx)
                processed_records += 1
            except UnwantedWarcRecord:
                failed_records += 1
                continue

        yield WarcResults(
            warc_url=warc_url,
            success=True,
            total_records=total_records,
            processed_records=processed_records,
            failed_records=failed_records,
        )
    except Exception:
        # Either an error occured whiling getting the WARC URL or during the streaming
        # This URL won't be mark as done yet and will be retry on next run starting from last_record_seen
        yield WarcResults(
            warc_url=warc_url,
            success=False,
            total_records=total_records,
            processed_records=processed_records,
            failed_records=failed_records,
        )


def document_generator_queue(
    args: LoadedArgs,
    input_queue,
    results_queue,
    group_idx: int,
    work_already_done: GroupProgress,
):
    time.sleep(random.uniform(0, 10))
    for warc_path in iter(input_queue.get, NO_MORE_INPUT):
        warc_url = f"https://data.commoncrawl.org/{warc_path}"
        for result in document_generator(args, warc_url, group_idx, work_already_done):
            results_queue.put(result)


def document_generator_group(
    args: LoadedArgs, warc_paths: list[str], group_idx: int, work_already_done: GroupProgress
) -> Iterator[Document | WarcResults]:
    # Since we mutate it in another function, to be sure
    work_already_done = work_already_done.copy()
    input_queue = multiprocessing.Queue()
    results_queue = multiprocessing.SimpleQueue()

    # Tracking stats
    total_warc_files = len(warc_paths)
    failed_warc_files = 0
    total_records_seen = 0
    total_records_processed = 0
    total_records_failed = 0

    processes = []
    for _ in range(args.workers):
        p = multiprocessing.Process(
            target=document_generator_queue,
            args=(args, input_queue, results_queue, group_idx, work_already_done),
        )
        p.start()
        processes.append(p)

    for warc_path in warc_paths:
        input_queue.put(warc_path)
    for _ in range(args.workers):
        input_queue.put(NO_MORE_INPUT)

    progress_bar = tqdm(
        total=len(warc_paths),
        desc="Warc paths processed in group",
        position=1,
        leave=False,
        disable=args.quiet,
    )
    while True:
        result = results_queue.get()
        if isinstance(result, WarcResults):
            total_records_seen += result.total_records
            total_records_processed += result.processed_records
            total_records_failed += result.failed_records
            if not result.success:
                failed_warc_files += 1
                if not args.quiet:
                    tqdm.write(f"Failed to download WARC: {result.warc_url}")
            progress_bar.update()

            # Log summary stats every 10 files or at the end
            if not args.quiet and (
                progress_bar.n % 10 == 0 or progress_bar.n == len(warc_paths)
            ):
                failed_records_pct = (
                    (total_records_failed / total_records_seen * 100)
                    if total_records_seen > 0
                    else 0
                )
                failed_warc_pct = failed_warc_files / total_warc_files * 100
                tqdm.write(
                    f"WARC progress: {progress_bar.n}/{total_warc_files} files "
                    f"({failed_warc_files} failed, {failed_warc_pct:.1f}%) | "
                    f"Records: {total_records_processed} processed, "
                    f"{total_records_failed} failed ({failed_records_pct:.1f}%)"
                )

            if progress_bar.n == len(warc_paths):
                break
        yield result

    for process in processes:
        process.join()
    for process in processes:
        process.terminate()


def download_warcs_for_group(args: LoadedArgs, group_idx: int, warc_paths: list[str]):
    bloom_filter = load_bloom_filter(args.bloom_filter)
    if args.languages == []:
        raise ValueError("Language list is empty")

    progress_bar_bytes = tqdm(
        unit_scale=True,
        unit="B",
        desc=f"Amount of text saved in the group {group_idx}",
        position=2,
        leave=False,
        disable=args.quiet,
    )
    progress_bar_records = tqdm(
        unit=" Records",
        desc=f"Records seen in group {group_idx}",
        position=3,
        leave=False,
        disable=args.quiet,
    )
    # fmt: off
    destination =          args.destination_directory / f"{group_idx}.jsonl.zstd"          # atomic
    destination_tmp =      args.destination_directory / f"{group_idx}.jsonl.zstd.tmp"      # for writing
    destination_tmp_old =  args.destination_directory / f"{group_idx}.jsonl.zstd.tmp.old"  # for rewinding
    destination_progress = args.destination_directory / f"{group_idx}.progress.json"       # for saving progress
    # fmt: on

    if destination.exists():
        tqdm.write(f"File {destination} already exists, skipping group {group_idx}")
        return

    if destination_tmp.exists():
        destination_tmp.rename(destination_tmp_old)

    with zstd_writer(destination_tmp) as out_f:
        work_already_done = rewind_old_file(
            destination_tmp_old, out_f, group_idx, destination_progress
        )

        for document in document_generator_group(
            args, warc_paths, group_idx, work_already_done
        ):
            if isinstance(document, WarcResults):
                # This is a WARC completion result, mark it as done if successful
                work_already_done[document.warc_url].done = document.success
                work_already_done.save()
                continue
            if bloom_filter is not None:
                document.text = dedup_document(
                    document.text, bloom_filter, args.min_bloom_threshold
                )
                if len(document.text) < args.min_length:
                    continue
            document.repetitions = compute_repetitions_rolling(document.text, 20)
            document.long_words = compute_long_words(document.text, min_length=15)

            if args.scoring_models is not None:
                scores = args.scoring_models.get_doc_scores(document.text, document.language)
                if scores["rand"] > args.max_rand_score:
                    continue
                document.scores = {k: round(v, 3) for k, v in scores.items()}

            progress_bar_bytes.update(len(document.text))
            work_already_done[document.warc_file].last_record_seen = document.record_idx
            progress_bar_records.update(
                work_already_done.nb_records_seen() - progress_bar_records.n
            )
            json_string = document.model_dump_json(by_alias=True)
            out_f.write((json_string + "\n").encode("utf-8"))

    destination_tmp.rename(destination)
    destination_progress.unlink(missing_ok=True)
    tqdm.write(f"Finished group {group_idx}")


def create_dataset(args: LoadedArgs):
    tqdm.write(f"Groups to do: {args.groups}")
    for group_idx in tqdm(
        args.groups, desc="Warc groups done", position=0, disable=args.quiet
    ):
        download_warcs_for_group(args, group_idx, args.warc_paths[group_idx])
    print(f"Groups {args.groups} done.")
