import contextlib
import io
import json
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import zstandard as zstd
from datasketch import MinHashLSH
from tqdm import tqdm

from dactory import compute_minhash_signature

BATCH_SIZE = 2048

# Worker state — set once per child process by _init_worker
_NUM_PERM = None


def _init_worker(num_perm):
    global _NUM_PERM
    _NUM_PERM = num_perm


def _compute_sig(text):
    return compute_minhash_signature(text, _NUM_PERM, 5)


@contextlib.contextmanager
def _open_reader(filepath):
    if filepath.name.endswith(".jsonl.zstd"):
        with filepath.open("rb") as raw:
            reader = zstd.ZstdDecompressor().stream_reader(raw)
            yield io.TextIOWrapper(reader, encoding="utf-8")
    else:
        with filepath.open(encoding="utf-8") as f:
            yield f


@contextlib.contextmanager
def _open_writer(filepath):
    if filepath.name.endswith(".jsonl.zstd"):
        from .zstd_writer import zstd_writer

        with zstd_writer(filepath) as f:
            yield f
    else:
        with filepath.open("wb") as f:
            yield f


def _iter_batches(reader, batch_size):
    batch = []
    for line in reader:
        batch.append(line)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _find_files(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("*.jsonl.zstd")) + sorted(input_dir.glob("*.jsonl"))


def _sig_basename(filepath: Path) -> str:
    """shard_00.jsonl -> shard_00, 0.jsonl.zstd -> 0"""
    name = filepath.name
    for ext in (".jsonl.zstd", ".jsonl"):
        if name.endswith(ext):
            return name[: -len(ext)]
    return filepath.stem


def compute_signatures(
    input_dir: Path,
    sigs_dir: Path,
    num_perm: int = 112,
    shard: int | None = None,
    workers: int = 4,
) -> None:
    """Pre-compute MinHash signatures. Use --shard for Slurm array jobs."""
    files = _find_files(input_dir)
    if not files:
        print(f"No .jsonl or .jsonl.zstd files found in {input_dir}")
        return

    if shard is not None:
        if shard >= len(files):
            print(f"Shard {shard} out of range (0-{len(files) - 1})")
            return
        files = [files[shard]]

    sigs_dir.mkdir(parents=True, exist_ok=True)

    with Pool(workers, initializer=_init_worker, initargs=(num_perm,)) as pool:
        for filepath in files:
            sigs = []
            pbar = tqdm(desc=filepath.name, unit=" docs", unit_scale=True)

            with _open_reader(filepath) as reader:
                for batch in _iter_batches(reader, BATCH_SIZE):
                    texts = [json.loads(line)["text"] for line in batch]
                    batch_sigs = pool.map(_compute_sig, texts, chunksize=64)
                    sigs.extend(batch_sigs)
                    pbar.update(len(batch))

            pbar.close()
            sig_path = sigs_dir / f"{_sig_basename(filepath)}.sigs.npy"
            np.save(sig_path, np.array(sigs, dtype=np.uint64))
            print(f"  {filepath.name}: {len(sigs):,} signatures -> {sig_path.name}")


def _get_band_range(band: int, threshold: float, num_perm: int) -> tuple[int, int]:
    """Get the hash range (start, end) for a given LSH band."""
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    return lsh.hashranges[band]


def _find_sig_files(sigs_dir: Path) -> list[Path]:
    return sorted(sigs_dir.glob("*.sigs.npy"))


def find_band_duplicates(
    sigs_dir: Path, dupes_dir: Path, band: int, threshold: float = 0.75, num_perm: int = 112
) -> None:
    """Find duplicate documents for a single LSH band. Writes band_{band}.dupes.npy."""
    start, end = _get_band_range(band, threshold, num_perm)
    sig_files = _find_sig_files(sigs_dir)
    if not sig_files:
        print(f"No .sigs.npy files found in {sigs_dir}")
        return

    dupes_dir.mkdir(parents=True, exist_ok=True)
    seen: dict[bytes, int] = {}
    duplicates: list[int] = []
    global_idx = 0

    pbar = tqdm(desc=f"Band {band} [{start}:{end}]", unit=" docs", unit_scale=True)
    for sig_file in sig_files:
        sigs = np.load(sig_file)
        for i in range(len(sigs)):
            band_hash = sigs[i, start:end].tobytes()
            if band_hash in seen:
                duplicates.append(global_idx)
            else:
                seen[band_hash] = global_idx
            global_idx += 1
        pbar.update(len(sigs))
        tqdm.write(
            f"  {sig_file.name}: {len(sigs):,} docs, {len(duplicates):,} duplicates so far"
        )

    pbar.close()
    out_path = dupes_dir / f"band_{band}.dupes.npy"
    np.save(out_path, np.array(duplicates, dtype=np.int64))
    print(
        f"\nBand {band}: {global_idx:,} docs, {len(duplicates):,} duplicates"
        f" ({len(duplicates) / global_idx * 100:.1f}%) -> {out_path.name}"
    )


def _load_dupe_set(dupes_dir: Path) -> set[int]:
    """Load and merge all per-band duplicate sets."""
    dupe_set: set[int] = set()
    for dupes_file in sorted(dupes_dir.glob("band_*.dupes.npy")):
        band_dupes = np.load(dupes_file)
        before = len(dupe_set)
        dupe_set.update(band_dupes.tolist())
        print(
            f"  {dupes_file.name}: {len(band_dupes):,} duplicates ({len(dupe_set) - before:,} new)"
        )
    print(f"Total unique duplicates: {len(dupe_set):,}")
    return dupe_set


def _compute_shard_offsets(sigs_dir: Path) -> list[tuple[Path, int]]:
    """Compute the global doc offset for each sig file, returned as (sig_path, offset) pairs."""
    sig_files = _find_sig_files(sigs_dir)
    offsets = []
    offset = 0
    for sig_file in sig_files:
        # Read only the header to get the shape without loading data
        sigs = np.load(sig_file, mmap_mode="r")
        offsets.append((sig_file, offset))
        offset += len(sigs)
    return offsets


def filter_duplicates(
    input_dir: Path,
    output_dir: Path,
    dupes_dir: Path,
    sigs_dir: Path | None = None,
    shard: int | None = None,
) -> None:
    """Filter documents using pre-computed per-band duplicate sets.

    If sigs_dir and shard are provided, only process that shard (for parallel Slurm jobs).
    The sigs_dir is used to compute global doc offsets per shard.
    """
    files = _find_files(input_dir)
    if not files:
        print(f"No .jsonl or .jsonl.zstd files found in {input_dir}")
        return

    dupe_set = _load_dupe_set(dupes_dir)

    if shard is not None:
        if sigs_dir is None:
            raise ValueError("--sigs-dir is required when using --shard")
        shard_offsets = _compute_shard_offsets(sigs_dir)
        if shard >= len(files):
            print(f"Shard {shard} out of range (0-{len(files) - 1})")
            return
        files = [files[shard]]
        global_idx = shard_offsets[shard][1]
    else:
        global_idx = 0

    output_dir.mkdir(parents=True, exist_ok=True)
    total_docs = 0
    total_kept = 0

    pbar = tqdm(desc="Filtering", unit=" docs", unit_scale=True)
    for filepath in files:
        file_docs = 0
        file_kept = 0
        out_path = output_dir / filepath.name
        pbar.set_postfix(file=filepath.name, refresh=False)

        with _open_reader(filepath) as reader, _open_writer(out_path) as out_f:
            for line in reader:
                file_docs += 1
                if global_idx not in dupe_set:
                    file_kept += 1
                    out_f.write(line.encode("utf-8") if isinstance(line, str) else line)
                global_idx += 1
                if file_docs % BATCH_SIZE == 0:
                    pbar.update(BATCH_SIZE)
            pbar.update(file_docs % BATCH_SIZE)

        total_docs += file_docs
        total_kept += file_kept
        tqdm.write(
            f"  {filepath.name}: {file_docs:,} docs -> {file_kept:,} kept"
            f" ({file_docs - file_kept:,} duplicates)"
        )

    pbar.close()
    duplicates = total_docs - total_kept
    pct = (duplicates / total_docs * 100) if total_docs else 0
    print(
        f"\nTotal: {total_docs:,} docs, {total_kept:,} kept,"
        f" {duplicates:,} duplicates removed ({pct:.1f}%)"
    )


