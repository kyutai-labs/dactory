from contextlib import contextmanager
from pathlib import Path

import zstandard as zstd


@contextmanager
def zstd_writer(path: Path):
    with path.open("wb") as out_f:
        with zstd.ZstdCompressor().stream_writer(out_f) as output_file_compressed:
            yield output_file_compressed
