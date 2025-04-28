import io
from pathlib import Path
from typing import Self

import pydantic
import zstandard as zstd
from pydantic import BaseModel
from tqdm import tqdm

from .document import Document


class WarcProgress(BaseModel):
    last_record_seen: int = -1
    done: bool = False


class GroupProgress(BaseModel):
    """Progress within one single group."""

    persistent_path: Path  # Save periodially there to avoid data loss
    warcs_progress: dict[str, WarcProgress]

    def __getitem__(self, item: str) -> WarcProgress:
        if item not in self.warcs_progress:
            self.warcs_progress[item] = WarcProgress()
        return self.warcs_progress[item]

    def nb_records_seen(self) -> int:
        return sum(x.last_record_seen for x in self.warcs_progress.values())

    def save(self):
        """Note that the record seens may not be up to date, because we save each time a warc is done."""
        self.persistent_path.write_text(self.model_dump_json(indent=4))

    @staticmethod
    def try_to_load(path: Path) -> Self:
        """Try to load the progress file. If it doesn't exist, create a new one."""
        if path.exists():
            return GroupProgress.model_validate_json(path.read_text())
        return GroupProgress(persistent_path=path, warcs_progress={})


def rewind_old_file(
    destination_tmp_old: Path, output_file, group_idx: int, progress_file: Path
) -> GroupProgress:
    # We need to do two things:
    # 1. Find out where we stopped at each warc file
    # 2. Cleanup the old file as we probably stopped in the middle of a record
    output = GroupProgress.try_to_load(progress_file)
    if not destination_tmp_old.exists():
        return output
    try:
        with destination_tmp_old.open("rb") as in_f:
            with zstd.ZstdDecompressor().stream_reader(in_f) as in_f_decompressed:
                in_f_decompressed_text = io.TextIOWrapper(in_f_decompressed, encoding="utf-8")
                for line in in_f_decompressed_text:
                    doc = Document.model_validate_json(line)
                    output[doc.warc_file].last_record_seen = doc.record_idx
                    output_file.write(line.encode("utf-8"))
    except (pydantic.ValidationError, UnicodeDecodeError):
        # An error is expected, it's very likely we stopped in the middle of a record
        pass
    destination_tmp_old.unlink()
    tqdm.write(
        f"Resuming group {group_idx}, we's already seen {output.nb_records_seen():,} records. If this isn't what you want, abort and delete the destination directory."
    )

    return output
