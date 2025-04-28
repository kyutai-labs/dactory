import gzip
import io
from collections import defaultdict

import requests

URL_TEMPLATE = "https://data.commoncrawl.org/crawl-data/{}/warc.paths.gz"


def get_group_idx(warc_path: str) -> int:
    return int(warc_path.split("/")[3].split(".")[1])


def get_warc_groups(corpus: str) -> list[list[str]]:
    corpus_url = URL_TEMPLATE.format(corpus)
    compressed_warc_paths = requests.get(corpus_url)
    compressed_warc_paths.raise_for_status()
    groups = defaultdict(list)

    # we load directly the text in memory
    file_like = io.BytesIO(compressed_warc_paths.content)
    with gzip.GzipFile(fileobj=file_like) as f:
        paths = f.read().decode("utf-8").splitlines()
    for path in paths:
        groups[get_group_idx(path)].append(path)

    # sort each group
    for group in groups.values():
        group.sort()
    # sort all groups
    sorted_groups = sorted(groups.items())

    # We check that the groups have numbers from 0 to n
    for i, group in enumerate(sorted_groups):
        if group[0] != i:
            raise ValueError(
                f"Group {i} not found in the corpus. Found {group[0]} instead. The corpus might be incomplete."
            )

    return [paths for _, paths in sorted_groups]
