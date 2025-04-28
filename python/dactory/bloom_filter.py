from dactory import BloomFilter
from dactory.download_models import download_if_necessary


def load_bloom_filter(bloom_filter_path_or_url: str) -> BloomFilter | None:
    if bloom_filter_path_or_url.lower() == "none":
        return None
    return BloomFilter.py_load(str(download_if_necessary(bloom_filter_path_or_url)))
