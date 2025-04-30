from pathlib import Path

import requests
from huggingface_hub import hf_hub_download

CACHE_DIRECTORY = Path.home() / ".cache" / "dactory"
HF_PREFIX = "hf://"

def download_if_necessary(path_or_url: str) -> Path:
    if not path_or_url.startswith(HF_PREFIX):
        return Path(path_or_url)
    path_or_url = path_or_url.removeprefix(HF_PREFIX)
    splitted = path_or_url.split("/")
    repo_id = "/".join(splitted[:2])
    filename = "/".join(splitted[2:])
    return Path(hf_hub_download(repo_id=repo_id, filename=filename))
