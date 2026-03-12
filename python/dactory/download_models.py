from pathlib import Path

import requests
from huggingface_hub import hf_hub_download

CACHE_DIRECTORY = Path.home() / ".cache" / "dactory"
HF_PREFIX = "hf://"


def download_if_necessary(path_or_url: str) -> Path:
    if path_or_url.startswith(HF_PREFIX):
        path_or_url = path_or_url.removeprefix(HF_PREFIX)
        splitted = path_or_url.split("/")
        repo_id = "/".join(splitted[:2])
        filename = "/".join(splitted[2:])
        return Path(hf_hub_download(repo_id=repo_id, filename=filename))
    elif path_or_url.startswith("https://"):
        CACHE_DIRECTORY.mkdir(parents=True, exist_ok=True)
        filename = path_or_url.removeprefix("https://").replace("/", "_")[-30:]
        local_path = CACHE_DIRECTORY / filename
        # stream as it might be large
        with requests.get(path_or_url, stream=True) as r:
            r.raise_for_status()
            with local_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_path
    else:
        return Path(path_or_url)
