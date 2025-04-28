from pathlib import Path

import requests

CACHE_DIRECTORY = Path.home() / ".cache" / "dactory"


def download_if_necessary(path_or_url: str) -> Path:
    if not path_or_url.startswith("http"):
        return Path(path_or_url)
    CACHE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    url = path_or_url
    filename = url.split("/")[-1]
    local_path = CACHE_DIRECTORY / filename
    if local_path.exists():
        return local_path
    tmp_path = local_path.with_suffix(".tmp")
    # model might be big, we stream to disk
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with tmp_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    tmp_path.rename(local_path)
    return local_path
