import time

import fasttext
from fasttext.FastText import _FastText as FastTextModel

from dactory.download_models import download_if_necessary


def load_language_detection_model(
    lang_detection_model: str, max_retries: int = 3, retry_delay: float = 10.0
) -> FastTextModel | None:
    if lang_detection_model.lower() == "none":
        return None
    path = str(download_if_necessary(lang_detection_model))
    for attempt in range(max_retries):
        try:
            return fasttext.load_model(path)
        except ValueError as e:
            if attempt < max_retries - 1:
                print(
                    f"FastText model loading failed (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {retry_delay}s: {e}"
                )
                time.sleep(retry_delay)
            else:
                raise


def get_all_languages_available(lang_detection_model: FastTextModel) -> list[str]:
    return [x.removeprefix("__label__") for x in lang_detection_model.get_labels()]
