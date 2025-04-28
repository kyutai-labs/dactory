import fasttext
from fasttext.FastText import _FastText as FastTextModel

from dactory.download_models import download_if_necessary


def load_language_detection_model(lang_detection_model: str) -> FastTextModel | None:
    if lang_detection_model.lower() == "none":
        return None
    return fasttext.load_model(str(download_if_necessary(lang_detection_model)))


def get_all_languages_available(lang_detection_model: FastTextModel) -> list[str]:
    return [x.removeprefix("__label__") for x in lang_detection_model.get_labels()]
