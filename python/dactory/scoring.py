from tqdm import tqdm

from dactory import FastTextPyWrapper
from dactory.download_models import download_if_necessary


def get_model_path_for_lang(models_directory: str, lang: str) -> str:
    return models_directory.removesuffix("/") + f"/filter_{lang}.bin"


class ScoringModels:
    """We load the models lazily for a better UX."""

    def __init__(
        self, models_directory: str, languages_supported: list[str], load_models_early: bool
    ):
        self.models_directory = models_directory
        self.languages_supported = languages_supported
        self.models = {}
        if load_models_early:
            self.load_all_models()

    def load_all_models(self):
        for lang in self.languages_supported:
            self.load_model_if_necessary(lang)

    def load_model_if_necessary(self, lang: str):
        if lang in self.models:
            return
        model_path_or_url = get_model_path_for_lang(self.models_directory, lang)
        model_path = download_if_necessary(model_path_or_url)
        if not model_path.exists():
            raise FileNotFoundError(f"Model for {lang} not found at {model_path}")
        tqdm.write(f"Loading scoring model for language: {lang}")
        self.models[lang] = FastTextPyWrapper.load(str(model_path))

    def get_doc_scores(self, text: str, lang: str) -> dict[str, float]:
        if lang not in self.languages_supported:
            raise ValueError(f"Language {lang} not supported")
        self.load_model_if_necessary(lang)
        return self.models[lang].get_doc_annotations(text)


def get_scoring_models(
    path_or_url: str, languages: list[str], load_models_early: bool
) -> ScoringModels | None:
    if path_or_url.lower() == "none":
        return None
    return ScoringModels(path_or_url, languages, load_models_early)
