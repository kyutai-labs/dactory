from dataclasses import dataclass

from dactory import compute_gopher_metrics


# https://arxiv.org/pdf/2112.11446
@dataclass
class GopherConfig:
    min_mean_word_length: float = 3.0
    max_mean_word_length: float = 10.0
    min_frac_words_with_alpha: float = 0.8
    min_frac_lines_end_punctuation: float = 0.1
    max_frac_lines_start_bullet: float = 0.9
    min_frac_alphabetic_chars: float = 0.7
    require_stop_words: bool = True
    max_frac_duplicate_sentences: float = 0.3
    max_frac_duplicate_paragraphs: float = 0.3


def passes_gopher_filters(
    text: str, language: str, config: GopherConfig
) -> tuple[bool, dict[str, float]]:
    metrics = compute_gopher_metrics(text, language)

    if metrics["mean_word_length"] < config.min_mean_word_length:
        return False, metrics
    if metrics["mean_word_length"] > config.max_mean_word_length:
        return False, metrics
    if metrics["frac_words_with_alpha"] < config.min_frac_words_with_alpha:
        return False, metrics
    if metrics["frac_lines_end_punctuation"] < config.min_frac_lines_end_punctuation:
        return False, metrics
    if metrics["frac_lines_start_bullet"] > config.max_frac_lines_start_bullet:
        return False, metrics
    if metrics["frac_alphabetic_chars"] < config.min_frac_alphabetic_chars:
        return False, metrics
    if config.require_stop_words and metrics["has_stop_words"] < 0.5:
        return False, metrics
    if metrics["frac_duplicate_sentences"] > config.max_frac_duplicate_sentences:
        return False, metrics
    if metrics["frac_duplicate_paragraphs"] > config.max_frac_duplicate_paragraphs:
        return False, metrics

    return True, metrics
