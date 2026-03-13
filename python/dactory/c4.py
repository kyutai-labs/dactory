from dataclasses import dataclass

from dactory import compute_c4_metrics


@dataclass
class C4Config:
    drop_curly_brackets: bool = True
    drop_javascript: bool = True
    drop_lorem_ipsum: bool = True
    min_num_sentences: float = 5.0
    max_policy_ratio: float = 0.1


def passes_c4_filters(text: str, config: C4Config) -> tuple[bool, dict[str, float]]:
    metrics = compute_c4_metrics(text)

    if config.drop_curly_brackets and metrics["has_curly_brackets"] > 0.5:
        return False, metrics
    if config.drop_javascript and metrics["has_javascript"] > 0.5:
        return False, metrics
    if config.drop_lorem_ipsum and metrics["has_lorem_ipsum"] > 0.5:
        return False, metrics
    if metrics["num_sentences"] < config.min_num_sentences:
        return False, metrics
    if metrics["policy_ratio"] > config.max_policy_ratio:
        return False, metrics

    return True, metrics
