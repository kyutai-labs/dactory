use pyo3::prelude::*;
use std::collections::HashMap;

const STOP_WORDS: &[(&str, &[&str])] = &[
    (
        "en",
        &[
            "the", "be", "to", "of", "and", "that", "have", "with", "this", "will", "your", "from",
            "they", "been", "have", "many", "some",
        ],
    ),
    (
        "fr",
        &[
            "le", "de", "un", "être", "et", "en", "avoir", "que", "pour", "dans", "ce", "son",
            "une", "sur", "avec",
        ],
    ),
    (
        "de",
        &[
            "der", "die", "und", "den", "von", "ist", "des", "das", "auf", "für", "dem", "mit",
            "als", "sich", "ein",
        ],
    ),
    (
        "es",
        &[
            "de", "la", "que", "el", "en", "los", "del", "las", "con", "una", "por", "para",
            "como", "pero", "más",
        ],
    ),
    (
        "it",
        &[
            "di", "che", "non", "per", "una", "del", "con", "sono", "gli", "dei", "anche", "nel",
            "della", "più", "suo",
        ],
    ),
    (
        "pt",
        &[
            "de", "que", "não", "uma", "com", "para", "por", "dos", "das", "mais", "como", "mas",
            "foi", "seu", "também",
        ],
    ),
    (
        "nl",
        &[
            "de", "van", "een", "het", "dat", "die", "voor", "met", "zijn", "ook", "als", "niet",
            "aan", "nog", "maar",
        ],
    ),
    (
        "pl",
        &[
            "nie", "się", "jest", "na", "to", "do", "jak", "ale", "czy", "tym", "tak", "jego",
            "już", "był", "tego",
        ],
    ),
    (
        "sv",
        &[
            "och", "att", "det", "som", "för", "den", "med", "var", "inte", "har", "till", "kan",
            "från", "alla", "ett",
        ],
    ),
    (
        "da",
        &[
            "og", "det", "den", "til", "som", "med", "for", "har", "der", "kan", "ikke", "fra",
            "var", "også", "efter",
        ],
    ),
    (
        "fi",
        &[
            "oli", "sen", "kun", "hän", "että", "mutta", "niin", "ovat", "olla", "myös", "tai",
            "kuin", "vain", "sekä", "jos",
        ],
    ),
    (
        "cs",
        &[
            "byl", "jeho", "jako", "ale", "pro", "jsou", "byl", "tak", "jsem", "není", "jen",
            "než", "být", "při", "jej",
        ],
    ),
    (
        "sk",
        &[
            "bol", "jeho", "ako", "ale", "pre", "nie", "tak", "som", "len", "pri", "ich", "aby",
            "bolo", "ešte", "jej",
        ],
    ),
    (
        "bg",
        &[
            "на",
            "за",
            "от",
            "по",
            "да",
            "се",
            "не",
            "със",
            "при",
            "което",
            "тя",
            "бе",
            "как",
            "все",
            "има",
        ],
    ),
    (
        "el",
        &[
            "και", "του", "της", "για", "που", "από", "στο", "τον", "στη", "με", "δεν", "τα", "θα",
            "ότι", "στην",
        ],
    ),
    (
        "et",
        &[
            "oli", "kui", "aga", "ning", "mis", "selle", "see", "oma", "tema", "kes", "veel",
            "nii", "siis", "või", "kuid",
        ],
    ),
    (
        "ga",
        &[
            "agus", "bhí", "sin", "air", "ach", "leis", "ann", "bhi", "mar", "ina", "ach", "gur",
            "chuig", "atá", "mór",
        ],
    ),
    (
        "hr",
        &[
            "bio", "njegov", "kao", "ali", "ima", "nisu", "tako", "sam", "samo", "pri", "biti",
            "još", "kako", "sve", "već",
        ],
    ),
    (
        "hu",
        &[
            "volt", "nem", "már", "meg", "csak", "egy", "hogy", "még", "mint", "nagy", "kell",
            "van", "lett", "igen", "más",
        ],
    ),
    (
        "lt",
        &[
            "yra", "kad", "tai", "bet", "jis", "buvo", "nuo", "dar", "kaip", "tik", "arba", "jau",
            "nes", "jos", "kas",
        ],
    ),
    (
        "lv",
        &[
            "bija", "kas", "bet", "tas", "nav", "tad", "kur", "jau", "gan", "arī", "vēl", "lai",
            "tik", "par", "vai",
        ],
    ),
    (
        "mt",
        &[
            "kien", "dan", "mill", "għal", "fost", "minn", "bejn", "dak", "kull", "dawn", "aktar",
            "fuq", "wara", "qabel", "kienu",
        ],
    ),
    (
        "ro",
        &[
            "este", "lui", "lor", "fost", "din", "mai", "care", "pentru", "sunt", "dar", "sau",
            "cum", "într", "prin", "doar",
        ],
    ),
    (
        "sl",
        &[
            "bil", "njegov", "kot", "ali", "ima", "niso", "tako", "sem", "samo", "pri", "biti",
            "več", "kako", "vse", "še",
        ],
    ),
];

fn get_stop_words(language: &str) -> Option<&'static [&'static str]> {
    STOP_WORDS
        .iter()
        .find(|(lang, _)| *lang == language)
        .map(|(_, words)| *words)
}

fn is_bullet_start(line: &str) -> bool {
    let trimmed = line.trim_start();
    if trimmed.is_empty() {
        return false;
    }
    let first = trimmed.as_bytes()[0];
    if first == b'-' || first == b'*' || first == b'+' {
        return true;
    }
    // Check for "digit." pattern
    let mut i = 0;
    let bytes = trimmed.as_bytes();
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    i > 0 && i < bytes.len() && bytes[i] == b'.'
}

fn is_end_punctuation(c: u8) -> bool {
    matches!(c, b'.' | b'!' | b'?' | b';' | b':')
}

#[pyfunction]
pub fn compute_gopher_metrics(text: &str, language: &str) -> HashMap<String, f32> {
    let mut metrics = HashMap::new();

    // Word-level metrics
    let mut total_word_len: usize = 0;
    let mut word_count: usize = 0;
    let mut words_with_alpha: usize = 0;

    for word in text.split_whitespace() {
        word_count += 1;
        total_word_len += word.len();
        if word.chars().any(|c| c.is_alphabetic()) {
            words_with_alpha += 1;
        }
    }

    let mean_word_length = if word_count > 0 {
        total_word_len as f32 / word_count as f32
    } else {
        0.0
    };
    let frac_words_with_alpha = if word_count > 0 {
        words_with_alpha as f32 / word_count as f32
    } else {
        0.0
    };

    // Line-level metrics
    let lines: Vec<&str> = text.lines().collect();
    let line_count = lines.len();
    let mut lines_end_punctuation: usize = 0;
    let mut lines_start_bullet: usize = 0;

    for line in &lines {
        let trimmed = line.trim_end();
        if !trimmed.is_empty() {
            if is_end_punctuation(*trimmed.as_bytes().last().unwrap()) {
                lines_end_punctuation += 1;
            }
        }
        if is_bullet_start(line) {
            lines_start_bullet += 1;
        }
    }

    let frac_lines_end_punctuation = if line_count > 0 {
        lines_end_punctuation as f32 / line_count as f32
    } else {
        0.0
    };
    let frac_lines_start_bullet = if line_count > 0 {
        lines_start_bullet as f32 / line_count as f32
    } else {
        0.0
    };

    // Character-level metrics
    let total_chars = text.chars().count();
    let alphabetic_chars = text.chars().filter(|c| c.is_alphabetic()).count();
    let frac_alphabetic_chars = if total_chars > 0 {
        alphabetic_chars as f32 / total_chars as f32
    } else {
        0.0
    };

    // Stop words check
    let has_stop_words = if let Some(stop_words) = get_stop_words(language) {
        let lower = text.to_lowercase();
        let found = stop_words
            .iter()
            .filter(|sw| lower.split_whitespace().any(|w| w == **sw))
            .count();
        if found >= 2 {
            1.0
        } else {
            0.0
        }
    } else {
        // If no stop words for this language, pass by default
        1.0
    };

    // Duplicate sentences: split on sentence boundaries (.!?) followed by whitespace
    let frac_duplicate_sentences = compute_frac_duplicate(text, SplitMode::Sentence);
    let frac_duplicate_paragraphs = compute_frac_duplicate(text, SplitMode::Paragraph);

    metrics.insert("mean_word_length".to_string(), mean_word_length);
    metrics.insert("frac_words_with_alpha".to_string(), frac_words_with_alpha);
    metrics.insert(
        "frac_lines_end_punctuation".to_string(),
        frac_lines_end_punctuation,
    );
    metrics.insert(
        "frac_lines_start_bullet".to_string(),
        frac_lines_start_bullet,
    );
    metrics.insert("frac_alphabetic_chars".to_string(), frac_alphabetic_chars);
    metrics.insert("has_stop_words".to_string(), has_stop_words);
    metrics.insert(
        "frac_duplicate_sentences".to_string(),
        frac_duplicate_sentences,
    );
    metrics.insert(
        "frac_duplicate_paragraphs".to_string(),
        frac_duplicate_paragraphs,
    );

    metrics
}

enum SplitMode {
    Sentence,
    Paragraph,
}

fn compute_frac_duplicate(text: &str, mode: SplitMode) -> f32 {
    let chunks: Vec<&str> = match mode {
        SplitMode::Sentence => split_sentences(text),
        SplitMode::Paragraph => text
            .split("\n\n")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect(),
    };

    let total_chars: usize = chunks.iter().map(|s| s.len()).sum();
    if total_chars == 0 {
        return 0.0;
    }

    let mut seen = std::collections::HashSet::new();
    let mut duplicate_chars: usize = 0;

    for chunk in &chunks {
        if !seen.insert(*chunk) {
            duplicate_chars += chunk.len();
        }
    }

    duplicate_chars as f32 / total_chars as f32
}

fn split_sentences(text: &str) -> Vec<&str> {
    let mut sentences = Vec::new();
    let bytes = text.as_bytes();
    let mut start = 0;

    for (i, &b) in bytes.iter().enumerate() {
        if is_end_punctuation(b) {
            let end = i + 1;
            if end >= bytes.len() || bytes[end].is_ascii_whitespace() {
                let s = text[start..end].trim();
                if !s.is_empty() {
                    sentences.push(s);
                }
                start = end;
            }
        }
    }
    let s = text[start..].trim();
    if !s.is_empty() {
        sentences.push(s);
    }

    sentences
}
