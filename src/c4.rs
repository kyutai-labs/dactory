use pyo3::prelude::*;
use std::collections::HashMap;

const POLICY_TERMS: &[&str] = &[
    "terms of use",
    "cookie policy",
    "privacy policy",
    "copyright",
];

fn is_terminal_punctuation(b: u8) -> bool {
    matches!(b, b'.' | b'!' | b'?' | b'"')
}

#[pyfunction]
pub fn compute_c4_metrics(text: &str) -> HashMap<String, f32> {
    let mut metrics = HashMap::new();
    let lower = text.to_lowercase();

    let has_curly = if text.as_bytes().iter().any(|&b| b == b'{') {
        1.0
    } else {
        0.0
    };
    let has_js = if lower.contains("javascript") {
        1.0
    } else {
        0.0
    };
    let has_lorem = if lower.contains("lorem ipsum") {
        1.0
    } else {
        0.0
    };

    let mut line_count: usize = 0;
    let mut sentence_count: usize = 0;
    let mut policy_line_count: usize = 0;

    for line in text.lines() {
        line_count += 1;
        let trimmed = line.trim_end();
        if let Some(&last) = trimmed.as_bytes().last() {
            if is_terminal_punctuation(last) {
                sentence_count += 1;
            }
        }
    }

    for lower_line in lower.lines() {
        if POLICY_TERMS.iter().any(|term| lower_line.contains(term)) {
            policy_line_count += 1;
        }
    }

    let policy_ratio = if line_count > 0 {
        policy_line_count as f32 / line_count as f32
    } else {
        0.0
    };

    metrics.insert("has_curly_brackets".to_string(), has_curly);
    metrics.insert("has_javascript".to_string(), has_js);
    metrics.insert("has_lorem_ipsum".to_string(), has_lorem);
    metrics.insert("num_sentences".to_string(), sentence_count as f32);
    metrics.insert("policy_ratio".to_string(), policy_ratio);

    metrics
}
