use crate::bloom;
use crate::code;
use clap::{Parser, Subcommand};
use fasttext::FastText;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::BufRead;
use std::io::BufReader;
use std::string::String;

use pyo3::prelude::*;

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum LID {
    Score(f32),
    Pair(String, f32),
}

#[derive(Serialize, Deserialize)]
struct Document {
    text: String,
    date: Option<String>,
    url: Option<String>,
    //lid: Option<(String, f32)>,
    lid: Option<LID>,
    language: Option<String>,
    language_score: Option<f32>,
    //uniq: Option<String>,
    repetitions: Option<f32>,
    long_words: Option<f32>,
    pnum: Option<f32>,
    #[serde(rename(serialize = "warc-id", deserialize = "warc-id"))]
    warc_id: Option<String>,
    #[serde(default)]
    scores: HashMap<String, f32>,
}

#[derive(Debug)]
struct DocumentStats {
    n_long_words: u32,
    avg_word_length: u32,
    n_words: u32,
    total_length: u32,
    n_chars: u32,
    n_numpunc: u32,
}

fn compute_stats(document: &Document) -> DocumentStats {
    let mut result = DocumentStats {
        n_long_words: 0,
        avg_word_length: 0,
        n_words: 0,
        total_length: 0,
        n_chars: 0,
        n_numpunc: 0,
    };
    for token in document.text.split(char::is_whitespace) {
        if token.len() == 0 {
            continue;
        }
        if token.len() > 15 {
            result.n_long_words += 1;
        }
        result.n_words += 1;
        result.total_length += token.len() as u32;
    }
    for c in document.text.chars() {
        result.n_chars += 1;
        if c.is_ascii_punctuation() || c.is_numeric() {
            result.n_numpunc += 1;
        }
    }
    result.avg_word_length = result.total_length / std::cmp::max(1, result.n_words);
    result
}

fn round_2(x: f32) -> f32 {
    (100.0 * x).round() as f32 / 100.0
}

fn add_heuristics() -> anyhow::Result<()> {
    let buf_stdin = BufReader::new(std::io::stdin());
    for line in buf_stdin.lines() {
        let line = line?;
        let mut document: Document = serde_json::from_str(&line)?;
        let lid_score = match document.lid {
            Some(LID::Score(s)) => s,
            Some(LID::Pair(_, s)) => s,
            //Some((_, s)) => s,
            None => 1.0,
        };
        //document.rand.unwrap_or(0.0);
        let rand_score = *document.scores.get("rand").unwrap_or(&0.0);
        if lid_score < 0.85 || rand_score > 0.8 {
            continue;
        }
        let stats = compute_stats(&document);
        //let n_repetitions = compute_repetitions_rolling(&document.text, 40);
        //let p_repetitions = n_repetitions as f32 / document.text.len() as f32;
        let p_numpunc = stats.n_numpunc as f32 / stats.n_chars as f32;
        let p_long_words = stats.n_long_words as f32 / stats.n_words as f32;

        //document.repetitions = Some(round_2(p_repetitions));
        document.long_words = Some(round_2(p_long_words));
        document.pnum = Some(round_2(p_numpunc));
        println!("{}", serde_json::to_string(&document).expect("yolo"));
    }
    Ok(())
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Parser)]
struct AnnotateArgs {
    #[arg(short, long)]
    model: String,
}

#[derive(Parser)]
struct FilterArgs {
    #[arg(short, long)]
    bloom_filter: Option<String>,
    #[arg(short, long, default_value_t = 0.0)]
    threshold: f32,
    #[arg(short, long)]
    model: String,
    #[arg(short, long)]
    lang: String,
    #[arg(long, default_value_t = 1)]
    min_text_length: usize,
    #[arg(long, default_value_t = 1.0)]
    max_rand_score: f32,
}

#[derive(Parser)]
struct DedupArgs {
    #[arg(short, long)]
    bloom_filter: Option<String>,
    #[arg(short, long, default_value_t = 0.0)]
    threshold: f32,
}

#[derive(Subcommand)]
enum Commands {
    Annotate(AnnotateArgs),
    Filter(FilterArgs),
    Dedup(DedupArgs),
    Benchmark,
    Heuristics,
    TheStack,
    Data(DedupArgs),
}

fn annotate_document(document: &mut Document, model: &FastText, n_labels: usize) {
    let mut text_len = 0.0;
    let mut scores = HashMap::<String, f32>::new();
    for line in document.text.split('\n') {
        if line.len() == 0 {
            continue;
        }
        let line = line.to_owned() + "\n";
        let p = model.predict(&line, n_labels as i32, 0.0).expect("yolo");
        for i in 0..n_labels {
            let e = scores.entry(p[i].label.clone()).or_insert(0.0);
            *e += p[i].prob * (line.len() as f32);
        }
        text_len += line.len() as f32;
    }
    if text_len == 0.0 {
        return;
    }
    for (k, v) in scores {
        document
            .scores
            .insert((&k[9..]).to_string(), round_2(v / text_len));
    }
    /*for label in model.get_labels().expect("yolo").0 {
    let e = document.scores.entry(label).or_insert(0.0);
    *e /= text_len;
    }*/
    /*document.rand = Some(round_2(scores["__label__rand"] / text_len));
    document.wiki = Some(round_2(scores["__label__wiki"] / text_len));
    document.books = Some(round_2(scores["__label__books"] / text_len));
    document.science = Some(round_2(scores["__label__science"] / text_len));
    document.news = Some(round_2(scores["__label__news"] / text_len));
    document.stem = Some(round_2(scores["__label__stem"] / text_len));
    document.life = Some(round_2(scores["__label__life"] / text_len));
    document.hum = Some(round_2(scores["__label__hum"] / text_len));
    document.pop = Some(round_2(scores["__label__pop"] / text_len));*/
}

// Because of the orphan rule, it's not possible to add a trait directly
// to the fasttext struct. So we need to create a wrapper struct.
#[pyclass]
pub struct FastTextPyWrapper {
    model: FastText,
    n_labels: usize,
}

#[pymethods]
impl FastTextPyWrapper {
    #[staticmethod]
    fn load(model_path: &str) -> PyResult<Self> {
        let mut model = FastText::new();
        let _ = model.load_model(model_path);
        let n_labels = model.get_labels().expect("yolo").0.len();
        Ok(FastTextPyWrapper { model, n_labels })
    }

    fn get_doc_annotations(&self, doc_text: &str) -> HashMap<String, f32> {
        let mut final_scores = HashMap::<String, f32>::new();
        let mut text_len = 0.0;
        let mut scores = HashMap::<String, f32>::new();
        for line in doc_text.split('\n') {
            if line.len() == 0 {
                continue;
            }
            let line = line.to_owned() + "\n";
            let p = self
                .model
                .predict(&line, self.n_labels as i32, 0.0)
                .expect("yolo");
            for i in 0..self.n_labels {
                let e = scores.entry(p[i].label.clone()).or_insert(0.0);
                *e += p[i].prob * (line.len() as f32);
            }
            text_len += line.len() as f32;
        }
        if text_len == 0.0 {
            return final_scores;
        }
        for (k, v) in scores {
            final_scores.insert((&k[9..]).to_string(), round_2(v / text_len));
        }
        final_scores
    }
}

#[pyfunction]
pub fn dedup_document(
    doc_text: &str,
    bloom_filter: &mut bloom::BloomFilter,
    threshold: f32,
) -> String {
    use regex::Regex;
    let re = Regex::new(r"\n\n+").expect("Regex is correct.");
    let mut text = String::new();
    for paragraph in re.split(doc_text) {
        let mut keep = 0.0f32;
        for line in paragraph.split('\n') {
            if !bloom_filter.get(line) {
                keep += line.len() as f32;
                bloom_filter.set(line);
            }
        }
        if keep / (paragraph.len() as f32) > threshold {
            text.push_str(paragraph);
            text.push_str("\n\n");
        }
    }
    text
}

fn filter(args: FilterArgs) -> anyhow::Result<()> {
    // BloomFilter for deduplication
    let mut bloom_filter = match args.bloom_filter {
        Some(path) => bloom::BloomFilter::load(&path)?,
        None => bloom::BloomFilter::new(1 << 24, 2),
    };
    // FastText model for quality annotation
    let mut model = FastText::new();
    let _ = model.load_model(&args.model);
    let n_labels = model.get_labels().expect("yolo").0.len();

    let buf_stdin = BufReader::new(std::io::stdin());
    for document in buf_stdin.lines() {
        let document = document?;
        let mut document: Document = serde_json::from_str(&document)?;
        let (lang, lang_score) = match document.lid {
            Some(LID::Pair(ref a, s)) => (a.to_string(), s),
            Some(LID::Score(s)) => (String::from("unk"), s),
            //Some((ref a, s)) => (a.to_string(), s),
            None => (String::from("unk"), 1.0),
        };
        if lang != args.lang {
            continue;
        }
        document.language = Some(lang);
        document.language_score = Some(lang_score);
        document.text = dedup_document(&document.text, &mut bloom_filter, args.threshold);
        if document.text.len() < args.min_text_length {
            continue;
        }
        //let mut p_repetitions = compute_repetitions_rolling(&document.text, 40) as f32;
        //p_repetitions /= document.text.len() as f32;
        //document.repetitions = Some(p_repetitions);
        annotate_document(&mut document, &model, n_labels);
        if document.scores["rand"] > args.max_rand_score {
            continue;
        }
        println!("{}", serde_json::to_string(&document).expect("yolo"));
    }
    Ok(())
}

fn annotate(args: AnnotateArgs) -> anyhow::Result<()> {
    let buf_stdin = BufReader::new(std::io::stdin());
    let mut model = FastText::new();
    let _ = model.load_model(&args.model);
    let n_labels = model.get_labels().expect("yolo").0.len();

    for document in buf_stdin.lines() {
        let document = document?;
        let mut document: Document = serde_json::from_str(&document)?;
        annotate_document(&mut document, &model, n_labels);
        println!("{}", serde_json::to_string(&document).expect("yolo"));
    }
    Ok(())
}

fn dedup(args: DedupArgs) -> anyhow::Result<()> {
    let buf_stdin = BufReader::new(std::io::stdin());
    let mut bloom_filter = match args.bloom_filter {
        Some(path) => bloom::BloomFilter::load(&path)?,
        None => bloom::BloomFilter::new(1 << 24, 2),
    };
    for document in buf_stdin.lines() {
        let document = document?;
        let mut document: Document = serde_json::from_str(&document)?;
        /*let mut text = String::new();
        for paragraph in re.split(&document.text) {
            let mut keep = 0.0f32;
            for line in paragraph.split('\n') {
            if !bloom_filter.get(line) {
                keep += line.len() as f32;
            }
            bloom_filter.set(line);
            }
            println!("{}", keep / (paragraph.len() as f32));
            if keep / (paragraph.len() as f32) > args.threshold {
            //println!("{paragraph}\n");
            text.push_str(paragraph);
            text.push_str("\n\n");
            }
        }
        document.text = text;*/
        document.text = dedup_document(&document.text, &mut bloom_filter, args.threshold);
        println!("{}", serde_json::to_string(&document).expect("yolo"));
        /*let mut uniq = String::new();
        for line in document.text.split('\n') {
            //let h = fnv1a_64(&line.as_bytes());
            //all += h;
            //let h = bloom::fnv1a_batch::<2>(&line.as_bytes());
            //all += h[0];
            let c = if bloom_filter.get(&line) { 'x' } else { '.' };
            uniq.push(c);
            bloom_filter.set(&line);
            println!("{} {}", c, line);
        }
        document.uniq = Some(uniq);
        println!("{}", serde_json::to_string(&document).expect("yolo"));*/
    }
    Ok(())
}

fn data_dedup(args: DedupArgs) -> anyhow::Result<()> {
    let buf_stdin = BufReader::new(std::io::stdin());
    let mut bloom_filter = match args.bloom_filter {
        Some(path) => bloom::BloomFilter::load(&path)?,
        None => bloom::BloomFilter::new(1 << 24, 2),
    };
    for document in buf_stdin.lines() {
        let document = document?;
        let document: Document = serde_json::from_str(&document)?;
        for line in document.text.split('\n') {
            if bloom_filter.get(line) {
                println!("{}", line);
            }
            bloom_filter.set(line);
        }
    }
    Ok(())
}

fn benchmark() -> anyhow::Result<()> {
    use std::time::SystemTime;

    //compute_repetitions_rolling(&String::from("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 8);
    let t0 = SystemTime::now();
    let buf_stdin = BufReader::new(std::io::stdin());
    let mut n_bytes: usize = 0;
    let mut n_text: usize = 0;
    for document in buf_stdin.lines() {
        let document = document?;
        n_bytes += document.len();
        let document: Document = serde_json::from_str(&document)?;
        n_text += document.text.len();
    }
    println!("{} {} {}", n_bytes, n_text, t0.elapsed()?.as_micros());
    println!("{}", n_bytes as u128 / t0.elapsed()?.as_micros());

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Filter(args)) => filter(args)?,
        Some(Commands::Annotate(args)) => annotate(args)?,
        Some(Commands::Dedup(args)) => dedup(args)?,
        Some(Commands::Heuristics) => add_heuristics()?,
        Some(Commands::Benchmark) => benchmark()?,
        Some(Commands::TheStack) => code::process_thestack()?,
        Some(Commands::Data(args)) => data_dedup(args)?,
        None => {}
    }
    Ok(())
}
