use std::collections::HashMap;
use pyo3::prelude::*;


fn fnv1a_64(data: &[u8]) -> u64 {
    let mut h = 14695981039346656037u64;
    for b in data {
	h ^= *b as u64;
	h = h.wrapping_mul(1099511628211u64);
    }
    h
}

fn init_adler_32(data: &[u8], n: usize) -> (u32, u32) {
    let mut a = 1;
    let mut b = 0;
    for i in 0..n {
	a = (a + data[i] as u32) % 65521;
	b = (b + a) % 65521;
    }
    (a, b)
}

#[pyfunction]
pub fn compute_repetitions_rolling(text: &str, n: usize) -> f32 {
    let mut n_repetitions = 0;
    let text = text.as_bytes();
    let mut ngrams = vec![0 as u32; 4 * text.len()];
    //let mut ngrams = vec![0 as u8; 16 * text.len()];

    if text.len() <= n {
	return 0.0;
    }
    
    //let (mut a, mut b) = init_adler_32(&text, n);
    //for i in n..text.len() {
    //a = (a - text[i-n] as u32 + text[i] as u32) % 65521;
    //b = (b - n as u32 * text[i-n] as u32 + a - 1) % 65521;
    //let h = (b * 65536 + a);
    let b = 33 as i64;
    let mut bn = 1 as i64;
    let prime = 1_000_000_007 as i64;
    let mut h = 0 as i64;
    for i in 0..text.len() {
	if i < n {
	    h = (h * b + text[i] as i64) % prime;
	    bn = (bn * b) % prime;
	    continue;
	}
	//h = ((h - text[i-n] as u32 * bn) * b + text[i] as u32) % prime;
	h = (h * b - bn * text[i-n] as i64 + text[i] as i64) % prime;

	/*let h1 = h as usize % (8 * ngrams.len());
	let h2 = (h * 127) as usize % (8 * ngrams.len());
	let (i1, j1) = (h1 / 8, h1 % 8);
	let (i2, j2) = (h2 / 8, h2 % 8);
	let seen = (ngrams[i1] & (1 << j1) > 0) && (ngrams[i2] & (1 << j2) > 0);
	ngrams[i1] |= 1 << j1;
	ngrams[i2] |= 1 << j2;
	if seen {
	    n_repetitions += 1;
	}*/
	
	let mut p = h as usize % ngrams.len();
	let h32 = h as u32;
	
	while ngrams[p] != 0 && ngrams[p] != h32 {
	    p = (p + 1) % ngrams.len();
	}
	if ngrams[p] == h32 {
	    n_repetitions += 1;
	} else {
	    ngrams[p] = h32;
	}
    }
    n_repetitions as f32 / text.len() as f32
}

#[pyfunction]
pub fn compute_long_words(text: &str, min_length: usize) -> f32 {
    if text.len() == 0 {
        return 0.0;
    }
    let mut n_long_words = 0;
    for word in text.split_whitespace() {
        if word.len() >= min_length {
            n_long_words += word.len();
        }
    }
    n_long_words as f32 / text.len() as f32
}

fn compute_repetitions(text: &String, n: usize) -> u32 {
    let mut n_repetitions = 0;
    let mut ngrams = HashMap::<&[u8], u32>::new();
    let text = text.as_bytes();

    if text.len() <= n {
	return 0;
    }
    
    for i in 0..(text.len()-n) {
	let ngram = &text[i..(i+n)];
	match ngrams.insert(ngram, 1) {
	    Some(_) => n_repetitions += 1,
	    None => (),
	}
    }
    n_repetitions
}

