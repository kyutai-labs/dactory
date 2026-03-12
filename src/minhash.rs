use pyo3::prelude::*;

const FNV_OFFSET: u64 = 14695981039346656037;
const FNV_PRIME: u64 = 1099511628211;

fn fnv1a(data: &[u8], seed: u64) -> u64 {
    let mut h = seed;
    for b in data {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

#[pyfunction]
pub fn compute_minhash_signature(text: &str, num_perm: usize, ngram_size: usize) -> Vec<u64> {
    let bytes = text.as_bytes();
    if bytes.len() < ngram_size {
        return vec![u64::MAX; num_perm];
    }

    let seeds: Vec<u64> = (0..num_perm)
        .map(|i| {
            let idx_bytes = (i as u64).to_le_bytes();
            fnv1a(&idx_bytes, FNV_OFFSET)
        })
        .collect();

    let mut mins = vec![u64::MAX; num_perm];

    for window in bytes.windows(ngram_size) {
        for (i, seed) in seeds.iter().enumerate() {
            let h = fnv1a(window, *seed);
            if h < mins[i] {
                mins[i] = h;
            }
        }
    }

    mins
}
