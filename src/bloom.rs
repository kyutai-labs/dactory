use pyo3::prelude::*;

const INIT_VALUES: [u64; 8] = [
    14695981039346656037u64,
    9425296925403859339u64,
    13716263814064014149u64,
    3525492407291847033u64,
    8607404175481815707u64,
    9818874561736458749u64,
    10026508429719773353u64,
    3560712257386009938u64,
];

fn fnv1a(data: &[u8], h: Option<u64>) -> u64 {
    let mut h = match h {
        Some(h) => h,
        None => 14695981039346656037u64,
    };
    for b in data {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211u64);
    }
    h
}

pub(crate) fn fnv1a_batch<const N: usize>(data: &[u8]) -> [u64; N] {
    let mut h = [0u64; N];
    for i in 0..N {
        h[i] = INIT_VALUES[i];
    }

    for b in data {
        for i in 0..N {
            h[i] ^= *b as u64;
            h[i] = h[i].wrapping_mul(1099511628211u64);
        }
        //h[0] ^= *b as u64;
        //h[1] ^= *b as u64;
        //h[0] = h[0].wrapping_mul(1099511628211u64);
        //h[1] = h[1].wrapping_mul(1099511628211u64);
    }
    h
}

#[pyclass]
pub(crate) struct BloomFilter {
    data: Vec<u8>,
    num_hashes: usize,
}

#[pymethods]
impl BloomFilter {
    #[staticmethod]
    pub(crate) fn py_load(path: &str) -> PyResult<BloomFilter> {
        let bloom = BloomFilter::load(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
        Ok(bloom)
    }
    pub(crate) fn get(&self, s: &str) -> bool {
        for i in 0..self.num_hashes {
            let h = fnv1a(s.as_bytes(), Some(INIT_VALUES[i]));
            if !self.get_idx(h) {
                return false;
            }
        }
        true
    }

    pub(crate) fn set(&mut self, s: &str) {
        for i in 0..self.num_hashes {
            let h = fnv1a(s.as_bytes(), Some(INIT_VALUES[i]));
            self.set_idx(h);
        }
    }
}

impl BloomFilter {
    pub(crate) fn new(size: usize, num_hashes: usize) -> BloomFilter {
        BloomFilter {
            data: vec![0; size / 8],
            num_hashes,
        }
    }

    pub(crate) fn load(path: &str) -> anyhow::Result<BloomFilter> {
        use byteorder::ReadBytesExt;
        use std::io::Read;

        let mut file = std::fs::File::open(path)?;
        let num_hashes = file.read_i32::<byteorder::LittleEndian>()? as usize;
        let size = file.read_u64::<byteorder::LittleEndian>()? as usize;
        let mut data = vec![0u8; size];
        file.read_exact(&mut data)?;
        Ok(Self { data, num_hashes })
    }

    /*fn save(&self, path: &String) -> anyhow::Result<()> {
    use byteorder::WriteBytesExt;
    use std::io::Write;
    }*/

    fn get_idx(&self, idx: u64) -> bool {
        let idx = idx as usize % (8 * self.data.len());
        let i = idx / 8;
        let j = idx % 8;
        self.data[i] & (1 << j) != 0
    }

    fn set_idx(&mut self, idx: u64) {
        let idx = idx as usize % (8 * self.data.len());
        let i = idx / 8;
        let j = idx % 8;
        self.data[i] |= 1 << j;
    }
}
