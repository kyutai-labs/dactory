use pyo3::prelude::*;

mod bloom;
mod code;
mod entry;

/// A Python module implemented in Rust.
#[pymodule]
fn dactory(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(entry::dedup_document, m)?)?;
    m.add_class::<bloom::BloomFilter>()?;
    m.add_class::<entry::FastTextPyWrapper>()?;
    Ok(())
}
