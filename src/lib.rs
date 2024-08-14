use pyo3::prelude::*;

mod bytes;
mod db;
mod hex;
mod keygen;
mod mapping;
mod nohash;
mod stash;

#[pymodule(name = "stash")]
fn stash_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    db::populate_module(m)
}
