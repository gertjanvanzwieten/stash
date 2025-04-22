use pyo3::prelude::*;

mod bytes;
mod db;
mod deserialize;
mod hex;
mod int;
mod mapping;
mod nohash;
mod serialize;
mod token;

#[pymodule(name = "stash")]
fn stash_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    db::populate_module(m)
}
