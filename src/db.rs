use pyo3::prelude::*;

mod fsdb;
mod nil;
mod pydb;
mod ram;
#[cfg(feature = "sled")]
mod sled;

pub fn populate_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(nil::hash, m)?)?;
    m.add_class::<fsdb::PyFsDB>()?;
    m.add_class::<pydb::PyDB>()?;
    m.add_class::<ram::PyRam>()?;
    #[cfg(feature = "sled")]
    m.add_class::<sled::PySled>()?;
    Ok(())
}
