use pyo3::{pyfunction, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    mapping::{Key, MappingResult, Put},
    serialize::serialize,
};

pub struct Nil;

impl Put for Nil {
    fn put(&mut self, _h: Key, _b: impl AsRef<[u8]>) -> MappingResult<()> {
        Ok(())
    }
}

#[pyfunction]
pub fn hash<'py>(obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    serialize(obj, &mut Nil)
}
