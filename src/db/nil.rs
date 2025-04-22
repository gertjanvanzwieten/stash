use pyo3::{pyfunction, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    mapping::{Mapping, MappingError, MappingResult, Key},
    serialize::serialize,
};

use std::ops::Deref;

pub struct Nil;

impl Mapping for Nil {
    fn put(&mut self, _h: Key, _b: impl AsRef<[u8]>) -> MappingResult<()> {
        Ok(())
    }
    fn get_blob(&self, h: Key) -> MappingResult<impl Deref<Target = [u8]>> {
        Err::<Vec<u8>, _>(MappingError::NotFound(h))
    }
}

#[pyfunction]
pub fn hash<'py>(obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    serialize(obj, &mut Nil)
}
