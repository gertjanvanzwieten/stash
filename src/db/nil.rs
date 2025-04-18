use pyo3::{pyfunction, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    keygen::{Blake3, KeyGenerator},
    mapping::{Mapping, MappingError, MappingResult},
    serialize::serialize,
};

use std::{hash::Hash, ops::Deref};

pub struct Nil<G: KeyGenerator>(G);

impl<G: KeyGenerator<Key: Hash>> Mapping for Nil<G> {
    type Key = G::Key;
    fn put_blob(&mut self, b: impl AsRef<[u8]>) -> MappingResult<Self::Key> {
        Ok(self.0.digest(b.as_ref()))
    }
    fn get_blob(&self, h: Self::Key) -> MappingResult<impl Deref<Target = [u8]>> {
        Err::<Vec<u8>, _>(MappingError::not_found(&h))
    }
}

#[pyfunction]
#[pyo3(signature = (obj, /, *, strict=false))]
pub fn hash<'py>(obj: &Bound<'py, PyAny>, strict: bool) -> PyResult<Bound<'py, PyBytes>> {
    serialize(obj, &mut Nil(Blake3), strict)
}
