use pyo3::{pyfunction, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    keygen::{Blake3, KeyGenerator},
    mapping::{Mapping, MappingError, MappingResult},
    stash::Serialize,
};

use std::{hash::Hash, ops::Deref};

pub struct Nil<G: KeyGenerator>(G);

impl<G: KeyGenerator<Key: Hash>> Mapping for Nil<G> {
    type Key = G::Key;
    fn put_blob(&mut self, b: Vec<u8>) -> MappingResult<Self::Key> {
        Ok(self.0.digest(&b))
    }
    fn get_blob(&self, h: &Self::Key) -> MappingResult<impl Deref<Target = [u8]>> {
        Err::<Vec<u8>, _>(MappingError::not_found(h))
    }
}

#[pyfunction]
pub fn hash<'py>(obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    Serialize::to_py(obj, &mut Nil(Blake3))
}
