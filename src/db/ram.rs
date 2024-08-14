use pyo3::{pyclass, pymethods, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    keygen::{Blake3, KeyGenerator},
    mapping::{Mapping, MappingError, MappingResult},
    nohash::NoHashBuilder,
    stash::{Deserialize, Serialize},
};

use std::{
    collections::HashMap,
    hash::{BuildHasher, Hash},
    ops::Deref,
};

pub struct Ram<G: KeyGenerator, S = NoHashBuilder> {
    hashmap: HashMap<G::Key, Vec<u8>, S>,
    keygen: G,
}

impl<G: KeyGenerator, S: Default> Ram<G, S> {
    pub fn new(keygen: G) -> Self {
        Self {
            hashmap: HashMap::default(),
            keygen,
        }
    }
}

impl<G: KeyGenerator<Key: Hash>, S: BuildHasher> Mapping for Ram<G, S> {
    type Key = G::Key;
    fn put_blob(&mut self, b: Vec<u8>) -> MappingResult<Self::Key> {
        let h = self.keygen.digest(&b);
        self.hashmap.entry(h.clone()).or_insert(b);
        Ok(h)
    }
    fn get_blob(&self, h: &Self::Key) -> MappingResult<impl Deref<Target = [u8]>> {
        self.hashmap
            .get(h)
            .map_or_else(|| Err(MappingError::not_found(h)), |v| Ok(v.deref()))
    }
}

#[pyclass(name = "RAM")]
pub struct PyRam {
    db: Ram<Blake3>,
}

#[pymethods]
impl PyRam {
    #[new]
    fn py_new() -> PyResult<Self> {
        Ok(Self {
            db: Ram::new(Blake3),
        })
    }
    fn dumps<'py>(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        Serialize::to_py(obj, &mut self.db)
    }
    fn loads<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        Deserialize::from_py(obj, &self.db)
    }
}
