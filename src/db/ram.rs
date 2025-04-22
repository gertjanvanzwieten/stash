use pyo3::{pyclass, pymethods, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    deserialize::deserialize,
    keygen::{Blake3, KeyGenerator},
    mapping::{Mapping, MappingError, MappingResult},
    nohash::NoHashBuilder,
    serialize::serialize,
};

use std::{
    collections::{HashMap, hash_map::Entry},
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
    fn put_blob(&mut self, b: impl AsRef<[u8]>) -> MappingResult<Self::Key> {
        let h = self.keygen.digest(b.as_ref());
        match self.hashmap.entry(h.clone()) {
            Entry::Occupied(e) =>
                if e.get() != b.as_ref() {
                    return Err(MappingError::collision(&h));
                }
            Entry::Vacant(e) => {
                e.insert_entry(b.as_ref().to_vec());
            }
        }
        Ok(h)
    }
    fn get_blob(&self, h: Self::Key) -> MappingResult<impl Deref<Target = [u8]>> {
        self.hashmap
            .get(&h)
            .map_or_else(|| Err(MappingError::not_found(&h)), |v| Ok(v.deref()))
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
    fn hash<'py>(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        serialize(obj, &mut self.db)
    }
    fn unhash<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        deserialize(obj, &self.db)
    }
}
