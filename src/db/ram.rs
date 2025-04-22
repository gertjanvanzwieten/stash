use pyo3::{pyclass, pymethods, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    deserialize::deserialize,
    mapping::{Get, Key, MappingError, MappingResult, Put},
    nohash::NoHashBuilder,
    serialize::serialize,
};

use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Deref,
};

struct Ram(HashMap<Key, Vec<u8>, NoHashBuilder>);

impl Put for Ram {
    fn put(&mut self, h: Key, b: impl AsRef<[u8]>) -> MappingResult<()> {
        match self.0.entry(h) {
            Entry::Occupied(e) => {
                if e.get() != b.as_ref() {
                    return Err(MappingError::Collision(h));
                }
            }
            Entry::Vacant(e) => {
                e.insert_entry(b.as_ref().to_vec());
            }
        }
        Ok(())
    }
}

impl Get for Ram {
    fn get_blob(&self, h: Key) -> MappingResult<impl Deref<Target = [u8]>> {
        self.0
            .get(&h)
            .map_or_else(|| Err(MappingError::NotFound(h)), |v| Ok(v.deref()))
    }
}

#[pyclass(name = "RAM")]
pub struct PyRam {
    db: Ram,
}

#[pymethods]
impl PyRam {
    #[new]
    fn py_new() -> PyResult<Self> {
        Ok(Self {
            db: Ram(HashMap::default()),
        })
    }
    fn hash<'py>(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        serialize(obj, &mut self.db)
    }
    fn unhash<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        deserialize(obj, &self.db)
    }
}
