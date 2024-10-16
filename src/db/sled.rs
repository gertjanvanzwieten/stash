use pyo3::{pyclass, pymethods, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    bytes::Bytes,
    keygen::{Blake3, KeyGenerator},
    mapping::{Mapping, MappingError, MappingResult},
    stash::{Deserialize, Serialize},
};

use std::{ops::Deref, path::PathBuf};

pub struct Sled<G> {
    db: sled::Db,
    keygen: G,
}

impl<G: KeyGenerator> Sled<G> {
    pub fn new(path: PathBuf, keygen: G) -> MappingResult<Self> {
        Ok(Self {
            db: sled::open(path)?,
            keygen,
        })
    }
}

impl<G: KeyGenerator> Mapping for Sled<G> {
    type Key = G::Key;
    fn put_blob(&mut self, b: Vec<u8>) -> MappingResult<Self::Key> {
        let h = self.keygen.digest(&b);
        self.db.insert(h.as_bytes(), b)?;
        Ok(h)
    }
    fn get_blob(&self, h: &Self::Key) -> MappingResult<impl Deref<Target = [u8]>> {
        self.db
            .get(h.as_bytes())?
            .ok_or_else(|| MappingError::not_found(h))
    }
}

impl From<sled::Error> for MappingError {
    fn from(err: sled::Error) -> Self {
        match err {
            sled::Error::Io(e) => MappingError::IoError(e),
            _ => MappingError::Dyn(err.into()),
        }
    }
}

#[pyclass(name = "Sled")]
pub struct PySled {
    db: Sled<Blake3>,
}

#[pymethods]
impl PySled {
    #[new]
    fn py_new(path: PathBuf) -> PyResult<Self> {
        Ok(Self {
            db: Sled::new(path, Blake3)?,
        })
    }
    fn dumps<'py>(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        Serialize::to_py(obj, &mut self.db)
    }
    fn loads<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        Deserialize::from_py(obj, &self.db)
    }
}
