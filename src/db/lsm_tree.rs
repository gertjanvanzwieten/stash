use pyo3::{pyclass, pymethods, types::PyBytes, Bound, PyAny, PyResult};

use lsm_tree::AbstractTree;

use crate::{
    bytes::Bytes,
    keygen::{Blake3, KeyGenerator},
    mapping::{Mapping, MappingError, MappingResult},
    stash::{Deserialize, Serialize},
};

use std::{ops::Deref, path::PathBuf};

pub struct LSMTree<G> {
    tree: lsm_tree::Tree,
    keygen: G,
}

impl<G: KeyGenerator> LSMTree<G> {
    pub fn new(path: PathBuf, keygen: G) -> MappingResult<Self> {
        Ok(Self {
            tree: lsm_tree::Config::new(path).open()?,
            keygen,
        })
    }
    pub fn flush(&mut self) -> MappingResult<()> {
        self.tree.flush_active_memtable(0)?;
        Ok(())
    }
}

impl<G: KeyGenerator> Mapping for LSMTree<G> {
    type Key = G::Key;
    fn put_blob(&mut self, b: Vec<u8>) -> MappingResult<Self::Key> {
        let h = self.keygen.digest(&b);
        self.tree.insert(h.as_bytes(), b, /* sequence number */ 0);
        Ok(h)
    }
    fn get_blob(&self, h: &Self::Key) -> MappingResult<impl Deref<Target = [u8]>> {
        self.tree
            .get(h.as_bytes())?
            .ok_or_else(|| MappingError::not_found(h))
    }
}

impl From<lsm_tree::Error> for MappingError {
    fn from(err: lsm_tree::Error) -> Self {
        match err {
            lsm_tree::Error::Io(e) => MappingError::IoError(e),
            _ => MappingError::Dyn(err.into()),
        }
    }
}

#[pyclass(name = "LSMTree")]
pub struct PyLSMTree {
    db: LSMTree<Blake3>,
}

#[pymethods]
impl PyLSMTree {
    #[new]
    fn py_new(path: PathBuf) -> PyResult<Self> {
        Ok(Self {
            db: LSMTree::new(path, Blake3)?,
        })
    }
    fn dumps<'py>(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        let retval = Serialize::to_py(obj, &mut self.db);
        if retval.is_ok() {
            self.db.flush()?;
        }
        retval
    }
    fn loads<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        Deserialize::from_py(obj, &self.db)
    }
}
