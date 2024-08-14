use pyo3::{pyclass, pymethods, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    bytes::Bytes,
    hex::Hex,
    keygen::{Blake3, KeyGenerator},
    mapping::{Mapping, MappingError, MappingResult},
    stash::{Deserialize, Serialize},
};

use std::{fmt::Write as FmtWrite, fs::OpenOptions, io::Write, ops::Deref, path::PathBuf};

pub struct FsDB<G> {
    path: PathBuf,
    keygen: G,
}

impl<G: KeyGenerator> FsDB<G> {
    pub fn new(path: PathBuf, keygen: G) -> Self {
        Self { path, keygen }
    }

    fn path_for(&self, h: &G::Key) -> PathBuf {
        let capacity = self.path.as_os_str().len() + G::Key::NBYTES * 2 + 2;
        let mut path = PathBuf::with_capacity(capacity);
        let s = path.as_mut_os_string();
        s.push(self.path.as_os_str());
        let (left, right) = h.as_bytes().split_at(1);
        write!(
            s,
            "{}{}{}{}",
            std::path::MAIN_SEPARATOR,
            Hex(left),
            std::path::MAIN_SEPARATOR,
            Hex(right)
        )
        .unwrap();
        path
    }
}

impl<G: KeyGenerator> Mapping for FsDB<G> {
    type Key = G::Key;
    fn put_blob(&mut self, b: Vec<u8>) -> MappingResult<Self::Key> {
        let h = self.keygen.digest(&b);
        let path = self.path_for(&h);
        if !path.is_file() {
            std::fs::create_dir_all(path.parent().unwrap())?;
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?
                .write_all(&b)?;
        }
        Ok(h)
    }
    fn get_blob(&self, h: &Self::Key) -> MappingResult<impl Deref<Target = [u8]>> {
        std::fs::read(self.path_for(h)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                MappingError::not_found(h)
            } else {
                e.into()
            }
        })
    }
}

#[pyclass(frozen, name = "FsDB")]
pub struct PyFsDB {
    path: PathBuf,
}

#[pymethods]
impl PyFsDB {
    #[new]
    fn py_new(path: PathBuf) -> Self {
        Self { path }
    }
    fn dumps<'py>(&self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        Serialize::to_py(obj, &mut FsDB::new(self.path.clone(), Blake3))
    }
    fn loads<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        Deserialize::from_py(obj, &FsDB::new(self.path.clone(), Blake3))
    }
}
