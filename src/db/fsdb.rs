use pyo3::{pyclass, pymethods, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    bytes::Bytes,
    deserialize::deserialize,
    hex::Hex,
    keygen::{Blake3, KeyGenerator},
    mapping::{Mapping, MappingError, MappingResult},
    serialize::serialize,
};

use std::{fmt::Write as FmtWrite, fs::File, io::{Read, Write, Result as IoResult}, ops::Deref, path::PathBuf};

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

fn file_equals(mut file: File, mut data: &[u8]) -> IoResult<bool> {
    let mut buf = [0; 131072]; // 128 KB; https://eklitzke.org/efficient-file-copying-on-linux
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            return Ok(data.is_empty());
        }
        if n > data.len() || buf[..n] != data[..n] {
            return Ok(false);
        }
        data = &data[n..];
    }
}

impl<G: KeyGenerator> Mapping for FsDB<G> {
    type Key = G::Key;
    fn put_blob(&mut self, b: impl AsRef<[u8]>) -> MappingResult<Self::Key> {
        let h = self.keygen.digest(b.as_ref());
        let path = self.path_for(&h);
        if let Ok(f) = File::open(&path) {
            if ! file_equals(f, b.as_ref())? {
                return Err(MappingError::collision(&h));
            }
        }
        else {
            std::fs::create_dir_all(path.parent().unwrap())?;
            File::create_new(&path)?.write_all(b.as_ref())?;
        }
        Ok(h)
    }
    fn get_blob(&self, h: Self::Key) -> MappingResult<impl Deref<Target = [u8]>> {
        std::fs::read(self.path_for(&h)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                MappingError::not_found(&h)
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
    #[pyo3(signature = (obj, /, *, strict=true))]
    fn hash<'py>(&self, obj: &Bound<'py, PyAny>, strict: bool) -> PyResult<Bound<'py, PyBytes>> {
        serialize(obj, &mut FsDB::new(self.path.clone(), Blake3), strict)
    }
    fn unhash<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        deserialize(obj, &FsDB::new(self.path.clone(), Blake3))
    }
}
