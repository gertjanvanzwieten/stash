use pyo3::{pyclass, pymethods, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    bytes::Bytes,
    deserialize::deserialize,
    hex::Hex,
    mapping::{Mapping, MappingError, MappingResult, Key},
    serialize::serialize,
};

use std::{fmt::Write as FmtWrite, fs::File, io::{Read, Write, Result as IoResult}, ops::Deref, path::PathBuf};

pub struct FsDB(PathBuf);

impl FsDB {
    fn path_for(&self, h: &Key) -> PathBuf {
        let capacity = self.0.as_os_str().len() + Key::NBYTES * 2 + 2;
        let mut path = PathBuf::with_capacity(capacity);
        let s = path.as_mut_os_string();
        s.push(self.0.as_os_str());
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

impl Mapping for FsDB {
    fn put(&mut self, h: Key, b: impl AsRef<[u8]>) -> MappingResult<()> {
        let path = self.path_for(&h);
        if let Ok(f) = File::open(&path) {
            if ! file_equals(f, b.as_ref())? {
                return Err(MappingError::Collision(h));
            }
        }
        else {
            std::fs::create_dir_all(path.parent().unwrap())?;
            File::create_new(&path)?.write_all(b.as_ref())?;
        }
        Ok(())
    }
    fn get_blob(&self, h: Key) -> MappingResult<impl Deref<Target = [u8]>> {
        std::fs::read(self.path_for(&h)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                MappingError::NotFound(h)
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
    fn hash<'py>(&self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        serialize(obj, &mut FsDB(self.path.clone()))
    }
    fn unhash<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        deserialize(obj, &FsDB(self.path.clone()))
    }
}
