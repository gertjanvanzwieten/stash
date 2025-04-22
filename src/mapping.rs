use crate::hex::Hex;
use pyo3::{
    exceptions::{PyException, PyKeyError, PyLookupError},
    PyErr,
};
use std::{fmt::Display, ops::Deref};

pub const NBYTES: usize = 16; // 128 bit
pub type Key = [u8; NBYTES];

fn digest(b: &[u8]) -> Key {
    cityhash_rs::cityhash_110_128(b).to_le_bytes()
}

pub trait Put {
    fn put(&mut self, h: Key, b: impl AsRef<[u8]>) -> MappingResult<()>;
    // default implementation
    fn put_blob(&mut self, b: impl AsRef<[u8]>) -> MappingResult<Key> {
        let h = digest(b.as_ref());
        self.put(h, b).and(Ok(h))
    }
}

pub trait Get {
    fn get(&self, h: Key) -> MappingResult<impl Deref<Target = [u8]>>;
    // default implementation
    fn get_blob<'a>(&self, b: &'a [u8]) -> MappingResult<(impl Deref<Target = [u8]>, &'a [u8])> {
        let (left, right) = b.split_at(NBYTES);
        Ok((self.get(left.try_into().unwrap())?, right))
    }
}

pub enum MappingError {
    NotFound(Key),
    Collision(Key),
    IoError(std::io::Error),
    PyError(PyErr),
    Dyn(Box<dyn std::error::Error>),
}

impl Display for MappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MappingError::NotFound(hash) => write!(f, "Not found: {}", Hex(hash)),
            MappingError::Collision(hash) => write!(f, "Hash collision: {}", Hex(hash)),
            MappingError::IoError(err) => write!(f, "{}", err),
            MappingError::PyError(err) => write!(f, "{}", err),
            MappingError::Dyn(err) => write!(f, "{}", err),
        }
    }
}

pub type MappingResult<T> = Result<T, MappingError>;

impl From<std::io::Error> for MappingError {
    fn from(err: std::io::Error) -> Self {
        MappingError::IoError(err)
    }
}

impl From<PyErr> for MappingError {
    fn from(err: PyErr) -> Self {
        MappingError::PyError(err)
    }
}

impl From<pyo3::DowncastError<'_, '_>> for MappingError {
    fn from(err: pyo3::DowncastError) -> Self {
        MappingError::PyError(err.into())
    }
}

impl From<MappingError> for PyErr {
    fn from(err: MappingError) -> Self {
        match err {
            MappingError::NotFound(hash) => PyErr::new::<PyKeyError, _>(format!("{}", Hex(&hash))),
            MappingError::Collision(hash) => PyErr::new::<PyLookupError, _>(format!(
                "hash collision encountered for {}",
                Hex(&hash)
            )),
            MappingError::PyError(py_error) => py_error,
            MappingError::IoError(err) => err.into(),
            MappingError::Dyn(err) => PyErr::new::<PyException, _>(format!("{}", err)),
        }
    }
}
