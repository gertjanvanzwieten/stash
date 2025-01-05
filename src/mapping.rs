use crate::{bytes::Bytes, hex::Hex};
use pyo3::{
    exceptions::{PyException, PyKeyError},
    PyErr,
};
use std::{fmt::Display, ops::Deref};

pub trait Mapping {
    type Key: Bytes;
    fn put_blob(&mut self, b: impl AsRef<[u8]>) -> MappingResult<Self::Key>;
    fn get_blob(&self, h: Self::Key) -> MappingResult<impl Deref<Target = [u8]>>;
}

pub enum MappingError {
    NotFound(Vec<u8>),
    IoError(std::io::Error),
    PyError(PyErr),
    Dyn(Box<dyn std::error::Error>),
}

impl MappingError {
    pub fn not_found(b: &impl Bytes) -> Self {
        Self::NotFound(b.as_bytes().into())
    }
}

impl Display for MappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MappingError::NotFound(hash) => write!(f, "Not found: {}", Hex(hash)),
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
            MappingError::PyError(py_error) => py_error,
            MappingError::IoError(err) => err.into(),
            MappingError::Dyn(err) => PyErr::new::<PyException, _>(format!("{}", err)),
        }
    }
}
