use pyo3::{
    exceptions::PyException, pyclass, pymethods, types::PyBytes, Bound, PyAny, PyErr, PyResult,
};

use crate::{
    bytes::Bytes,
    deserialize::deserialize,
    mapping::{Mapping, MappingError, MappingResult},
    serialize::serialize,
};

use std::{ops::Deref, path::PathBuf};

impl Bytes for iroh::blobs::Hash {
    const NBYTES: usize = 32;
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
    fn from_bytes(b: &[u8]) -> Option<Self> {
        b.try_into().ok().map(Self::from_bytes)
    }
}

impl Mapping for iroh::client::Iroh {
    type Key = iroh::blobs::Hash;
    fn put_blob(&mut self, b: impl AsRef<[u8]>) -> MappingResult<Self::Key> {
        let rt = tokio::runtime::Handle::current();
        let _guard = rt.enter();
        let outcome = rt.block_on(self.blobs().add_bytes(b.as_ref().to_vec()))?;
        Ok(outcome.hash)
    }
    fn get_blob(&self, h: Self::Key) -> MappingResult<impl Deref<Target = [u8]>> {
        let rt = tokio::runtime::Handle::current();
        let _guard = rt.enter();
        let mut reader = rt.block_on(self.blobs().read(h))?;
        let bytes = rt.block_on(reader.read_to_bytes())?;
        Ok(bytes)
    }
}

impl From<anyhow::Error> for MappingError {
    fn from(err: anyhow::Error) -> Self {
        MappingError::Dyn(err.into())
    }
}

#[pyclass(name = "Iroh")]
pub struct PyIroh {
    runtime: tokio::runtime::Runtime,
    client: Option<iroh::client::Iroh>,
}

#[pymethods]
impl PyIroh {
    #[new]
    fn py_new(path: PathBuf) -> PyResult<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let _guard = runtime.enter();
        let client = runtime
            .block_on(iroh::client::Iroh::connect_path(&path))
            .map_err(|e| PyErr::new::<PyException, _>(format!("{}", e)))?;
        Ok(Self {
            runtime,
            client: Some(client),
        })
    }
    fn hash<'py>(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        let _guard = self.runtime.enter();
        serialize(obj, self.client.as_mut().unwrap())
    }
    fn unhash<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        let _guard = self.runtime.enter();
        deserialize(obj, self.client.as_ref().unwrap())
    }
}

impl Drop for PyIroh {
    fn drop(&mut self) {
        let _guard = self.runtime.enter();
        std::mem::drop(self.client.take());
    }
}
