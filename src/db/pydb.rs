use pyo3::{
    pyclass, pymethods,
    types::{PyAnyMethods, PyBytes, PyBytesMethods},
    Bound, PyAny, PyObject, PyResult,
};

use std::ops::Deref;

use crate::{
    deserialize::deserialize,
    keygen::{Blake3, KeyGenerator},
    mapping::{Mapping, MappingResult},
    serialize::serialize,
};

struct PyBytesWrapper<'py>(Bound<'py, PyBytes>);

impl Deref for PyBytesWrapper<'_> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.0.as_bytes()
    }
}

impl Mapping for &Bound<'_, PyAny> {
    type Key = [u8; 32];
    fn put_blob(&mut self, b: impl AsRef<[u8]>) -> MappingResult<Self::Key> {
        let h = Blake3.digest(b.as_ref());
        self.set_item(
            PyBytes::new(self.py(), &h),
            PyBytes::new(self.py(), b.as_ref()),
        )?;
        Ok(h)
    }
    fn get_blob(&self, h: Self::Key) -> MappingResult<impl Deref<Target = [u8]>> {
        let item = self
            .get_item(PyBytes::new(self.py(), &h))?
            .downcast_exact::<PyBytes>()?
            .clone();
        Ok(PyBytesWrapper(item))
    }
}

#[pyclass(frozen)]
pub struct PyDB {
    pydb: PyObject,
}

#[pymethods]
impl PyDB {
    #[new]
    fn py_new(pydb: PyObject) -> PyResult<Self> {
        Ok(Self { pydb })
    }
    fn dumps<'py>(&self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        serialize(obj, &mut self.pydb.bind(obj.py()))
    }
    fn loads<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        deserialize(obj, &self.pydb.bind(obj.py()))
    }
}
