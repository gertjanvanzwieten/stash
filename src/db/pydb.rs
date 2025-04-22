use pyo3::{
    pyclass, pymethods,
    types::{PyAnyMethods, PyBytes, PyBytesMethods},
    Bound, PyAny, PyObject, PyResult,
};

use std::ops::Deref;

use crate::{
    deserialize::deserialize,
    mapping::{Mapping, MappingError, MappingResult, Key},
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
    fn put(&mut self, h: Key, b: impl AsRef<[u8]>) -> MappingResult<()> {
        if let Ok(existing) = self.get_item(PyBytes::new(self.py(), &h)) {
            if existing.downcast_exact::<PyBytes>()?.as_bytes() != b.as_ref() {
                return Err(MappingError::Collision(h));
            }
        }
        else {
            self.set_item(PyBytes::new(self.py(), &h), PyBytes::new(self.py(), b.as_ref()))?;
        }
        Ok(())
    }
    fn get_blob(&self, h: Key) -> MappingResult<impl Deref<Target = [u8]>> {
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
    fn hash<'py>(&self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        serialize(obj, &mut self.pydb.bind(obj.py()))
    }
    fn unhash<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        deserialize(obj, &self.pydb.bind(obj.py()))
    }
}
