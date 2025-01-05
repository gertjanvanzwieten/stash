use pyo3::{
    prelude::*,
    types::{PyBytes, PyDict, PyInt},
    PyTypeInfo,
};

pub struct Int<'py> {
    from_bytes: Bound<'py, PyAny>,
    to_bytes: Bound<'py, PyAny>,
    bit_length: Bound<'py, PyAny>,
    kwargs: Bound<'py, PyDict>,
}

impl<'py> Int<'py> {
    pub fn new(py: Python<'py>) -> PyResult<Self> {
        let t_int = PyInt::type_object(py);
        let from_bytes = t_int.getattr("from_bytes")?;
        let to_bytes = t_int.getattr("to_bytes")?;
        let bit_length = t_int.getattr("bit_length")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("byteorder", "big")?;
        kwargs.set_item("signed", true)?;
        Ok(Self {
            from_bytes,
            to_bytes,
            bit_length,
            kwargs,
        })
    }
    pub fn write_to(&self, b: &mut Vec<u8>, obj: &Bound<'py, PyAny>) -> PyResult<()> {
        let neg = obj.lt(0)?;
        let n: usize = (if !neg {
            self.bit_length.call1((obj,))?
        } else {
            self.bit_length.call1((obj.add(1)?,))?
        })
        .extract()?;
        if neg || n > 0 {
            let bytes = self.to_bytes.call((obj, 1 + n / 8), Some(&self.kwargs))?;
            b.extend_from_slice(bytes.downcast_exact::<PyBytes>()?.as_bytes());
        }
        Ok(())
    }
    pub fn read_from(&self, b: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        self.from_bytes.call((b,), Some(&self.kwargs))
    }
}
