use crate::{bytes::Bytes, int::Int, mapping::Mapping, token};
use pyo3::{
    exceptions::PyTypeError,
    intern,
    prelude::*,
    types::{
        PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyFunction, PyInt, PyList,
        PySet, PyString, PyTuple, PyType,
    },
};
use std::collections::HashMap;

pub fn serialize<'py, M: Mapping>(
    obj: &Bound<'py, PyAny>,
    db: &mut M,
) -> PyResult<Bound<'py, PyBytes>> {
    let h = Serialize::new(obj.py())?.serialize(obj, db)?;
    let hash = db.put_blob(&h)?;
    Ok(PyBytes::new(obj.py(), hash.as_bytes()))
}

struct Serialize<'py, M: Mapping> {
    dispatch_table: Bound<'py, PyDict>,
    modules: HashMap<String, Bound<'py, PyAny>>,
    int: Int<'py>,
    seen: HashMap<*mut pyo3::ffi::PyObject, (M::Key, Bound<'py, PyAny>)>,
}

impl<'py, M: Mapping> Serialize<'py, M> {
    fn new(py: Python<'py>) -> PyResult<Self> {
        let dispatch_table = PyModule::import(py, "copyreg")?
            .getattr("dispatch_table")?
            .downcast_exact::<PyDict>()?
            .clone();
        let modules = PyModule::import(py, "sys")?.getattr("modules")?.extract()?;
        Ok(Self {
            dispatch_table,
            modules,
            int: Int::new(py)?,
            seen: HashMap::new(),
        })
    }
    fn serialize(&mut self, obj: &Bound<'py, PyAny>, db: &mut M) -> PyResult<Vec<u8>> {
        let mut v: Vec<u8> = Vec::with_capacity(255);
        if let Ok(s) = obj.downcast_exact::<PyString>() {
            v.push(token::STRING);
            v.extend_from_slice(s.to_str()?.as_bytes());
        } else if let Ok(b) = obj.downcast_exact::<PyByteArray>() {
            v.push(token::BYTEARRAY);
            // SAFETY: We promise to not let the interpreter regain control
            // or invoke any PyO3 APIs while using the slice.
            v.extend_from_slice(unsafe { b.as_bytes() });
        } else if let Ok(b) = obj.downcast_exact::<PyBytes>() {
            v.push(token::BYTES);
            v.extend_from_slice(b.as_bytes());
        } else if obj.downcast_exact::<PyInt>().is_ok() {
            v.push(token::INT);
            self.int.write_to(&mut v, obj)?;
        } else if let Ok(f) = obj.downcast_exact::<PyFloat>() {
            let f: f64 = f.extract()?;
            v.push(token::FLOAT);
            v.extend_from_slice(&f.to_le_bytes());
        } else if let Ok(l) = obj.downcast_exact::<PyList>() {
            v.push(token::LIST);
            for item in l {
                v = self.serialize_chunk(v, &item, db)?;
            }
        } else if let Ok(t) = obj.downcast_exact::<PyTuple>() {
            v.push(token::TUPLE);
            for item in t {
                v = self.serialize_chunk(v, &item, db)?;
            }
        } else if let Ok(s) = obj.downcast_exact::<PySet>() {
            v.push(token::SET);
            let chunks: PyResult<Vec<Vec<u8>>> = s
                .iter()
                .map(|item| self.serialize_chunk(Vec::with_capacity(256), &item, db))
                .collect();
            let mut chunks = chunks?;
            chunks.sort();
            for chunk in chunks {
                v.extend_from_slice(&chunk);
            }
        } else if let Ok(s) = obj.downcast_exact::<PyFrozenSet>() {
            v.push(token::FROZENSET);
            let chunks: PyResult<Vec<Vec<u8>>> = s
                .iter()
                .map(|item| self.serialize_chunk(Vec::with_capacity(256), &item, db))
                .collect();
            let mut chunks = chunks?;
            chunks.sort();
            for chunk in chunks {
                v.extend_from_slice(&chunk);
            }
        } else if let Ok(s) = obj.downcast_exact::<PyDict>() {
            v.push(token::DICT);
            let chunks: PyResult<Vec<Vec<u8>>> = s
                .iter()
                .map(|(key, value)| {
                    let v = self.serialize_chunk(Vec::with_capacity(512), &key, db)?;
                    self.serialize_chunk(v, &value, db)
                })
                .collect();
            let mut chunks = chunks?;
            chunks.sort();
            for chunk in chunks {
                v.extend_from_slice(&chunk);
            }
        } else if obj.is_none() {
            v.push(token::NONE);
        } else if let Ok(b) = obj.downcast_exact::<PyBool>() {
            v.push(if b.is_true() {
                token::TRUE
            } else {
                token::FALSE
            });
        } else if obj.downcast_exact::<PyFunction>().is_ok() {
            self.extend_global(
                &mut v,
                obj,
                obj.getattr(intern!(obj.py(), "__name__"))?
                    .downcast_exact()?,
            )?;
        } else if let Ok(t) = obj.downcast_exact::<PyType>() {
            self.extend_global(&mut v, obj, &t.qualname()?)?;
        } else if let Some(reduce) = self.get_reduce(obj.get_type())? {
            let reduced = reduce.call1((obj,))?;
            if let Ok(t) = reduced.downcast_exact::<PyTuple>() {
                v.push(token::REDUCE);
                for item in t {
                    v = self.serialize_chunk(v, &item, db)?;
                }
            } else if let Ok(s) = reduced.downcast_exact::<PyString>() {
                self.extend_global(&mut v, obj, s)?;
            } else {
                return Err(PyTypeError::new_err("invalid return value for reduce"));
            }
        } else {
            return Err(PyTypeError::new_err(format!("cannot dump {}", obj)));
        };
        Ok(v)
    }
    fn serialize_chunk(
        &mut self,
        mut v: Vec<u8>,
        obj: &Bound<'py, PyAny>,
        db: &mut M,
    ) -> PyResult<Vec<u8>> {
        if let Some((hash, _)) = self.seen.get(&obj.as_ptr()) {
            v.push(0);
            v.extend_from_slice(hash.as_bytes());
        } else {
            let b = self.serialize(obj, db)?;
            if let Ok(n) = b.len().try_into() {
                v.push(n);
                v.extend_from_slice(&b);
            } else {
                v.push(0);
                let hash = db.put_blob(&b)?;
                v.extend_from_slice(hash.as_bytes());
                let _ = self.seen.insert(obj.as_ptr(), (hash, obj.clone())); // TODO use entry
            }
        }
        Ok(v)
    }
    fn extend_global(
        &self,
        v: &mut Vec<u8>,
        obj: &Bound<PyAny>,
        name: &Bound<PyString>,
    ) -> PyResult<()> {
        v.push(token::GLOBAL);
        if let Ok(module) = obj.getattr(intern!(obj.py(), "__module__")) {
            v.extend_from_slice(module.extract::<&str>()?.as_bytes());
        } else if let Some(module_name) = self
            .modules
            .iter()
            .filter_map(|(module_name, module)| match module.getattr(name) {
                Ok(found_obj) if found_obj.is(obj) => Some(module_name),
                _ => None,
            })
            .next()
        {
            v.extend_from_slice(module_name.as_bytes());
        } else {
            v.extend_from_slice("__main__".as_bytes())
        }
        v.extend_from_slice(":".as_bytes());
        v.extend_from_slice(name.to_str()?.as_bytes());
        Ok(())
    }
    fn get_reduce(&self, objtype: Bound<'py, PyType>) -> PyResult<Option<Bound<'py, PyAny>>> {
        if let Some(reduce) = self.dispatch_table.get_item(&objtype)? {
            Ok(Some(reduce))
        } else if let Ok(reduce) = objtype.getattr(intern!(objtype.py(), "__reduce__")) {
            Ok(Some(reduce))
        } else {
            Ok(None)
        }
    }
}
