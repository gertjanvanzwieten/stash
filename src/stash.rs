use crate::{bytes::Bytes, mapping::Mapping, nohash::NoHashMap};
use pyo3::{
    exceptions::PyTypeError,
    intern,
    prelude::*,
    types::{
        PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyFunction, PyInt, PyList,
        PySet, PyString, PyTuple, PyType,
    },
    PyTypeInfo,
};
use std::{collections::HashMap, hash::Hash};

mod token {
    pub const INT: u8 = 1;
    pub const BYTES: u8 = 2;
    pub const STRING: u8 = 3;
    pub const FLOAT: u8 = 4;
    pub const LIST: u8 = 5;
    pub const TUPLE: u8 = 6;
    pub const SET: u8 = 7;
    pub const FROZENSET: u8 = 8;
    pub const DICT: u8 = 9;
    pub const NONE: u8 = 10;
    pub const TRUE: u8 = 11;
    pub const FALSE: u8 = 12;
    pub const BYTEARRAY: u8 = 13;
    pub const REDUCE: u8 = 14;
    pub const GLOBAL: u8 = 15;
}

// INTEGER TO/FROM BYTES

struct Int<'py> {
    from_bytes: Bound<'py, PyAny>,
    to_bytes: Bound<'py, PyAny>,
    bit_length: Bound<'py, PyAny>,
    kwargs: Bound<'py, PyDict>,
}

impl<'py> Int<'py> {
    fn new(py: Python<'py>) -> PyResult<Self> {
        let t_int = PyInt::type_object_bound(py);
        let from_bytes = t_int.getattr("from_bytes")?;
        let to_bytes = t_int.getattr("to_bytes")?;
        let bit_length = t_int.getattr("bit_length")?;
        let kwargs = PyDict::new_bound(py);
        kwargs.set_item("byteorder", "big")?;
        kwargs.set_item("signed", true)?;
        Ok(Self {
            from_bytes,
            to_bytes,
            bit_length,
            kwargs,
        })
    }
    fn write_to(&self, b: &mut Vec<u8>, obj: &Bound<'py, PyAny>) -> PyResult<()> {
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
    fn read_from(&self, b: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        self.from_bytes.call((b,), Some(&self.kwargs))
    }
}

// SERIALIZATION

pub struct Serialize<'py, M: Mapping> {
    dispatch_table: Bound<'py, PyDict>,
    modules: HashMap<String, Bound<'py, PyAny>>,
    int: Int<'py>,
    seen: HashMap<*mut pyo3::ffi::PyObject, (M::Key, Bound<'py, PyAny>)>,
}

impl<'py, M: Mapping> Serialize<'py, M> {
    pub fn to_py(obj: &Bound<'py, PyAny>, db: &mut M) -> PyResult<Bound<'py, PyBytes>> {
        let h = Self::new(obj.py())?.serialize(obj, db)?;
        let hash = db.put_blob(h)?;
        Ok(PyBytes::new_bound(obj.py(), hash.as_bytes()))
    }
    fn new(py: Python<'py>) -> PyResult<Self> {
        let dispatch_table = PyModule::import_bound(py, "copyreg")?
            .getattr("dispatch_table")?
            .downcast_exact::<PyDict>()?
            .clone();
        let modules = PyModule::import_bound(py, "sys")?
            .getattr("modules")?
            .extract()?;
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
                let hash = db.put_blob(b)?;
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

// DESERIALIZATION

pub struct Deserialize<'py, M: Mapping> {
    py: Python<'py>,
    int: Int<'py>,
    cache: NoHashMap<M::Key, Object<'py>>,
}

impl<'py, M: Mapping<Key: Hash>> Deserialize<'py, M> {
    pub fn from_py(obj: &Bound<'py, PyBytes>, db: &M) -> PyResult<Bound<'py, PyAny>> {
        let hash = M::Key::from_bytes(obj.as_bytes()).unwrap();
        let b = db.get_blob(&hash)?;
        Self::new(obj.py())?.deserialize(&b, db)?.create(obj.py())
    }
    fn new(py: Python<'py>) -> PyResult<Self> {
        Ok(Self {
            py,
            int: Int::new(py)?,
            cache: NoHashMap::default(),
        })
    }
    fn deserialize(&mut self, s: &[u8], db: &M) -> PyResult<Object<'py>> {
        let token = s[0];
        let data = &s[1..];
        Ok(match token {
            token::BYTES => Object::Immutable(PyBytes::new_bound(self.py, data).into_any()),
            token::BYTEARRAY => Object::ByteArray(data.into()),
            token::STRING => Object::Immutable(
                PyString::new_bound(self.py, std::str::from_utf8(data)?).into_any(),
            ),
            token::INT => Object::Immutable(self.int.read_from(data)?),
            token::FLOAT => Object::Immutable(
                PyFloat::new_bound(self.py, f64::from_le_bytes(data.try_into()?)).into_any(),
            ),
            token::LIST => Object::List(self.deserialize_chunks(data, db)?),
            token::TUPLE => {
                let v = self.deserialize_chunks(data, db)?;
                if v.iter().all(|obj| matches!(obj, Object::Immutable(_))) {
                    Object::Immutable(Object::Tuple(v).create(self.py)?)
                } else {
                    Object::Tuple(v)
                }
            }
            token::SET => Object::Set(self.deserialize_chunks(data, db)?),
            token::FROZENSET => {
                let v = self.deserialize_chunks(data, db)?;
                if v.iter().all(|obj| matches!(obj, Object::Immutable(_))) {
                    Object::Immutable(Object::FrozenSet(v).create(self.py)?)
                } else {
                    Object::FrozenSet(v)
                }
            }
            token::DICT => {
                let mut it = self.deserialize_chunks(data, db)?.into_iter();
                let mut v = Vec::with_capacity(it.len() / 2);
                while let Some(k) = it.next() {
                    v.push((k, it.next().unwrap()));
                }
                Object::Dict(v)
            }
            token::NONE => Object::Immutable(self.py.None().into_bound(self.py)),
            token::TRUE => {
                Object::Immutable(PyBool::new_bound(self.py, true).to_owned().into_any())
            }
            token::FALSE => {
                Object::Immutable(PyBool::new_bound(self.py, false).to_owned().into_any())
            }
            token::GLOBAL => {
                let (module, qualname) = std::str::from_utf8(data)?.split_once(':').unwrap();
                Object::Immutable(
                    PyModule::import_bound(self.py, module)?
                        .getattr(qualname)?
                        .into_any(),
                )
            }
            token::REDUCE => Object::Reduce(self.deserialize_chunks(data, db)?),
            _ => return Err(PyTypeError::new_err("cannot load object")),
        })
    }
    fn deserialize_chunks(&mut self, s: &[u8], db: &M) -> PyResult<Vec<Object<'py>>> {
        let mut rem = s;
        let mut items = Vec::new();
        while !rem.is_empty() {
            let mut n: usize = rem[0].into();
            items.push(if n == 0 {
                n = M::Key::NBYTES;
                let hash = M::Key::from_bytes(&rem[1..1 + n]).unwrap();
                match self.cache.get(&hash) {
                    Some(obj) => obj.clone(),
                    None => {
                        let obj = self.deserialize(&db.get_blob(&hash)?, db)?;
                        self.cache.insert(hash, obj.clone());
                        obj
                    }
                }
            } else {
                self.deserialize(&rem[1..1 + n], db)?
            });
            rem = &rem[1 + n..];
        }
        Ok(items)
    }
}

#[derive(Clone)]
enum Object<'py> {
    ByteArray(Vec<u8>),
    List(Vec<Object<'py>>),
    Tuple(Vec<Object<'py>>),
    Set(Vec<Object<'py>>),
    FrozenSet(Vec<Object<'py>>),
    Dict(Vec<(Object<'py>, Object<'py>)>),
    Reduce(Vec<Object<'py>>),
    Immutable(Bound<'py, PyAny>),
}

impl<'py> Object<'py> {
    fn create(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(match self {
            Self::ByteArray(v) => PyByteArray::new_bound(py, &v).into_any(),
            Self::List(v) => PyList::new_bound(
                py,
                v.into_iter()
                    .map(|item| item.create(py))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .into_any(),
            Self::Tuple(v) => PyTuple::new_bound(
                py,
                v.into_iter()
                    .map(|item| item.create(py))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .into_any(),
            Self::Set(v) => PySet::new_bound(
                py,
                v.into_iter()
                    .map(|item| item.create(py))
                    .collect::<Result<Vec<_>, _>>()?
                    .iter(),
            )?
            .into_any(),
            Self::FrozenSet(v) => PyFrozenSet::new_bound(
                py,
                v.into_iter()
                    .map(|item| item.create(py))
                    .collect::<Result<Vec<_>, _>>()?
                    .iter(),
            )?
            .into_any(),
            Self::Dict(v) => {
                let d = PyDict::new_bound(py);
                for (k, v) in v {
                    d.set_item(&k.create(py)?, &v.create(py)?)?;
                }
                d.into_any()
            }
            Self::Reduce(v) => {
                let mut it = v.into_iter();
                let func = it.next().unwrap().create(py)?;
                let args: Bound<PyTuple> = it.next().unwrap().create(py)?.extract()?;
                let obj = func.call1(args)?;
                if let Some(state) = it.next() {
                    let state = state.create(py)?;
                    if let Ok(setstate) = obj.getattr(intern!(py, "__setstate__")) {
                        setstate.call1((state,))?;
                    } else if let Ok(items) = state.downcast_exact::<PyDict>() {
                        for (k, v) in items {
                            let attrname: &str = k.extract()?; // TODO avoid extraction
                            obj.setattr(attrname, v)?;
                        }
                    }
                }
                // TODO else errors
                obj
            }
            Self::Immutable(obj) => obj,
        })
    }
}
