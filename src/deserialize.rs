use crate::{bytes::Bytes, int::Int, mapping::Mapping, nohash::NoHashMap, token};
use pyo3::{
    exceptions::PyTypeError,
    intern,
    prelude::*,
    types::{
        PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyList, PySet, PyString,
        PyTuple,
    },
};
use std::hash::Hash;

pub fn deserialize<'py, M: Mapping<Key: Hash>>(
    obj: &Bound<'py, PyBytes>,
    db: &M,
) -> PyResult<Bound<'py, PyAny>> {
    let hash = M::Key::from_bytes(obj.as_bytes()).unwrap();
    let b = db.get_blob(hash)?;
    Deserialize::new(obj.py())?
        .deserialize(&b, db)?
        .create(obj.py())
}

struct Deserialize<'py, M: Mapping> {
    py: Python<'py>,
    int: Int<'py>,
    cache: NoHashMap<M::Key, Object<'py>>,
}

impl<'py, M: Mapping<Key: Hash>> Deserialize<'py, M> {
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
            token::BYTES => Object::Immutable(PyBytes::new(self.py, data).into_any()),
            token::BYTEARRAY => Object::ByteArray(data.into()),
            token::STRING => {
                Object::Immutable(PyString::new(self.py, std::str::from_utf8(data)?).into_any())
            }
            token::INT => Object::Immutable(self.int.read_from(data)?),
            token::FLOAT => Object::Immutable(
                PyFloat::new(self.py, f64::from_le_bytes(data.try_into()?)).into_any(),
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
            token::TRUE => Object::Immutable(PyBool::new(self.py, true).to_owned().into_any()),
            token::FALSE => Object::Immutable(PyBool::new(self.py, false).to_owned().into_any()),
            token::GLOBAL => {
                let (module, qualname) = std::str::from_utf8(data)?.split_once(':').unwrap();
                Object::Immutable(
                    PyModule::import(self.py, module)?
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
                        let obj = self.deserialize(&db.get_blob(hash.clone())?, db)?;
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
            Self::ByteArray(v) => PyByteArray::new(py, &v).into_any(),
            Self::List(v) => PyList::new(
                py,
                v.into_iter()
                    .map(|item| item.create(py))
                    .collect::<Result<Vec<_>, _>>()?,
            )?
            .into_any(),
            Self::Tuple(v) => PyTuple::new(
                py,
                v.into_iter()
                    .map(|item| item.create(py))
                    .collect::<Result<Vec<_>, _>>()?,
            )?
            .into_any(),
            Self::Set(v) => PySet::new(
                py,
                v.into_iter()
                    .map(|item| item.create(py))
                    .collect::<Result<Vec<_>, _>>()?
                    .iter(),
            )?
            .into_any(),
            Self::FrozenSet(v) => PyFrozenSet::new(
                py,
                v.into_iter()
                    .map(|item| item.create(py))
                    .collect::<Result<Vec<_>, _>>()?
                    .iter(),
            )?
            .into_any(),
            Self::Dict(v) => {
                let d = PyDict::new(py);
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
