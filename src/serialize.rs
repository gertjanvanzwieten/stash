use crate::{bytes::Bytes, int::Int, mapping::Mapping, token, usize};
use pyo3::{
    exceptions::PyTypeError,
    intern,
    prelude::*,
    types::{
        PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyFunction, PyInt, PyList,
        PySet, PyString, PyTuple, PyType,
    },
};
use std::collections::{hash_map::Entry, HashMap};

pub fn serialize<'py, M: Mapping>(
    obj: &Bound<'py, PyAny>,
    db: &mut M,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut br = (Vec::new(), HashMap::new());
    let mut v: Vec<u8> = Vec::with_capacity(255);
    v.push(token::TRAVERSE);
    serialize_chunk(
        obj,
        db,
        &mut v,
        Some(&mut br),
        &Helpers::new(obj.py())?,
        &mut Vec::new(),
        &mut HashMap::new(),
    )?;
    v.reserve(br.0.len()); // reserve at least one byte per number
    for index in br.0.into_iter() {
        v.extend(usize::encode(index))
    }
    let hash = db.put_blob(&v)?;
    Ok(PyBytes::new(obj.py(), hash.as_bytes()))
}

pub fn serialize_notraverse<'py, M: Mapping>(
    obj: &Bound<'py, PyAny>,
    db: &mut M,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut v: Vec<u8> = Vec::with_capacity(255);
    serialize_chunk(
        obj,
        db,
        &mut v,
        None,
        &Helpers::new(obj.py())?,
        &mut Vec::new(),
        &mut HashMap::new(),
    )?;
    let hash;
    let h = if v[0] == 0 {
        &v[1..]
    } else {
        hash = db.put_blob(&v[1..])?;
        hash.as_bytes()
    };
    Ok(PyBytes::new(obj.py(), h))
}

struct Helpers<'py> {
    dispatch_table: Bound<'py, PyDict>,
    modules: HashMap<String, Bound<'py, PyAny>>,
    int: Int<'py>,
}

impl<'py> Helpers<'py> {
    fn new(py: Python<'py>) -> PyResult<Self> {
        let dispatch_table = PyModule::import(py, "copyreg")?
            .getattr("dispatch_table")?
            .downcast_exact::<PyDict>()?
            .clone();
        let modules = PyModule::import(py, "sys")?.getattr("modules")?.extract()?;
        let int = Int::new(py)?;
        Ok(Self {
            dispatch_table,
            modules,
            int,
        })
    }
}

fn serialize_chunk<'py, M: Mapping>(
    obj: &Bound<'py, PyAny>,
    db: &mut M,
    v: &mut Vec<u8>,
    mut backrefs: Option<&mut (Vec<usize>, HashMap<*mut pyo3::ffi::PyObject, usize>)>,
    helpers: &Helpers<'py>,
    keep_alive: &mut Vec<Bound<'py, PyAny>>,
    seen: &mut HashMap<*mut pyo3::ffi::PyObject, M::Key>,
) -> PyResult<()> {

    if let Some(ref mut br) = backrefs {
        let n = br.1.len();
        match br.1.entry(obj.as_ptr()) {
            Entry::Occupied(e) => {
                br.0.push(n - *e.get());
                backrefs = None; // object will not be entered during deserialisation
            }
            Entry::Vacant(e) => {
                e.insert(n);
                br.0.push(0);
            }
        }
    }

    v.push(0);

    if let Some(hash) = seen.get(&obj.as_ptr()) {
        v.extend_from_slice(hash.as_bytes());
        return Ok(());
    }

    let n = v.len();

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
        helpers.int.write_to(v, obj)?;
    } else if let Ok(f) = obj.downcast_exact::<PyFloat>() {
        let f: f64 = f.extract()?;
        v.push(token::FLOAT);
        v.extend_from_slice(&f.to_le_bytes());
    } else if let Ok(l) = obj.downcast_exact::<PyList>() {
        v.push(token::LIST);
        for item in l {
            serialize_chunk(
                &item,
                db,
                v,
                backrefs.as_deref_mut(),
                helpers,
                keep_alive,
                seen,
            )?;
        }
    } else if let Ok(t) = obj.downcast_exact::<PyTuple>() {
        v.push(token::TUPLE);
        for item in t {
            serialize_chunk(
                &item,
                db,
                v,
                backrefs.as_deref_mut(),
                helpers,
                keep_alive,
                seen,
            )?;
        }
    } else if let Ok(s) = obj.downcast_exact::<PySet>() {
        v.push(token::SET);
        if let Some(ref mut br) = backrefs {
            let mut chunks = Vec::with_capacity(s.len());
            let n = br.0.len();
            br.0.resize(n + s.len(), 0); // allocate space
            for (i, item) in s.iter().enumerate() {
                let mut b = Vec::with_capacity(256);
                serialize_chunk(&item, db, &mut b, Some(br), helpers, keep_alive, seen)?;
                chunks.push((i, b));
            }
            chunks.sort_by(|(_, a), (_, b)| a.cmp(b));
            for (j, (i, chunk)) in chunks.iter().enumerate() {
                v.extend_from_slice(chunk);
                br.0[n + i] = j;
            }
        } else {
            let mut chunks = Vec::with_capacity(s.len());
            for item in s.iter() {
                let mut b = Vec::with_capacity(256);
                serialize_chunk(&item, db, &mut b, None, helpers, keep_alive, seen)?;
                chunks.push(b);
            }
            chunks.sort();
            for chunk in chunks.iter() {
                v.extend_from_slice(chunk);
            }
        }
    } else if let Ok(s) = obj.downcast_exact::<PyFrozenSet>() {
        v.push(token::FROZENSET);
        if let Some(ref mut br) = backrefs {
            let mut chunks = Vec::with_capacity(s.len());
            let n = br.0.len();
            br.0.resize(n + s.len(), 0); // allocate space
            for (i, item) in s.iter().enumerate() {
                let mut b = Vec::with_capacity(256);
                serialize_chunk(&item, db, &mut b, Some(br), helpers, keep_alive, seen)?;
                chunks.push((i, b));
            }
            chunks.sort_by(|(_, a), (_, b)| a.cmp(b));
            for (j, (i, chunk)) in chunks.iter().enumerate() {
                v.extend_from_slice(chunk);
                br.0[n + i] = j;
            }
        } else {
            let mut chunks = Vec::with_capacity(s.len());
            for item in s.iter() {
                let mut b = Vec::with_capacity(256);
                serialize_chunk(&item, db, &mut b, None, helpers, keep_alive, seen)?;
                chunks.push(b);
            }
            chunks.sort();
            for chunk in chunks.iter() {
                v.extend_from_slice(chunk);
            }
        }
    } else if let Ok(s) = obj.downcast_exact::<PyDict>() {
        v.push(token::DICT);
        if let Some(ref mut br) = backrefs {
            let mut chunks = Vec::with_capacity(s.len());
            let n = br.0.len();
            br.0.resize(n + s.len(), 0); // allocate space
            for (i, (key, value)) in s.iter().enumerate() {
                let mut b = Vec::with_capacity(256);
                serialize_chunk(&key, db, &mut b, Some(br), helpers, keep_alive, seen)?;
                serialize_chunk(&value, db, &mut b, Some(br), helpers, keep_alive, seen)?;
                chunks.push((i, b));
            }
            chunks.sort_by(|(_, a), (_, b)| a.cmp(b));
            for (j, (i, chunk)) in chunks.iter().enumerate() {
                v.extend_from_slice(chunk);
                br.0[n + i] = j;
            }
        } else {
            let mut chunks = Vec::with_capacity(s.len());
            for (key, value) in s.iter() {
                let mut b = Vec::with_capacity(256);
                serialize_chunk(&key, db, &mut b, None, helpers, keep_alive, seen)?;
                serialize_chunk(&value, db, &mut b, None, helpers, keep_alive, seen)?;
                chunks.push(b);
            }
            chunks.sort();
            for chunk in chunks.iter() {
                v.extend_from_slice(chunk);
            }
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
        extend_global(
            &helpers.modules,
            v,
            obj,
            obj.getattr(intern!(obj.py(), "__name__"))?
                .downcast_exact()?,
        )?;
    } else if let Ok(t) = obj.downcast_exact::<PyType>() {
        extend_global(&helpers.modules, v, obj, &t.qualname()?)?;
    } else if let Some(reduce) = get_reduce(&helpers.dispatch_table, obj.get_type())? {
        let reduced = reduce.call1((obj,))?;
        if let Ok(t) = reduced.downcast_exact::<PyTuple>() {
            v.push(token::REDUCE);
            for item in t {
                serialize_chunk(
                    &item,
                    db,
                    v,
                    backrefs.as_deref_mut(),
                    helpers,
                    keep_alive,
                    seen,
                )?;
            }
            keep_alive.push(reduced); // to make sure IDs in seen map are not reused
        } else if let Ok(s) = reduced.downcast_exact::<PyString>() {
            extend_global(&helpers.modules, v, obj, s)?;
        } else {
            return Err(PyTypeError::new_err("invalid return value for reduce"));
        }
    } else {
        return Err(PyTypeError::new_err(format!("cannot dump {}", obj)));
    };

    if let Ok(l) = (v.len() - n).try_into() {
        v[n - 1] = l;
    } else {
        let hash = db.put_blob(&v[n..])?;
        v.truncate(n);
        v.extend_from_slice(hash.as_bytes());
        let _ = seen.insert(obj.as_ptr(), hash);
    }

    Ok(())
}

fn extend_global(
    modules: &HashMap<String, Bound<PyAny>>,
    v: &mut Vec<u8>,
    obj: &Bound<PyAny>,
    name: &Bound<PyString>,
) -> PyResult<()> {
    v.push(token::GLOBAL);
    if let Ok(module) = obj.getattr(intern!(obj.py(), "__module__")) {
        v.extend_from_slice(module.extract::<&str>()?.as_bytes());
    } else if let Some(module_name) = modules
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

fn get_reduce<'py>(
    dispatch_table: &Bound<'py, PyDict>,
    objtype: Bound<'py, PyType>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if let Some(reduce) = dispatch_table.get_item(&objtype)? {
        Ok(Some(reduce))
    } else if let Ok(reduce) = objtype.getattr(intern!(objtype.py(), "__reduce__")) {
        Ok(Some(reduce))
    } else {
        Ok(None)
    }
}
