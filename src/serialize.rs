use crate::{bytes::Bytes, int::Int, mapping::Mapping, token, usize};
use pyo3::{
    exceptions::PyTypeError,
    intern,
    prelude::*,
    types::{
        PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyInt, PyList, PySet, PyString,
        PyTuple, PyType,
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
    function_type: Bound<'py, PyAny>,
}

impl<'py> Helpers<'py> {
    fn new(py: Python<'py>) -> PyResult<Self> {
        let dispatch_table = PyModule::import(py, "copyreg")?
            .getattr("dispatch_table")?
            .downcast_exact::<PyDict>()?
            .clone();
        let function_type = PyModule::import(py, "types")?
            .getattr("FunctionType")?
            .clone();
        let modules = PyModule::import(py, "sys")?.getattr("modules")?.extract()?;
        let int = Int::new(py)?;
        Ok(Self {
            dispatch_table,
            modules,
            int,
            function_type,
        })
    }
    fn isfunction(&self, obj: &Bound<'py, PyAny>) -> PyResult<bool> {
        obj.is_instance(&self.function_type)
    }
}

// Serialize a Python object to a byte vector
//
// This routine takes an arbitrary Python object and appends its serialization to a byte vector.
// The first written byte encodes the length of the subsequent chunk, which is at least 1 and at
// most 255 bytes. Longer chunks are added in hashed form, preceded by a zero byte. The chunk
// itself starts with a single byte token to denote the type of the Python object - hence the
// minimum length of one byte. Subsequent bytes are type dependent and my result from recursion.
//
// * `obj` - Python object to be serialized.
// * `db` - Database to store hashed blobs.
// * `v` - Byte vector that the serialization is appended to.
// * `backrefs` - Structure to keep track of object references and dictionary orderings.
// * `helpers` - Helper object containing a `dispatch_table`, `modules` and `int` member.
// * `keep_alive` - Python object vector to prevent garbage collection.
// * `seen` - Hashmap with previously hashed objects.
fn serialize_chunk<'py, M: Mapping>(
    obj: &Bound<'py, PyAny>,
    db: &mut M,
    v: &mut Vec<u8>,
    mut backrefs: Option<&mut (Vec<usize>, HashMap<*mut pyo3::ffi::PyObject, usize>)>,
    helpers: &Helpers<'py>,
    keep_alive: &mut Vec<Bound<'py, PyAny>>,
    seen: &mut HashMap<*mut pyo3::ffi::PyObject, M::Key>,
) -> PyResult<()> {
    // The backrefs object is an optional Vector, Hashmap tuple to keep track of object aspects
    // that are not represented in the object's hash. Here we check if `obj` was seen before, in
    // which case its index (relative to the tip of the object stack) is pushed to the vector - or
    // a zero value otherwise. In either case we need to continue down to form the hash, but since
    // deserialization will skip recursion if an existing object can be referenced, `backrefs` is
    // set to None in this situation to keep indices coherent.
    if let Some(ref mut br) = backrefs {
        let n = br.1.len();
        // We trust that the `keep_alive` vec is doing its job keeping objects alive while we use
        // their object ID as a key.
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

    // The first byte is the length of the chunk. We write a zero now a go back to replace if with
    // the actual length when we're done serializing, or leave it at zero in case the length
    // exceeds 255 and the data needs to be hashed.
    v.push(0);

    // The `seen` hashmap serves to speed up hashing by recognizing that an object was serialized
    // before. It overlaps with the backrefs hashmap in that it tracks previously seen objects, but
    // is limited to objects that resulted in long enough byte sequences to be hashed, and stores
    // this hash. This allows the result to be kept in memory as a database reference, rather than
    // the full serialization that would amount to duplicating the entire object in memory. We also
    // reduce potentially expensive database operations by not writing the same entry twice.
    if let Some(hash) = seen.get(&obj.as_ptr()) {
        assert!(
            backrefs.is_none(),
            "already seen object was previously overlooked"
        );
        v.extend_from_slice(hash.as_bytes());
        return Ok(());
    }

    // Store the current length of the byte vector, so that we can compute and update the chunk
    // length afterward.
    let n = v.len();

    // We now differentiate between different Python object types by trying to downcast `obj` into
    // them one by one, or reducing it to a new form otherwise.
    if let Ok(s) = obj.downcast_exact::<PyString>() {
        v.push(token::STRING);
        v.extend_from_slice(s.to_cow()?.as_bytes());
    } else if let Ok(b) = obj.downcast_exact::<PyByteArray>() {
        v.push(token::BYTEARRAY);
        // SAFETY: We promise to not let the interpreter regain control or invoke any PyO3 APIs
        // while using the slice.
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
        // Since a set is an unordered object, its serialization (and hash) cannot be formed like
        // that of a list or tuple by simply iterating over its items. Instead we serialize all
        // items separately and then add the chunks in ascending order. This, however, presents a
        // problem for the back references, as the deserialization will need to undo this sorting
        // to maintain coherence of the object indices. To this end we perform an argsort in case
        // back references are present, and add it to the index vector in inverse order.
        if let Some(ref mut br) = backrefs {
            let mut chunks = Vec::with_capacity(s.len());
            let n = br.0.len();
            br.0.resize(n + s.len(), 0); // allocate space for item order prior to recursion
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
        // See SET above.
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
        // Since a dictionary is an unordered object as far as the equality test is concerned, its
        // serialization (and hash) cannot be formed like that of a list or tuple by simply
        // iterating over its items. Instead we serialize all items separately and then add the
        // chunks in ascending order. This, however, presents a problem for the back references, as
        // the deserialization will need to undo this sorting to maintain coherence of the object
        // indices. To this end we perform an argsort in case back references are present, and add
        // it to the index vector in inverse order. This also restores the original dictionary's
        // insertion order.
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
    } else if helpers.isfunction(obj)? {
        // A function object is stored by its qualified name.
        extend_global(
            &helpers.modules,
            v,
            obj,
            obj.getattr(intern!(obj.py(), "__name__"))?
                .downcast_exact()?,
        )?;
    } else if let Ok(t) = obj.downcast_exact::<PyType>() {
        // A type object is stored by its qualified name.
        extend_global(&helpers.modules, v, obj, &t.qualname()?)?;
    } else if let Some(reduce) = get_reduce(&helpers.dispatch_table, obj.get_type())? {
        let reduced = reduce.call1((obj,))?;
        // The reduce operation can either return a qualified name, or a tuple with a reduced form.
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
            // Since the items in `reduced` are potentially newly formed, we bump its reference
            // count so we can safely use their IDs in the `backrefs` and `seen` hashmaps without
            // risking them being reused by other objects down the line.
            keep_alive.push(reduced);
        } else if let Ok(s) = reduced.downcast_exact::<PyString>() {
            extend_global(&helpers.modules, v, obj, s)?;
        } else {
            return Err(PyTypeError::new_err("invalid return value for reduce"));
        }
    } else {
        return Err(PyTypeError::new_err(format!("cannot dump {}", obj)));
    };

    // Finally, the length byte is updated to the length of the chunk. If the length exceeds 255
    // then the chunk is added to the database and its hash written to the vector instead, as well
    // as added to the `seen` hashmap for potential future work avoidance.
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
        v.extend_from_slice(module.downcast_exact::<PyString>()?.to_cow()?.as_bytes());
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
    v.extend_from_slice(name.to_cow()?.as_bytes());
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
