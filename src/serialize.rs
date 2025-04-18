use crate::{bytes::Bytes, int::Int, mapping::Mapping, token};
use pyo3::{
    exceptions::PyTypeError,
    intern,
    prelude::*,
    types::{
        PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyInt, PyList, PySet, PyString,
        PyTuple, PyType,
    },
};
use std::collections::hash_map::HashMap;

pub fn serialize<'py, M: Mapping>(
    obj: &Bound<'py, PyAny>,
    db: &mut M,
    strict: bool,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut v: Vec<u8> = Vec::with_capacity(255);
    let helpers = &Helpers::new(obj.py())?;
    let keep_alive = &mut Vec::new();
    if strict {
        serialize_chunk(
            obj,
            db,
            &mut v,
            helpers,
            keep_alive,
            &mut HashMap::<PyID, Strict>::new(),
        )?;
    } else {
        serialize_chunk(
            obj,
            db,
            &mut v,
            helpers,
            keep_alive,
            &mut HashMap::<PyID, NotStrict>::new(),
        )?;
    }
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
    fn extend_global(
        &self,
        v: &mut Vec<u8>,
        obj: &Bound<PyAny>,
        name: &Bound<PyString>,
    ) -> PyResult<()> {
        v.push(token::GLOBAL);
        if let Ok(module) = obj.getattr(intern!(obj.py(), "__module__")) {
            v.extend_from_slice(module.downcast_exact::<PyString>()?.to_cow()?.as_bytes());
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
        v.extend_from_slice(name.to_cow()?.as_bytes());
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

type PyID = *mut pyo3::ffi::PyObject;
type NotStrict = Box<[u8]>;
type Strict = usize;

trait Seen {
    fn cached(&self, obj: &Bound<PyAny>, v: &mut Vec<u8>) -> bool;
    fn sort<const N: usize, K: Bytes>(v: &mut [u8]);
    fn store(&mut self, v: &[u8], obj: &Bound<PyAny>);
}

impl Seen for HashMap<PyID, NotStrict> {
    fn cached(&self, obj: &Bound<PyAny>, v: &mut Vec<u8>) -> bool {
        if let Some(b) = self.get(&obj.as_ptr()) {
            v.extend_from_slice(b);
            true
        } else {
            false
        }
    }
    fn sort<const N: usize, K: Bytes>(v: &mut [u8]) {
        let copy: Box<[u8]> = v.into();
        let mut chunks = Vec::<&[u8]>::new();
        let mut left;
        let mut right = copy.as_ref();
        while !right.is_empty() {
            let mut i = 0;
            for _ in 0..N {
                let n = right[i];
                i += 1 + if n == 0 { K::NBYTES } else { n as usize };
            }
            (left, right) = right.split_at(i);
            chunks.push(left);
        }
        chunks.sort();
        let mut left;
        let mut right = v;
        for chunk in chunks {
            (left, right) = right.split_at_mut(chunk.len());
            left.clone_from_slice(chunk);
        }
    }
    fn store(&mut self, v: &[u8], obj: &Bound<PyAny>) {
        if v.len() > 2 && obj.get_refcnt() > 2 {
            let _ = self.insert(obj.as_ptr(), v.into());
        }
    }
}

impl Seen for HashMap<PyID, Strict> {
    fn cached(&self, obj: &Bound<PyAny>, v: &mut Vec<u8>) -> bool {
        if let Some(index) = self.get(&obj.as_ptr()) {
            v.push(0);
            let n = v.len();
            v.push(token::REF);
            let mut i = *index;
            while i != 0 {
                v.push(i as u8);
                i >>= 8;
            }
            v[n - 1] = (v.len() - n) as u8;
            true
        } else {
            false
        }
    }
    fn sort<const N: usize, K: Bytes>(_v: &mut [u8]) {}
    fn store(&mut self, _v: &[u8], obj: &Bound<PyAny>) {
        let index = self.len();
        let _ = self.insert(obj.as_ptr(), index);
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
// * `seen` - Hashmap with previously seen objects.
fn serialize_chunk<'py, M: Mapping, S: Seen>(
    obj: &Bound<'py, PyAny>,
    db: &mut M,
    v: &mut Vec<u8>,
    helpers: &Helpers<'py>,
    keep_alive: &mut Vec<Bound<'py, PyAny>>,
    seen: &mut S,
) -> PyResult<()> {
    // The `seen` hashmap serves to speed up hashing by recognizing that an object was serialized
    // before. It overlaps with the backrefs hashmap in that it tracks previously seen objects, but
    // is limited to objects that resulted in long enough byte sequences to be hashed, and stores
    // this hash. This allows the result to be kept in memory as a database reference, rather than
    // the full serialization that would amount to duplicating the entire object in memory. We also
    // reduce potentially expensive database operations by not writing the same entry twice.

    if seen.cached(obj, v) {
        return Ok(());
    }

    // The first byte is the length of the chunk. We write a zero now and go back to replace if
    // with the actual length when we're done serializing, or leave it at zero in case the length
    // exceeds 255 and the data needs to be hashed.
    v.push(0);

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
        v.push(token::FLOAT);
        v.extend_from_slice(&f.value().to_le_bytes());
    } else if let Ok(l) = obj.downcast_exact::<PyList>() {
        v.push(token::LIST);
        for item in l {
            serialize_chunk(&item, db, v, helpers, keep_alive, seen)?;
        }
    } else if let Ok(t) = obj.downcast_exact::<PyTuple>() {
        v.push(token::TUPLE);
        for item in t {
            serialize_chunk(&item, db, v, helpers, keep_alive, seen)?;
        }
    } else if let Ok(s) = obj.downcast_exact::<PySet>() {
        v.push(token::SET);
        // Since a set is an unordered object, its serialization (and hash) cannot be formed like
        // that of a list or tuple by simply iterating over its items. Instead we serialize all
        // items separately and then add the chunks in ascending order. This, however, presents a
        // problem for the back references, as the deserialization will need to undo this sorting
        // to maintain coherence of the object indices. To this end we perform an argsort in case
        // back references are present, and add it to the index vector in inverse order.
        for item in s.iter() {
            serialize_chunk(&item, db, v, helpers, keep_alive, seen)?;
        }
        S::sort::<1, M::Key>(&mut v[n + 1..]);
    } else if let Ok(s) = obj.downcast_exact::<PyFrozenSet>() {
        v.push(token::FROZENSET);
        for item in s.iter() {
            serialize_chunk(&item, db, v, helpers, keep_alive, seen)?;
        }
        S::sort::<1, M::Key>(&mut v[n + 1..]);
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
        for (key, value) in s.iter() {
            serialize_chunk(&key, db, v, helpers, keep_alive, seen)?;
            serialize_chunk(&value, db, v, helpers, keep_alive, seen)?;
        }
        S::sort::<2, M::Key>(&mut v[n + 1..]);
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
        helpers.extend_global(
            v,
            obj,
            obj.getattr(intern!(obj.py(), "__name__"))?
                .downcast_exact()?,
        )?;
    } else if let Ok(t) = obj.downcast_exact::<PyType>() {
        // A type object is stored by its qualified name.
        helpers.extend_global(v, obj, &t.qualname()?)?;
    } else if let Some(reduce) = helpers.get_reduce(obj.get_type())? {
        let reduced = reduce.call1((obj,))?;
        // The reduce operation can either return a qualified name, or a tuple with a reduced form.
        if let Ok(t) = reduced.downcast_exact::<PyTuple>() {
            v.push(token::REDUCE);
            for item in t {
                serialize_chunk(&item, db, v, helpers, keep_alive, seen)?;
            }
            // Since the items in `reduced` are potentially newly formed, we bump its reference
            // count so we can safely use their IDs in the `backrefs` and `seen` hashmaps without
            // risking them being reused by other objects down the line.
            keep_alive.push(reduced);
        } else if let Ok(s) = reduced.downcast_exact::<PyString>() {
            helpers.extend_global(v, obj, s)?;
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
    }
    seen.store(&v[n - 1..], obj);

    Ok(())
}
