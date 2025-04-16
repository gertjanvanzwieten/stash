use crate::{bytes::Bytes, int::Int, mapping::Mapping, token, usize};
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
    let b = db.get_blob_from_bytes_exact(obj.as_bytes())?;
    assert!(
        b[0] == token::TRAVERSE,
        "can only deserialize chunks of type 'traverse' for now"
    );
    let n = chunk_length::<M>(b[1]);
    let (chunk, tail) = b[1..].split_at(n);
    let mut items: Vec<Option<Bound<PyAny>>> = Vec::new();
    let mut backrefs = tail.iter().cloned();
    let py = obj.py();
    let int = Int::new(py)?;
    deserialize_chunk(chunk, db, &mut items, &mut backrefs, py, &int)
}

// Deserialize a Python object from a byte stream
//
// This routine takes a byte stream and deserializes it to the corresponding Python object. See
// serialize_chunk for details on the serialization format.
//
// * `b` - Byte vector to deserialize into a Python object
// * `db` - Database to load hashed blobs from.
// * `items` - Vector of previously deserialized objects for potential backreferencing
// * `backrefs` - Structure to keep track of object references and dictionary orderings.
// * `py` - A marker token that represents holding the GIL.
// * `int` - Helper object to facilitate deserialization of integers.
fn deserialize_chunk<'py, M: Mapping<Key: Hash>, I: std::iter::Iterator<Item = u8>>(
    b: &[u8],
    db: &M,
    items: &mut Vec<Option<Bound<'py, PyAny>>>,
    backrefs: &mut I,
    py: Python<'py>,
    int: &Int<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let nitems = items.len();
    let index = usize::decode(backrefs);
    if index != 0 {
        return Ok(items[nitems - index]
            .as_ref()
            .expect("referenced object is not yet finished")
            .clone());
    }
    items.push(None);

    let owned;
    let s = if b[0] == 0 {
        owned = db.get_blob_from_bytes(&b[1..])?;
        owned.as_ref()
    } else {
        &b[1..1 + usize::from(b[0])]
    };
    let token = s[0];
    let data = &s[1..];

    let obj = match token {
        token::BYTES => PyBytes::new(py, data).into_any(),
        token::BYTEARRAY => PyByteArray::new(py, data).into_any(),
        token::STRING => PyString::new(py, std::str::from_utf8(data)?).into_any(),
        token::INT => int.read_from(data)?.into_any(),
        token::FLOAT => PyFloat::new(py, f64::from_le_bytes(data.try_into()?)).into_any(),
        token::LIST => {
            let obj = PyList::empty(py);
            let mut i = 0;
            while i < data.len() {
                obj.append(deserialize_chunk(&data[i..], db, items, backrefs, py, int)?)?;
                i += chunk_length::<M>(data[i]);
            }
            obj.into_any()
        }
        token::TUPLE => {
            let mut objs = Vec::new();
            let mut i = 0;
            while i < data.len() {
                objs.push(deserialize_chunk(&data[i..], db, items, backrefs, py, int)?);
                i += chunk_length::<M>(data[i]);
            }
            PyTuple::new(py, objs)?.into_any()
        }
        token::SET => {
            let mut offsets = Vec::new();
            let mut indices = Vec::new();
            let mut i: usize = 0;
            while i < data.len() {
                indices.push(usize::decode(backrefs));
                offsets.push(i);
                i += chunk_length::<M>(data[i]);
            }
            assert!(i == data.len(), "invalid data length for set");
            let obj = PySet::empty(py)?;
            for index in indices {
                let offset = offsets[index];
                let item = deserialize_chunk(&data[offset..], db, items, backrefs, py, int)?;
                obj.add(item)?;
            }
            obj.into_any()
        }
        token::FROZENSET => {
            let mut offsets = Vec::new();
            let mut indices = Vec::new();
            let mut i: usize = 0;
            while i < data.len() {
                indices.push(usize::decode(backrefs));
                offsets.push(i);
                i += chunk_length::<M>(data[i]);
            }
            assert!(i == data.len(), "invalid data length for frozenset");
            let items = indices
                .into_iter()
                .map(|index| {
                    deserialize_chunk(&data[offsets[index]..], db, items, backrefs, py, int)
                })
                .collect::<PyResult<Vec<_>>>()?;
            PyFrozenSet::new(py, items)?.into_any()
        }
        token::DICT => {
            let mut offsets = Vec::new();
            let mut indices = Vec::new();
            let mut i: usize = 0;
            while i < data.len() {
                indices.push(usize::decode(backrefs));
                let j = i + chunk_length::<M>(data[i]);
                offsets.push((i, j));
                i = j + chunk_length::<M>(data[j]);
            }
            assert!(i == data.len(), "invalid data length for dict");
            let d = PyDict::new(py);
            for index in indices {
                let (i, j) = offsets[index];
                let k = deserialize_chunk(&data[i..], db, items, backrefs, py, int)?;
                let v = deserialize_chunk(&data[j..], db, items, backrefs, py, int)?;
                d.set_item(k, v)?;
            }
            d.into_any()
        }
        token::NONE => py.None().into_bound(py),
        token::TRUE => PyBool::new(py, true).to_owned().into_any(),
        token::FALSE => PyBool::new(py, false).to_owned().into_any(),
        token::GLOBAL => {
            let (module, qualname) = std::str::from_utf8(data)?
                .split_once(':')
                .expect("qualname does not contain a colon");
            PyModule::import(py, module)?.getattr(qualname)?.into_any()
        }
        token::REDUCE => {
            let mut objs = Vec::new();
            let mut i = 0;
            while i < data.len() {
                objs.push(deserialize_chunk(&data[i..], db, items, backrefs, py, int)?);
                i += chunk_length::<M>(data[i]);
            }
            let mut it = objs.into_iter();
            let func = it
                .next()
                .expect("reduction tuple does not contain function");
            let args: Bound<PyTuple> = it
                .next()
                .expect("reduction tuple does not contain arguments")
                .extract()?;
            let obj = func.call1(args)?;
            if let Some(state) = it.next() {
                if let Ok(setstate) = obj.getattr(intern!(py, "__setstate__")) {
                    setstate.call1((state,))?;
                } else if let Ok(items) = state.downcast_exact::<PyDict>() {
                    for (k, v) in items {
                        obj.setattr(k.downcast_exact::<PyString>()?, v)?;
                    }
                }
            }
            // TODO else errors
            obj
        }
        _ => return Err(PyTypeError::new_err("cannot load object")),
    };

    items[nitems] = Some(obj.clone());
    Ok(obj)
}

fn chunk_length<M: Mapping>(nbytes: u8) -> usize {
    1 + if nbytes != 0 {
        nbytes.into()
    } else {
        M::Key::NBYTES
    }
}
