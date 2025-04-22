use crate::{int::Int, mapping::Mapping, token};
use pyo3::{
    exceptions::PyTypeError,
    intern,
    prelude::*,
    types::{
        PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyList, PySet, PyString,
        PyTuple,
    },
};

pub fn deserialize<'py, M: Mapping>(
    obj: &Bound<'py, PyBytes>,
    db: &M,
) -> PyResult<Bound<'py, PyAny>> {
    let b = db.get_blob_from_bytes(obj.as_bytes())?;
    let py = obj.py();
    let int = Int::new(py)?;
    deserialize_chunk(&b, db, py, &int)
}

// Deserialize a Python object from a byte stream
//
// This routine takes a byte stream and deserializes it to the corresponding Python object. See
// serialize_chunk for details on the serialization format.
//
// * `b` - Byte vector to deserialize into a Python object
// * `db` - Database to load hashed blobs from.
// * `py` - A marker token that represents holding the GIL.
// * `int` - Helper object to facilitate deserialization of integers.
fn deserialize_chunk<'py, M: Mapping>(
    b: &[u8],
    db: &M,
    py: Python<'py>,
    int: &Int<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let (token, mut data) = b.split_at(1);

    let mut owned;
    let obj = match token[0] {
        token::BYTES => PyBytes::new(py, data).into_any(),
        token::BYTEARRAY => PyByteArray::new(py, data).into_any(),
        token::STRING => PyString::new(py, std::str::from_utf8(data)?).into_any(),
        token::INT => int.read_from(data)?.into_any(),
        token::FLOAT => PyFloat::new(py, f64::from_le_bytes(data.try_into()?)).into_any(),
        token::LIST => {
            let obj = PyList::empty(py);
            while !data.is_empty() {
                obj.append(deserialize_chunk(
                    if data[0] == 0 {
                        (owned, data) = db.get_blob_and_tail(&data[1..])?;
                        owned.as_ref()
                    } else {
                        let chunk;
                        (chunk, data) = data[1..].split_at(data[0] as usize);
                        chunk
                    },
                    db,
                    py,
                    int,
                )?)?;
            }
            obj.into_any()
        }
        token::TUPLE => {
            let mut objs = Vec::new();
            while !data.is_empty() {
                objs.push(deserialize_chunk(
                    if data[0] == 0 {
                        (owned, data) = db.get_blob_and_tail(&data[1..])?;
                        owned.as_ref()
                    } else {
                        let chunk;
                        (chunk, data) = data[1..].split_at(data[0] as usize);
                        chunk
                    },
                    db,
                    py,
                    int,
                )?);
            }
            PyTuple::new(py, objs)?.into_any()
        }
        token::SET => {
            let obj = PySet::empty(py)?;
            while !data.is_empty() {
                obj.add(deserialize_chunk(
                    if data[0] == 0 {
                        (owned, data) = db.get_blob_and_tail(&data[1..])?;
                        owned.as_ref()
                    } else {
                        let chunk;
                        (chunk, data) = data[1..].split_at(data[0] as usize);
                        chunk
                    },
                    db,
                    py,
                    int,
                )?)?;
            }
            obj.into_any()
        }
        token::FROZENSET => {
            let mut objs = Vec::new();
            while !data.is_empty() {
                objs.push(deserialize_chunk(
                    if data[0] == 0 {
                        (owned, data) = db.get_blob_and_tail(&data[1..])?;
                        owned.as_ref()
                    } else {
                        let chunk;
                        (chunk, data) = data[1..].split_at(data[0] as usize);
                        chunk
                    },
                    db,
                    py,
                    int,
                )?);
            }
            PyFrozenSet::new(py, objs)?.into_any()
        }
        token::DICT => {
            let d = PyDict::new(py);
            while !data.is_empty() {
                let k = deserialize_chunk(
                    if data[0] == 0 {
                        (owned, data) = db.get_blob_and_tail(&data[1..])?;
                        owned.as_ref()
                    } else {
                        let chunk;
                        (chunk, data) = data[1..].split_at(data[0] as usize);
                        chunk
                    },
                    db,
                    py,
                    int,
                )?;
                let v = deserialize_chunk(
                    if data[0] == 0 {
                        (owned, data) = db.get_blob_and_tail(&data[1..])?;
                        owned.as_ref()
                    } else {
                        let chunk;
                        (chunk, data) = data[1..].split_at(data[0] as usize);
                        chunk
                    },
                    db,
                    py,
                    int,
                )?;
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
            while !data.is_empty() {
                objs.push(deserialize_chunk(
                    if data[0] == 0 {
                        (owned, data) = db.get_blob_and_tail(&data[1..])?;
                        owned.as_ref()
                    } else {
                        let chunk;
                        (chunk, data) = data[1..].split_at(data[0] as usize);
                        chunk
                    },
                    db,
                    py,
                    int,
                )?);
            }
            let mut it = objs.into_iter();
            let func = it
                .next()
                .expect("reduction tuple does not contain function");
            let obj = func.call1(
                it.next()
                    .expect("reduction tuple does not contain arguments")
                    .downcast_exact()?,
            )?;
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

    Ok(obj)
}
