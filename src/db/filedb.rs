use pyo3::{pyclass, pymethods, types::PyBytes, Bound, PyAny, PyResult};

use crate::{
    deserialize::deserialize,
    mapping::{Get, Key, MappingError, MappingResult, Put, NBYTES},
    nohash::NoHashBuilder,
    serialize::serialize,
};

use std::{
    collections::{hash_map::Entry, HashMap},
    fs::File,
    io::{Read, Seek, Write},
    ops::Deref,
    path::PathBuf,
};

#[pyclass(name = "FileDB")]
pub struct FileDB {
    file: File,
    offsets: HashMap<Key, (u64, usize), NoHashBuilder>,
}

impl FileDB {
    fn new(path: PathBuf) -> std::io::Result<Self> {
        let mut offsets = HashMap::default();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut file = std::io::BufReader::new(file);
        let mut buf = [0; NBYTES + 8];
        let mut h = [0; NBYTES];
        let mut l = [0; 8];
        let filesize = file.seek(std::io::SeekFrom::End(0))?;
        file.rewind()?;
        let mut pos: u64 = 0;
        while pos != filesize {
            file.read_exact(&mut buf)?;
            h.copy_from_slice(&buf[..NBYTES]);
            l.copy_from_slice(&buf[NBYTES..]);
            let len = u64::from_le_bytes(l);
            pos += (NBYTES + 8) as u64;
            offsets.insert(h, (pos, len as usize));
            pos += len;
            file.seek(std::io::SeekFrom::Start(pos))?;
        }
        Ok(Self {
            file: file.into_inner(),
            offsets,
        })
    }
}

impl Put for FileDB {
    fn put(&mut self, h: Key, b: impl AsRef<[u8]>) -> MappingResult<()> {
        match self.offsets.entry(h) {
            Entry::Occupied(e) => {
                let (pos, len) = e.get();
                let mut data = b.as_ref();
                if data.len() != *len {
                    return Err(MappingError::Collision(h));
                }
                self.file.seek(std::io::SeekFrom::Start(*pos))?;
                const NBUF: usize = 131072; // 128 KB
                let mut readbuf = [0; NBUF];
                let mut chunk;
                while !data.is_empty() {
                    let n = self
                        .file
                        .read(&mut readbuf[..std::cmp::min(NBUF, data.len())])?;
                    (chunk, data) = data.split_at(n);
                    if &readbuf[..n] != chunk {
                        return Err(MappingError::Collision(h));
                    }
                }
            }
            Entry::Vacant(e) => {
                let data = b.as_ref();
                let pos = self.file.seek(std::io::SeekFrom::End(0))?;
                let mut writebuf = [0; NBYTES + 8];
                writebuf[..NBYTES].copy_from_slice(&h);
                writebuf[NBYTES..].copy_from_slice(&(data.len() as u64).to_le_bytes());
                self.file.write_all(&writebuf)?;
                self.file.write_all(data)?;
                e.insert_entry((pos + (NBYTES + 8) as u64, data.len()));
            }
        }
        Ok(())
    }
}

impl Get for FileDB {
    fn get(&self, h: Key) -> MappingResult<impl Deref<Target = [u8]>> {
        if let Some((pos, len)) = self.offsets.get(&h) {
            let mut file = self.file.try_clone()?;
            file.seek(std::io::SeekFrom::Start(*pos))?;
            let mut v = vec![0; *len];
            file.read_exact(&mut v)?;
            Ok(v)
        } else {
            Err(MappingError::NotFound(h))
        }
    }
}

#[pymethods]
impl FileDB {
    #[new]
    fn py_new(path: PathBuf) -> PyResult<Self> {
        Ok(Self::new(path)?)
    }
    fn hash<'py>(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        serialize(obj, self)
    }
    fn unhash<'py>(&self, obj: &'py Bound<'py, PyBytes>) -> PyResult<Bound<'py, PyAny>> {
        deserialize(obj, self)
    }
}
