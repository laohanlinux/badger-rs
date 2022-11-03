use std::fs::File;
use std::io::Cursor;
use std::mem::size_of;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use memmap::MmapMut;

use crate::Result;
use crate::y::read_at;

struct LogFile {
    _path: Box<String>,
    fd: File,
    fid: u32,
    _mmap: MmapMut,
    sz: u32,
}

impl LogFile {
    fn open_read_only(_path: Box<String>) -> Result<LogFile> {
        let mut fd = std::fs::OpenOptions::new().read(true).open(_path.as_str())?;
        let meta = fd.metadata()?;
        let file_sz = meta.len();
        let mut _mmap = MmapMut::map_anon(file_sz as usize).unwrap();
        let read = read_at(&fd, &mut _mmap, 0)?;
        Ok(LogFile {
            _path,
            fd,
            fid: 0,
            _mmap,
            sz: 0,
        })
    }
}


#[derive(Debug, Default)]
#[repr(C)]
struct Header {
    k_len: u32,
    v_len: u32,
    meta: u8,
    user_mata: u8,
    cas_counter: u64,
    cas_counter_check: u64,
}

impl Header {
    const fn encoded_size() -> usize {
        size_of::<Self>()
    }
}

impl From<&[u8]> for Header {
    fn from(buf: &[u8]) -> Self {
        let mut cur = Cursor::new(buf);
        let mut h = Header::default();
        h.k_len = cur.read_u32::<BigEndian>().unwrap();
        h.v_len = cur.read_u32::<BigEndian>().unwrap();
        h.meta = cur.read_u8().unwrap();
        h.user_mata = cur.read_u8().unwrap();
        h.cas_counter = cur.read_u64::<BigEndian>().unwrap();
        h.cas_counter_check = cur.read_u64::<BigEndian>().unwrap();
        h
    }
}

impl Into<Vec<u8>> for Header {
    fn into(self) -> Vec<u8> {
        let mut cur = Cursor::new(vec![0u8; Header::encoded_size()]);
        cur.write_u32::<BigEndian>(self.k_len).unwrap();
        cur.write_u32::<BigEndian>(self.v_len).unwrap();
        cur.write_u8(self.meta).unwrap();
        cur.write_u8(self.user_mata).unwrap();
        cur.write_u64::<BigEndian>(self.cas_counter).unwrap();
        cur.write_u64::<BigEndian>(self.cas_counter_check).unwrap();
        cur.into_inner()
    }
}

/// Entry provides Key, Value and if required, CASCounterCheck to kv.BatchSet() API.
/// If CASCounterCheck is provided, it would be compared against the current casCounter
/// assigned to this key-value. Set be done on this key only if the counters match.
pub struct Entry {
    key: Vec<u8>,
    meta: u8,
    user_meta: u8,
    value: Vec<u8>,
    // If nonzero, we will check if existing casCounter matches.
    cas_counter_check: u64,
    // Fields maintained internally.
    offset: u32,
    cas_counter: u64,
}

#[derive(Debug, Default)]
#[repr(C)]
struct ValuePointer {
    fid: u32,
    len: u32,
    offset: u32,
}

impl ValuePointer {
    fn less(&self, o: &ValuePointer) -> bool {
        if self.fid != o.fid {
            return self.fid < o.fid;
        }
        if self.offset != o.offset {
            return self.offset < o.offset;
        }

        // todo why
        self.len < o.len
    }

    fn is_zero(&self) -> bool {
        self.fid == 0 && self.offset == 0 && self.len == 0
    }

    const fn value_pointer_encoded_size() -> usize {
        size_of::<Self>()
    }
}

impl From<&[u8]> for ValuePointer {
    fn from(buf: &[u8]) -> Self {
        let mut cur = Cursor::new(buf);
        let mut value = ValuePointer::default();
        value.fid = cur.read_u32::<BigEndian>().unwrap();
        value.len = cur.read_u32::<BigEndian>().unwrap();
        value.offset = cur.read_u32::<BigEndian>().unwrap();
        value
    }
}

impl Into<Vec<u8>> for ValuePointer {
    fn into(self) -> Vec<u8> {
        let mut cur = Cursor::new(vec![0u8; ValuePointer::value_pointer_encoded_size()]);
        cur.write_u32::<BigEndian>(self.fid).unwrap();
        cur.write_u32::<BigEndian>(self.len).unwrap();
        cur.write_u32::<BigEndian>(self.offset).unwrap();
        cur.into_inner()
    }
}

struct ValueLogCore {}

#[test]
fn it() {}