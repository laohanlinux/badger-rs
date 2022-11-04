use std::collections::HashMap;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use memmap::MmapMut;
use serde_json::to_vec;
use std::fmt;
use std::fmt::Formatter;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;

use crate::options::Options;
use crate::skl::BlockBytes;
use crate::table::iterator::BlockSlice;
use crate::y::{is_eof, read_at, Decode, Encode};
use crate::{Error, Result};

struct LogFile {
    _path: Box<String>,
    fd: Option<File>,
    fid: u32,
    _mmap: Option<MmapMut>,
    sz: u32,
}

impl LogFile {
    fn open_read_only(&mut self) -> Result<()> {
        let mut fd = std::fs::OpenOptions::new()
            .read(true)
            .open(self._path.as_ref())?;
        let meta = fd.metadata()?;
        let file_sz = meta.len();
        let mut _mmap = MmapMut::map_anon(file_sz as usize).unwrap();
        let read = read_at(&fd, &mut _mmap, 0)?;
        self._mmap.replace(_mmap);
        self.fd.replace(fd);
        self.sz = file_sz as u32;
        Ok(())
    }

    // Acquire lock on mmap if you are calling this.
    fn read(&self, p: &ValuePointer) -> Result<&[u8]> {
        let offset = p.offset;
        let sz = self._mmap.as_ref().unwrap().len();
        let value_sz = p.len;
        if offset >= sz as u32 || offset + value_sz > sz as u32 {
            return Err(Error::EOF);
        } else {
            return Ok(&self._mmap.as_ref().unwrap()[offset as usize..(offset + value_sz) as usize]);
        }
        // todo add metrics
    }

    // todo opz
    fn done_writing(&mut self, offset: u32) -> Result<()> {
        self.sync()?;
        self._mmap.as_mut().unwrap().flush_async()?;
        self.fd.as_mut().unwrap().set_len(offset as u64)?;
        self.fd.as_mut().unwrap().sync_all()?;
        {
            self._mmap.take();
            self.fd.take();
        }
        self.open_read_only()
    }

    // You must hold lf.lock to sync()
    fn sync(&mut self) -> Result<()> {
        self.fd.as_mut().unwrap().sync_all()?;
        Ok(())
    }

    fn iterate(
        &mut self,
        offset: u32,
        mut f: impl FnMut(&Entry, &ValuePointer) -> Result<bool>,
    ) -> Result<()> {
        let mut fd = self.fd.as_mut().unwrap();
        fd.seek(SeekFrom::Start(offset as u64))?;
        let mut entry = Entry::default();
        let mut truncate = false;
        let mut record_offset = offset;
        loop {
            let mut h = Header::default();
            let ok = h.dec(&mut fd);
            if ok.is_err() && ok.as_ref().unwrap_err().is_io_eof() {
                break;
            }
            // todo add truncate currenct
            ok?;
            if h.k_len as usize > entry.key.capacity() {
                entry.key = vec![0u8; h.k_len as usize];
            }
            if h.v_len as usize > entry.value.capacity() {
                entry.value = vec![0u8; h.v_len as usize];
            }
            entry.key.clear();
            entry.value.clear();

            let ok = fd.read(&mut entry.key);
            if is_eof(&ok) {
                break;
            }
            ok?;

            let ok = fd.read(&mut entry.value);
            if is_eof(&ok) {
                break;
            }
            ok?;
            entry.offset = record_offset;
            entry.meta = h.meta;
            entry.user_meta = h.user_mata;
            entry.cas_counter = h.cas_counter;
            entry.cas_counter_check = h.cas_counter_check;
            let ok = fd.read_u32::<BigEndian>();
            if is_eof(&ok) {
                break;
            }
            let crc = ok?;

            let mut vp = ValuePointer::default();
            vp.len = Header::encoded_size() as u32 + h.k_len + h.v_len + 4;
            record_offset += vp.len;

            vp.offset = entry.offset;
            vp.fid = self.fid;

            let _continue = f(&entry, &vp)?;
            if !_continue {
                break;
            }
        }

        // todo add truncate
        Ok(())
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

impl Encode for Header {
    fn enc(&self, wt: &mut dyn Write) -> Result<usize> {
        let mut cur = Cursor::new(vec![0u8; Header::encoded_size()]);
        cur.write_u32::<BigEndian>(self.k_len)?;
        cur.write_u32::<BigEndian>(self.v_len)?;
        cur.write_u8(self.meta)?;
        cur.write_u8(self.user_mata)?;
        cur.write_u64::<BigEndian>(self.cas_counter)?;
        cur.write_u64::<BigEndian>(self.cas_counter_check)?;
        let block = cur.into_inner();
        wt.write_all(&block)?;
        Ok(block.len())
    }
}

impl Decode for Header {
    fn dec(&mut self, rd: &mut dyn Read) -> Result<()> {
        self.k_len = rd.read_u32::<BigEndian>()?;
        self.v_len = rd.read_u32::<BigEndian>()?;
        self.meta = rd.read_u8()?;
        self.user_mata = rd.read_u8()?;
        self.cas_counter = rd.read_u64::<BigEndian>()?;
        self.cas_counter_check = rd.read_u64::<BigEndian>()?;
        Ok(())
    }
}

/// Entry provides Key, Value and if required, CASCounterCheck to kv.BatchSet() API.
/// If CASCounterCheck is provided, it would be compared against the current casCounter
/// assigned to this key-value. Set be done on this key only if the counters match.
#[derive(Default)]
pub struct Entry {
    pub(crate) key: Vec<u8>,
    meta: u8,
    user_meta: u8,
    pub(crate) value: Vec<u8>,
    // If nonzero, we will check if existing casCounter matches.
    cas_counter_check: u64,
    // Fields maintained internally.
    offset: u32,
    cas_counter: u64,
}

impl Entry {
    fn to_string(&self, prefix: &str) -> String {
        format!("{} {}", prefix, self)
    }
}

impl Encode for Entry {
    fn enc(&self, wt: &mut dyn Write) -> Result<usize> {
        use crc32fast::Hasher;
        let mut h = Header::default();
        h.k_len = self.key.len() as u32;
        h.v_len = self.value.len() as u32;
        h.meta = self.meta;
        h.user_mata = self.user_meta;
        h.cas_counter = self.cas_counter;
        h.cas_counter_check = self.cas_counter_check;
        let mut buffer = vec![0u8; Header::encoded_size() + (h.k_len + h.v_len + 4) as usize];
        // write header
        h.enc(&mut buffer)?;
        // write key
        buffer.write(&self.key)?;
        // write value
        buffer.write(&self.value)?;
        let mut hasher = Hasher::new();
        hasher.update(&buffer[..Header::encoded_size() + (h.k_len + h.v_len) as usize]);
        let check_sum = hasher.finalize();
        // write crc32
        (&mut buffer[(h.k_len + h.v_len) as usize..]).write_u32::<BigEndian>(check_sum)?;
        wt.write_all(&buffer)?;
        Ok(buffer.len())
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "key: {:?} meta: {} usermeta: {} offset: {} len={} cas={} check={}",
            self.key,
            self.meta,
            self.user_meta,
            self.offset,
            self.value.len(),
            self.cas_counter,
            self.cas_counter_check
        )
    }
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

impl Encode for ValuePointer {
    fn enc(&self, wt: &mut dyn Write) -> Result<usize> {
        wt.write_u32::<BigEndian>(self.fid)?;
        wt.write_u32::<BigEndian>(self.len)?;
        wt.write_u32::<BigEndian>(self.offset)?;
        Ok(ValuePointer::value_pointer_encoded_size())
    }
}

impl Decode for ValuePointer {
    fn dec(&mut self, rd: &mut dyn Read) -> Result<()> {
        self.fid = rd.read_u32::<BigEndian>()?;
        self.len = rd.read_u32::<BigEndian>()?;
        self.offset = rd.read_u32::<BigEndian>()?;
        Ok(())
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

struct ValueLogCore {
    dir_path: Box<String>,
    max_fid: AtomicU32,
    files_map: RwLock<HashMap<u32, LogFile>>,
    writable_log_offset: AtomicU32,
    opt: Options,
}

impl ValueLogCore {
    fn vlog_file_path(dir_path: &str, fid: u32) -> String {
        let mut path = Path::new(dir_path).join(format!("{:6}", fid));
        path.to_str().unwrap().to_string()
    }
    fn fpath(&self, fid: u32) -> String {
        ValueLogCore::vlog_file_path(&self.dir_path, fid)
    }

    // todo add logFile as return value.
    fn create_vlog_file(&mut self, fid: u32) -> Result<()> {
        let _path = self.fpath(fid);
        let mut lf = LogFile {
            _path: Box::new(_path.clone()),
            fd: None,
            fid,
            _mmap: None,
            sz: 0,
        };
        self.writable_log_offset.store(0, Ordering::Acquire);
        let fd = OpenOptions::new().read(true).create(true).write(self.opt.sync_writes).open(_path)?;
        lf.fd.replace(fd);
        self.files_map.write().unwrap().insert(fid, lf);
        Ok(())
    }

    fn open(&self) {
        todo!()
    }

    fn pick_log(&self) {

    }
}

struct ValueLogIterator<'a> {
    fd: &'a File,
}

impl<'a> ValueLogIterator<'a> {
    fn new(fd: &mut std::fs::File, offset: u32) -> Result<ValueLogIterator<'_>> {
        fd.seek(SeekFrom::Start(offset as u64))?;
        Ok(ValueLogIterator { fd })
    }

    fn iterate(&mut self, log_file: &mut LogFile, offset: u32) -> Result<()> {
        todo!()
    }
}

#[test]
fn it() {}
