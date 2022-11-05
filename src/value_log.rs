use bitflags::bitflags;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use log::info;
use memmap::MmapMut;
use parking_lot::*;
use rand::random;
use serde_json::to_vec;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::fs::{read_dir, File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;
use std::process::id;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::{fmt, fs, thread};
use tabled::object::Entity::Cell;

use crate::kv::KV;
use crate::log_file::LogFile;
use crate::options::Options;
use crate::skl::BlockBytes;
use crate::table::iterator::BlockSlice;
use crate::y::{create_synced_file, is_eof, open_existing_synced_file, read_at, Decode, Encode};
use crate::Error::Unexpected;
use crate::{Error, Result};

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
bitflags! {
    pub struct MetaBit: u8{
        // Set if the key has been deleted.
        const BitDelete = 1;
        // Set if the value is NOT stored directly next to key.
        const BitValuePointer = 2;
        const BitUnused = 4;
        // Set if the key is set using SetIfAbsent.
        const BitSetIfAbsent = 8;
    }
}

const M: u64 = 1 << 20;

#[derive(Debug, Default)]
#[repr(C)]
pub(crate) struct Header {
    pub(crate) k_len: u32,
    pub(crate) v_len: u32,
    pub(crate) meta: u8,
    pub(crate) user_mata: u8,
    pub(crate) cas_counter: u64,
    pub(crate) cas_counter_check: u64,
}

impl Header {
    pub(crate) const fn encoded_size() -> usize {
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
    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
    pub(crate) value: Vec<u8>,
    // If nonzero, we will check if existing casCounter matches.
    pub(crate) cas_counter_check: u64,
    // Fields maintained internally.
    pub(crate) offset: u32,
    pub(crate) cas_counter: u64,
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
pub struct ValuePointer {
    pub(crate) fid: u32,
    pub(crate) len: u32,
    pub(crate) offset: u32,
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

struct Request<'a> {
    entries: &'a [Entry],
}

pub struct ValueLogCore {
    dir_path: Box<String>,
    max_fid: AtomicU32,
    files: RwLock<ValueFiles>,
    num_active_iterators: AtomicI32,
    writable_log_offset: AtomicU32,
    opt: Options,
    kv: *const KV,
}

#[derive(Debug)]
struct ValueFiles {
    files_to_be_deleted: HashSet<u32>,
    files_map: HashMap<u32, LogFile>,
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
    fn create_vlog_file(&self, fid: u32) -> Result<LogFile> {
        let _path = self.fpath(fid);
        let mut log_file = LogFile {
            _path: Box::new(_path.clone()),
            fd: None,
            fid,
            _mmap: None,
            sz: 0,
        };
        self.writable_log_offset.store(0, Ordering::Release);
        let fd = create_synced_file(&_path, self.opt.sync_writes)?;
        log_file.fd.replace(fd);
        // todo sync directly
        Ok(log_file)
    }

    fn open(&mut self, kv: &KV, opt: Options) -> Result<()> {
        self.dir_path = opt.value_dir.clone();
        self.opt = opt;
        let kv = kv as *const KV;
        self.kv = kv;
        self.open_create_files()?;
        // todo add garbage
        Ok(())
    }

    fn get_kv(&self) -> &KV {
        unsafe { &*(self.kv) }
    }

    pub fn close(&self) -> Result<()> {
        info!("Stopping garbage collection of values.");
        let mut files = self.files.write();
        for file_log in files.files_map.iter_mut() {
            file_log.1._mmap.as_mut().unwrap().flush()?;
            if *file_log.0 == self.max_fid.load(Ordering::Acquire) {
                file_log
                    .1
                    .fd
                    .as_mut()
                    .unwrap()
                    .set_len(self.writable_log_offset.load(Ordering::Acquire) as u64)?;
            }
        }
        files.files_map.clear();
        Ok(())
    }

    fn open_create_files(&mut self) -> Result<()> {
        match fs::create_dir_all(self.dir_path.as_str()) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err.into()),
        }

        /// add pid_lock
        let mut vlog_files = Self::get_data_files(&self.dir_path)?;
        let fids = Self::parse_file_ids(&mut vlog_files)?;
        let mut max_fid = 0;
        for fid in fids {
            let mut log_file = LogFile {
                _path: Box::new(self.fpath(fid as u32)),
                fd: None,
                fid: fid as u32,
                _mmap: None,
                sz: 0,
            };
            self.files.write().files_map.insert(fid as u32, log_file);
            if fid > max_fid {
                max_fid = fid;
            }
        }

        self.max_fid.store(max_fid as u32, Ordering::Release);

        // Open all previous log files are read only. Open the last log file
        // as read write.
        let mut files = self.files.write();
        for (fid, fp) in files.files_map.iter_mut() {
            if *fid == max_fid as u32 {
                let fpath = self.fpath(*fid as u32);
                let _fp = open_existing_synced_file(&fpath, self.opt.sync_writes)?;
                fp.fd.replace(_fp);
            } else {
                fp.open_read_only()?;
            }
        }
        // If no files are found, the create a new file.
        if files.files_map.is_empty() {
            let log_file = self.create_vlog_file(0)?;
            files.files_map.insert(0, log_file);
        }
        Ok(())
    }

    // Read the value log at given location.
    pub fn read(
        &self,
        vp: &ValuePointer,
        mut consumer: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        // Check for valid offset if we are reading to writable log.
        if vp.fid == self.max_fid.load(Ordering::Acquire)
            && vp.offset >= self.writable_log_offset.load(Ordering::Acquire)
        {
            return Err(format!(
                "Invalid value pointer offset: {} greater than current offset: {}",
                vp.offset,
                self.writable_log_offset.load(Ordering::Acquire)
            )
                .into());
        }

        self.read_value_bytes(vp, |buffer| {
            let mut cursor = Cursor::new(buffer);
            let mut h = Header::default();
            h.dec(&mut cursor)?;
            if (h.meta & MetaBit::BitDelete.bits()) != 0 {
                // Tombstone key
                return consumer(&vec![]);
            }
            let n = Header::encoded_size() + h.k_len as usize;
            consumer(&buffer[n..n + h.v_len as usize])
        })
    }

    /// Replays the value log. The kv provide is only valid for the lifetime of function call.
    pub fn replay(
        &self,
        vp: &ValuePointer,
        mut f: impl FnMut(&Entry, &ValuePointer) -> Result<bool>,
    ) -> Result<()> {
        let mut files = self.files.write();
        let fid_set = files
            .files_map
            .keys()
            .map(|fid| *fid)
            .collect::<HashSet<u32>>();
        let mut fids = fid_set
            .symmetric_difference(&files.files_to_be_deleted)
            .map(|fid| *fid)
            .collect::<Vec<_>>();
        fids.sort();

        info!("Seeking at value pointer: {:?}", vp);
        let offset = vp.offset + vp.len;
        for id in fids {
            if id < vp.fid {
                continue;
            }
            let mut of = offset;
            if id > vp.fid {
                of = 0;
            }
            let log_file = files.files_map.get_mut(&id).unwrap();
            log_file.iterate(of, &mut f)?;
        }
        // Seek to the end to start writing.
        let mut last_file = files
            .files_map
            .get_mut(&self.max_fid.load(Ordering::Acquire))
            .unwrap();
        let last_offset = last_file.fd.as_mut().unwrap().seek(SeekFrom::End(0))?;
        self.writable_log_offset
            .store(last_offset as u32, Ordering::Release);
        Ok(())
    }

    fn delete_log_file(&mut self, log_file: &LogFile) -> Result<()> {
        todo!()
    }

    // sync is thread-unsafe and should not be called concurrently with write.
    fn sync(&self) -> Result<()> {
        if self.opt.sync_writes {
            return Ok(());
        }
        let cur_log_file = self
            .files
            .write()
            .files_map
            .get_mut(&self.max_fid.load(Ordering::Acquire)).unwrap();
        // todo add sync directory meta
        // sync_dir_async()
        Ok(())
    }

    fn read_value_bytes(
        &self,
        vp: &ValuePointer,
        mut consumer: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut files = self.files.write();
        let log_file = files.files_map.get_mut(&vp.fid).unwrap();
        let buffer = log_file.read(vp)?;
        consumer(buffer)
    }

    // write is thread-unsafe by design and should not be called concurrently.
    fn write(&self, req: &[Request]) -> Result<()> { todo!() }

    fn pick_log(&self) -> Option<lock_api::MappedRwLockReadGuard<'_, RawRwLock, LogFile>> {
        let files = self.files.read();
        let fid_set = files
            .files_map
            .keys()
            .map(|fid| *fid)
            .collect::<HashSet<u32>>();
        let mut fids = fid_set
            .symmetric_difference(&files.files_to_be_deleted)
            .collect::<Vec<_>>();
        fids.sort();
        if fids.len() <= 1 {
            return None;
        }
        // This file shouldn't be being written to.
        let mut idx = random::<usize>() % fids.len();
        if idx > 0 {
            // Another level of rand to favor smaller fids.
            idx = random::<usize>() % idx;
        }
        let log = RwLockReadGuard::try_map(files, |fs| fs.files_map.get(&(idx as u32))).unwrap();
        Some(log)
    }

    fn pick_mut_log(&self) -> Option<lock_api::MappedRwLockWriteGuard<'_, RawRwLock, LogFile>> {
        let files = self.files.write();
        let fid_set = files
            .files_map
            .keys()
            .map(|fid| *fid)
            .collect::<HashSet<u32>>();
        let mut fids = fid_set
            .symmetric_difference(&files.files_to_be_deleted)
            .collect::<Vec<_>>();
        fids.sort();
        if fids.len() <= 1 {
            return None;
        }
        // This file shouldn't be being written to.
        let mut idx = random::<usize>() % fids.len();
        if idx > 0 {
            // Another level of rand to favor smaller fids.
            idx = random::<usize>() % idx;
        }
        let log =
            RwLockWriteGuard::try_map(files, |fs| fs.files_map.get_mut(&(idx as u32))).unwrap();
        Some(log)
    }

    fn get_data_files(path: &str) -> Result<Vec<String>> {
        let entries = read_dir(path)
            .map_err(|err| Unexpected(err.to_string()))?
            .filter(|res| res.is_ok())
            .map(|dir| {
                let dir = dir.unwrap().path().clone();
                dir.to_string_lossy().to_string()
            })
            .collect::<Vec<_>>();
        let mut ps = entries
            .into_iter()
            .filter(|file| file.ends_with(".vlog"))
            .collect::<Vec<_>>();
        ps.sort();
        Ok(ps)
    }
    fn parse_file_ids(data_files: &mut Vec<String>) -> Result<Vec<u64>> {
        let mut data_file_ids = data_files
            .iter_mut()
            .map(|data_file| {
                Path::new(data_file)
                    .file_prefix()
                    .unwrap()
                    .to_string_lossy()
                    .parse()
                    .unwrap()
            })
            .collect::<Vec<u64>>();
        data_file_ids.sort();
        Ok(data_file_ids)
    }

    fn wait_gc(&self) {todo!()}
    fn run_gc(&self) -> Result<()> {todo!()}

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
fn it() {
    use parking_lot::*;
    struct Flock {
        df: RwLock<HashMap<u32, String>>,
        age: u32,
    }
    impl Flock {
        fn get_df(
            &self,
        ) -> std::result::Result<
            lock_api::MappedRwLockReadGuard<'_, RawRwLock, String>,
            lock_api::RwLockReadGuard<'_, RawRwLock, HashMap<u32, String>>,
        > {
            RwLockReadGuard::try_map(self.df.read(), |df| df.get(&0))
        }

        fn get_mut(
            &self,
        ) -> std::result::Result<
            lock_api::MappedRwLockWriteGuard<'_, RawRwLock, String>,
            lock_api::RwLockWriteGuard<'_, RawRwLock, HashMap<u32, String>>,
        > {
            RwLockWriteGuard::try_map(self.df.write(), |df| df.get_mut(&0))
        }
    }
}

#[test]
fn arc_lock() {
    let fd = Arc::new(parking_lot::RwLock::new((100, 200)));
    let mut v = vec![];
    for i in 0..100 {
        let mut fd = fd.clone();
        let a = thread::spawn(move || {
            fd.write().0 += 1;
        });
        v.push(a);
    }
    for i in v {
        i.join().unwrap();
    }
    println!("{:?}", fd.read());
}
