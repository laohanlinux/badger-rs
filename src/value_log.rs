use awaitgroup::{WaitGroup, Worker};
use bitflags::bitflags;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use log::info;
use log::kv::Source;
use memmap::MmapMut;
use parking_lot::*;
use rand::random;
use serde_json::to_vec;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::fs::{read_dir, File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Deref, Index};
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
use crate::types::Channel;
use crate::y::{
    create_synced_file, is_eof, open_existing_synced_file, read_at, sync_directory, Decode, Encode,
};
use crate::Error::Unexpected;
use crate::{Error, Result};

/// Values have their first byte being byteData or byteDelete. This helps us distinguish between
/// a key that has never been seen and a key that has been explicitly deleted.
bitflags! {
    pub struct MetaBit: u8{
        /// Set if the key has been deleted.
        const BitDelete = 1;
        /// Set if the value is NOT stored directly next to key.
        const BitValuePointer = 2;
        const BitUnused = 4;
        /// Set if the key is set using SetIfAbsent.
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

impl Encode for Header {
    fn enc(&self, wt: &mut dyn Write) -> Result<usize> {
        wt.write_u32::<BigEndian>(self.k_len)?;
        wt.write_u32::<BigEndian>(self.v_len)?;
        wt.write_u8(self.meta)?;
        wt.write_u8(self.user_mata)?;
        wt.write_u64::<BigEndian>(self.cas_counter)?;
        wt.write_u64::<BigEndian>(self.cas_counter_check)?;
        Ok(Self::encoded_size())
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

/// Entry provides Key, Value and if required, cas_counter_check to kv.batch_set() API.
/// If cas_counter_check is provided, it would be compared against the current `cas_counter`
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

#[derive(Debug, Clone, Default)]
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

    pub(crate) fn is_zero(&self) -> bool {
        self.fid == 0 && self.offset == 0 && self.len == 0
    }

    pub(crate) const fn value_pointer_encoded_size() -> usize {
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

pub(crate) struct Request {
    // Input values
    pub(crate) entries: Vec<RefCell<Entry>>,
    // Output Values and wait group stuff below
    pub(crate) ptrs: RefCell<Vec<Option<ValuePointer>>>,
    pub(crate) wait_group: RefCell<Option<Worker>>,
    pub(crate) err: RefCell<Arc<Result<()>>>,
}

pub struct ValueLogCore {
    dir_path: Box<String>,
    max_fid: AtomicU32,
    // TODO
    // guards our view of which files exist, which to be deleted, how many active iterators
    files_log: Arc<RwLock<()>>,
    vlogs: Arc<RwLock<HashMap<u32, Arc<RwLock<LogFile>>>>>,
    dirty_vlogs: Arc<RwLock<HashSet<u32>>>,
    // TODO why?
    // A refcount of iterators -- when this hits zero, we can delete the files_to_be_deleted.
    num_active_iterators: AtomicI32,
    writable_log_offset: AtomicU32,
    buf: RefCell<BufWriter<Vec<u8>>>,
    opt: Options,
    kv: *const KV,
    // Only allow one GC at a time.
    garbage_ch: Channel<()>,
}

impl Default for ValueLogCore {
    fn default() -> Self {
        ValueLogCore {
            dir_path: Box::new("".to_string()),
            max_fid: Default::default(),
            files_log: Arc::new(Default::default()),
            vlogs: Arc::new(Default::default()),
            dirty_vlogs: Arc::new(Default::default()),
            num_active_iterators: Default::default(),
            writable_log_offset: Default::default(),
            buf: RefCell::new(BufWriter::new(vec![0u8; 0])),
            opt: Default::default(),
            kv: std::ptr::null(),
            garbage_ch: Channel::new(1),
        }
    }
}

impl ValueLogCore {
    fn vlog_file_path(dir_path: &str, fid: u32) -> String {
        let mut path = Path::new(dir_path).join(format!("{:06}.vlog", fid));
        path.to_str().unwrap().to_string()
    }
    fn fpath(&self, fid: u32) -> String {
        ValueLogCore::vlog_file_path(&self.dir_path, fid)
    }

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
        sync_directory(&self.dir_path)?;
        Ok(log_file)
    }

    fn create_mmap_vlog_file(&self, fid: u32, offset: u64) -> Result<LogFile> {
        let mut vlog_file = self.create_vlog_file(fid)?;
        vlog_file.fd.as_mut().unwrap().set_len(offset)?;
        let mut _mmap = unsafe { MmapMut::map_mut(vlog_file.fd.as_ref().unwrap())? };
        vlog_file._mmap.replace(_mmap);
        Ok(vlog_file)
    }

    // TODO Use Arc<KV> to replace it
    pub(crate) fn open(&mut self, kv: &KV, opt: Options) -> Result<()> {
        self.dir_path = opt.value_dir.clone();
        self.opt = opt;
        let kv = kv as *const KV;
        self.kv = kv;
        self.open_create_files()?;
        // todo add garbage and metrics
        Ok(())
    }

    fn get_kv(&self) -> &KV {
        unsafe { &*(self.kv) }
    }

    pub fn close(&self) -> Result<()> {
        info!("Stopping garbage collection of values.");
        let mut vlogs = self.vlogs.write();
        for vlog in vlogs.iter() {
            vlog.1.write()._mmap.as_mut().unwrap().flush()?;
            if *vlog.0 == self.max_fid.load(Ordering::Acquire) {
                vlog.1
                    .write()
                    .fd
                    .as_mut()
                    .unwrap()
                    .set_len(self.writable_log_offset.load(Ordering::Acquire) as u64)?;
            }
        }
        vlogs.clear();
        Ok(())
    }

    fn open_create_files(&self) -> Result<()> {
        match fs::create_dir_all(self.dir_path.as_str()) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err.into()),
        }

        // add pid_lock
        let mut vlog_files = Self::get_data_files(&self.dir_path)?;
        let fids = Self::parse_file_ids(&mut vlog_files)?;
        let mut max_fid = 0;
        for fid in fids {
            let log_file = LogFile {
                _path: Box::new(self.fpath(fid as u32)),
                fd: None,
                fid: fid as u32,
                _mmap: None,
                sz: 0,
            };
            self.vlogs
                .write()
                .insert(fid as u32, Arc::new(RwLock::new(log_file)));
            if fid > max_fid {
                max_fid = fid;
            }
        }
        self.max_fid.store(max_fid as u32, Ordering::Release);

        // Open all previous log files are read only. Open the last log file
        // as read write.
        let mut vlogs = self.vlogs.write();
        for (fid, fp) in vlogs.iter() {
            if *fid == max_fid as u32 {
                let fpath = self.fpath(*fid as u32);
                let _fp = open_existing_synced_file(&fpath, self.opt.sync_writes)?;
                fp.write().fd.replace(_fp);
            } else {
                fp.write().open_read_only()?;
            }
        }
        // If no files are found, creating a new file.
        if vlogs.is_empty() {
            let log_file = self.create_vlog_file(0)?;
            vlogs.insert(0, Arc::new(RwLock::new(log_file)));
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
        let vlogs = self.pick_log_guard();
        info!("Seeking at value pointer: {:?}", vp);
        let offset = vp.offset + vp.len;
        for id in vlogs.fids {
            if id < vp.fid {
                continue;
            }
            let mut of = offset;
            if id > vp.fid {
                of = 0;
            }
            let log_file = vlogs.vlogs.get(&id).unwrap();
            log_file.write().iterate(of, &mut f)?;
        }
        // Seek to the end to start writing.
        let last_file = vlogs
            .vlogs
            .get(&self.max_fid.load(Ordering::Acquire))
            .unwrap();
        let last_offset = last_file
            .write()
            .fd
            .as_mut()
            .unwrap()
            .seek(SeekFrom::End(0))?;
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
        let cur_wt_vlog = self
            .pick_log_guard()
            .vlogs
            .get(&self.max_fid.load(Ordering::Acquire))
            .unwrap();
        // todo add sync directory meta
        // sync_dir_async()
        Ok(())
    }

    fn read_value_bytes(
        &self,
        vp: &ValuePointer,
        mut consumer: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let log_file = self.pick_log_by_vlog_id(&vp.fid);
        let lf = log_file.read();
        let buffer = lf.read(vp)?;
        consumer(buffer)
    }

    // write is thread-unsafe by design and should not be called concurrently.
    pub(crate) fn write(&self, reqs: &[Request]) -> Result<()> {
        let cur_vlog_file = self.pick_log_by_vlog_id(&self.max_fid.load(Ordering::Acquire));
        let to_disk = || -> Result<()> {
            if self.buf.borrow().buffer().is_empty() {
                return Ok(());
            }
            info!(
                " Flushing {} blocks of total size: {}",
                reqs.len(),
                self.buf.borrow().buffer().len()
            );

            let n = cur_vlog_file
                .write()
                .fd
                .as_mut()
                .unwrap()
                .write(self.buf.borrow().buffer())?;
            // todo add metrics
            info!("Done");
            self.writable_log_offset
                .fetch_add(n as u32, Ordering::Release);
            self.buf.borrow_mut().get_mut().clear();

            if self.writable_log_offset.load(Ordering::Acquire)
                > self.opt.value_log_file_size as u32
            {
                cur_vlog_file
                    .write()
                    .done_writing(self.writable_log_offset.load(Ordering::Acquire))?;

                let new_id = self.max_fid.fetch_add(1, Ordering::Release);
                assert!(new_id < 1 << 16, "newid will overflow u16: {}", new_id);
                *cur_vlog_file.write() =
                    self.create_mmap_vlog_file(new_id, 2 * self.opt.value_log_file_size)?;
            }
            Ok(())
        };

        for req in reqs {
            for (idx, entry) in req.entries.iter().enumerate() {
                if !self.opt.sync_writes && entry.borrow().value.len() < self.opt.value_threshold {
                    // No need to write to value log.
                    req.ptrs.borrow_mut()[idx] = None;
                    continue;
                }

                let mut ptr = ValuePointer::default();
                ptr.fid = cur_vlog_file.read().fid;
                // Use the offset including buffer length so far.
                ptr.offset = self.writable_log_offset.load(Ordering::Acquire)
                    + self.buf.borrow().buffer().len() as u32;
                let mut buf = self.buf.borrow_mut();
                entry.borrow_mut().enc(&mut *buf)?;
            }
        }
        to_disk()
    }

    fn rewrite(&self, lf: &LogFile) -> Result<()> {
        let max_fid = self.max_fid.load(Ordering::Relaxed);
        assert!(
            lf.fid < max_fid,
            "fid to move: {}. Current max fid: {}",
            lf.fid,
            max_fid
        );
        // TODO add metrics

        // let mut wb = Vec::with_capacity(1000);
        let mut size = 0i64;
        let mut count = 0;
        let fe = |e: &Entry| -> Result<()> {
            count += 1;
            if count % 1000 == 0 {
                info!("Processing entry {}", count);
            }
            let vs = self.get_kv().get(&e.key);
            Ok(())
        };

        Ok(())
    }

    fn pick_log(&self) -> Option<Arc<lock_api::RwLock<RawRwLock, LogFile>>> {
        let vlogs_guard = self.pick_log_guard();
        if vlogs_guard.vlogs.len() <= 1 {
            return None;
        }
        // This file shouldn't be being written to.
        let mut idx = random::<usize>() % vlogs_guard.fids.len();
        if idx > 0 {
            // Another level of rand to favor smaller fids.
            idx = random::<usize>() % idx;
        }
        let fid = vlogs_guard.fids.index(idx);
        let vlog = vlogs_guard.vlogs.get(fid).unwrap();
        Some(vlog.clone())
    }

    fn pick_log_by_vlog_id(&self, id: &u32) -> Arc<RwLock<LogFile>> {
        let pick_vlogs = self.pick_log_guard();
        let vlogs = pick_vlogs.vlogs.get(id);
        let vlog = vlogs.unwrap();
        vlog.clone()
    }

    // Note: it not including dirty file
    fn pick_log_guard(&self) -> PickVlogsGuardsReadLock {
        let vlogs = self.vlogs.read();
        let vlogs_fids = vlogs.keys().map(|fid| *fid).collect::<HashSet<_>>();
        let dirty_vlogs = self.dirty_vlogs.read();
        let mut fids = vlogs_fids
            .difference(&*dirty_vlogs)
            .map(|fid| *fid)
            .collect::<Vec<_>>();
        fids.sort();
        PickVlogsGuardsReadLock { vlogs, fids }
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

    fn wait_gc(&self) {
        todo!()
    }
    fn run_gc(&self) -> Result<()> {
        todo!()
    }
}

struct PickVlogsGuardsReadLock<'a> {
    vlogs: lock_api::RwLockReadGuard<
        'a,
        RawRwLock,
        HashMap<u32, Arc<lock_api::RwLock<RawRwLock, LogFile>>>,
    >,
    fids: Vec<u32>,
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

pub struct SafeValueLog {
    gc_channel: Channel<()>,
    value_log: Arc<ValueLogCore>,
}

impl SafeValueLog {
    async fn trigger_gc(&self, gc_threshold: f64) -> Result<()> {
        return match self.gc_channel.try_send(()) {
            Ok(()) => {
                let ok = self.do_run_gcc(gc_threshold).await;
                self.gc_channel.recv().await.unwrap();
                ok
            }
            Err(err) => Err(Error::ValueRejected),
        };
    }

    async fn do_run_gcc(&self, gc_threshold: f64) -> Result<()> {
        Ok(())
    }
}

#[test]
fn it() {
    use parking_lot::*;
    struct Flock {
        df: RwLock<HashMap<u32, RwLock<String>>>,
        age: u32,
    }
    // impl Flock {
    //     fn get_df(
    //         &self,
    //     ) -> std::result::Result<
    //         lock_api::MappedRwLockReadGuard<'_, RawRwLock, String>,
    //         lock_api::RwLockReadGuard<'_, RawRwLock, HashMap<u32, String>>,
    //     > {
    //         RwLockReadGuard::try_map(self.df.read(), |df| df.get(&0))
    //     }
    //
    //     fn get_mut(
    //         &self,
    //         idx: u32,
    //     ) -> std::result::Result<
    //         lock_api::MappedRwLockWriteGuard<'_, RawRwLock, String>,
    //         lock_api::RwLockWriteGuard<'_, RawRwLock, HashMap<u32, String>>,
    //     > {
    //         RwLockWriteGuard::try_map(self.df.write(), |df| df.get_mut(&idx))
    //     }
    // }

    let mut flock = Flock {
        df: RwLock::new(HashMap::new()),
        age: 19,
    };
    {
        flock
            .df
            .write()
            .insert(0, RwLock::new("foobat".to_string()));
        flock.df.write().insert(1, RwLock::new("ok!".to_string()));
    }
    // let lock1 = flock.df.write().get(&0).as_mut().unwrap();
    // let lock2 = flock.df.write().get(&1).as_mut().unwrap();
    // flock.df.write().insert(3, RwLock::new("ok!".to_string()));
    // let value = RwLockReadGuard::try_map(lock1.read(), |df| Some(df));
    // println!("WHat??? {:?}", value);
}

#[tokio::test]
async fn lock() {
    use parking_lot::*;

    #[derive(Debug)]
    struct FileLog {}

    #[derive(Debug)]
    struct FileLogProxy {
        files: HashMap<u32, RwLock<FileLog>>,
    }

    impl FileLogProxy {
        fn get_file(
            &self,
            idx: u32,
        ) -> parking_lot::lock_api::RwLockReadGuard<'_, RawRwLock, FileLog> {
            let flog = self.files.get(&idx).unwrap();
            let c = flog.read();
            c
        }

        fn get_mut_file(
            &self,
            idx: u32,
        ) -> std::result::Result<
            parking_lot::lock_api::MappedRwLockWriteGuard<'_, RawRwLock, FileLog>,
            parking_lot::lock_api::RwLockWriteGuard<'_, RawRwLock, FileLog>,
        > {
            let flog = self.files.get(&idx).unwrap();
            RwLockWriteGuard::try_map(flog.write(), |df| Some(df))
        }
    }

    struct ValueLog {
        df: RwLock<FileLogProxy>,
        age: u32,
    }

    impl ValueLog {
        // fn max_vlog_rl(
        //     &self,
        // ) -> parking_lot::lock_api::RwLockReadGuard<'_, RawRwLock, FileLog> {
        //     let rl = self.rl();
        //     let vlog = rl.get_file(0);
        //     vlog
        // }

        // fn rl(&self) -> parking_lot::lock_api::RwLockReadGuard<'_, RawRwLock, FileLog> {
        //     let df  = self.df.read().get_file(0);
        //     df
        // }
    }
}
