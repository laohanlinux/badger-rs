use async_channel::{Receiver, RecvError, Sender};
use atomic::Atomic;
use awaitgroup::{WaitGroup, Worker};
use bitflags::bitflags;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::BufMut;
use crc32fast::Hasher;
use drop_cell::defer;
use either::Either;
use libc::{endgrent, memchr};
use log::info;
use log::kv::Source;
use memmap::{Mmap, MmapMut};
use parking_lot::*;
use rand::random;
use serde_json::to_vec;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{read_dir, remove_file, File, OpenOptions};
use std::future::Future;
use std::io::{BufRead, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Deref, Index};
use std::path::Path;
use std::pin::Pin;
use std::process::{id, Output};
use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use std::{fmt, fs, io, ptr, thread};
use tabled::object::Entity::Cell;
use tokio::macros::support::thread_rng_n;

use crate::kv::{ArcKV, BoxKV, WeakKV, KV};
use crate::log_file::LogFile;
use crate::options::Options;
use crate::skl::BlockBytes;
use crate::table::iterator::BlockSlice;
use crate::types::{ArcMx, ArcRW, Channel, Closer, TArcMx, TArcRW, XArc};
use crate::y::{
    create_synced_file, is_eof, open_existing_synced_file, read_at, sync_directory, Decode, Encode,
};
use crate::Error::{Unexpected, ValueNoRewrite, ValueRejected};
use crate::{Error, Result, EMPTY_SLICE, META_SIZE};

/// Values have their first byte being byteData or byteDelete. This helps us distinguish between
/// a key that has never been seen and a key that has been explicitly deleted.
bitflags! {
    pub struct MetaBit: u8{
        /// Set if the key has been deleted.
        const BIT_DELETE = 1;
        /// Set if the value is NOT stored directly next to key.
        const BIT_VALUE_POINTER = 2;
        const BIT_UNUSED = 4;
        /// Set if the key is set using SetIfAbsent.
        const BIT_SET_IF_ABSENT = 8;
    }
}

const M: u64 = 1 << 20;

pub(crate) const MAX_KEY_SIZE: usize = 1 << 20;

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
#[derive(Default, Debug)]
pub struct Entry {
    pub(crate) key: Vec<u8>,
    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
    pub(crate) value: Vec<u8>,
    // If nonzero, we will check if existing casCounter matches.
    pub(crate) cas_counter_check: u64,
    // Fields maintained internally.
    pub(crate) offset: u32,
    pub(crate) cas_counter: AtomicU64,
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            meta: self.meta.clone(),
            user_meta: self.user_meta.clone(),
            value: self.value.clone(),
            cas_counter_check: self.cas_counter_check.clone(),
            offset: self.offset.clone(),
            cas_counter: AtomicU64::new(self.get_cas_counter()),
        }
    }
}

impl Entry {
    pub fn key(mut self, key: Vec<u8>) -> Self {
        self.key = key;
        self
    }

    pub fn value(mut self, value: Vec<u8>) -> Self {
        self.value = value;
        self
    }

    pub fn meta(mut self, meta: u8) -> Self {
        self.meta = meta;
        self
    }

    pub fn user_meta(mut self, user_meta: u8) -> Self {
        self.user_meta = user_meta;
        self
    }

    pub fn cas_counter_check(mut self, cas: u64) -> Self {
        self.cas_counter_check = cas;
        self
    }

    pub fn get_cas_counter(&self) -> u64 {
        self.cas_counter.load(Ordering::Relaxed)
    }
}

impl Entry {
    pub(crate) fn from_slice(cursor_offset: u32, m: &[u8]) -> Result<Entry> {
        let mut entry = Entry::default();
        let mut h = Header::default();
        h.dec(&mut Cursor::new(
            &m[cursor_offset as usize..cursor_offset as usize + Header::encoded_size()],
        ))?;
        entry.key = vec![0u8; h.k_len as usize];
        entry.value = vec![0u8; h.v_len as usize];
        entry.meta = h.meta;
        entry.offset = cursor_offset as u32;
        entry.cas_counter = AtomicU64::new(h.cas_counter);
        entry.user_meta = h.user_mata;
        entry.cas_counter_check = h.cas_counter_check;
        let mut start = cursor_offset as usize + Header::encoded_size();
        entry
            .key
            .extend_from_slice(&m[start..start + h.k_len as usize]);
        start += h.k_len as usize;
        entry
            .value
            .extend_from_slice(&m[start..start + h.v_len as usize]);
        start += h.v_len as usize;
        let crc32 = Cursor::new(&m[start..start + 4]).read_u32::<BigEndian>()?;
        Ok(entry)
    }

    fn to_string(&self, prefix: &str) -> String {
        format!("{} {}", prefix, self)
    }
}

impl Encode for Entry {
    fn enc(&self, wt: &mut dyn Write) -> Result<usize> {
        let mut h = Header::default();
        h.k_len = self.key.len() as u32;
        h.v_len = self.value.len() as u32;
        h.meta = self.meta;
        h.user_mata = self.user_meta;
        h.cas_counter = self.cas_counter.load(Ordering::Relaxed);
        h.cas_counter_check = self.cas_counter_check;
        let mut buffer = vec![0u8; Header::encoded_size() + (h.k_len + h.v_len + 4) as usize];
        // write header
        let mut start = 0;
        h.enc(&mut Cursor::new(&mut buffer[start..]))?;
        // write key
        start += Header::encoded_size();
        (&mut buffer[start..]).write(&self.key)?;
        // write value
        start += h.k_len as usize;
        (&mut buffer[start..]).write(&self.value)?;
        start += h.v_len as usize;
        let mut hasher = Hasher::new();
        hasher.update(&buffer[..start]);
        let check_sum = hasher.finalize();
        // write crc32
        (&mut buffer[start..]).write_u32::<BigEndian>(check_sum)?;
        wt.write_all(&buffer)?;
        Ok(buffer.len())
    }
}

impl Decode for Entry {
    fn dec(&mut self, rd: &mut dyn Read) -> Result<()> {
        let mut h = Header::default();
        let mut buffer = vec![0u8; Header::encoded_size()];
        let sz = rd.read(&mut buffer)?;
        assert_eq!(sz, buffer.len());
        h.dec(&mut Cursor::new(&buffer))?;
        self.key = vec![0u8; h.k_len as usize];
        self.value = vec![0u8; h.v_len as usize];
        self.meta = h.meta;
        // self.offset = cursor_offset as u32;
        self.cas_counter = AtomicU64::new(h.cas_counter);
        self.user_meta = h.user_mata;
        self.cas_counter_check = h.cas_counter_check;
        let sz = rd.read(&mut self.key)?;
        assert_eq!(sz, h.k_len as usize);
        let sz = rd.read(&mut self.value)?;
        assert_eq!(sz, h.v_len as usize);
        // TODO check crc32
        let crc32 = rd.read_u32::<BigEndian>()?;
        Ok(())
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entry")
            .field("key", &String::from_utf8_lossy(&self.key).to_string())
            .field("meta", &self.meta)
            .field("user_meta", &self.user_meta)
            .field("offset", &self.offset)
            .field("value", &self.value)
            .field("case=", &self.get_cas_counter())
            .field("check", &self.cas_counter_check)
            .finish()
    }
}

#[derive(Debug, Clone, Default, Copy)]
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

#[derive(Clone)]
pub(crate) struct EntryType {
    entry: Entry,
    fut_ch: Channel<Result<()>>,
}

impl Debug for EntryType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntryType")
            .field("entry", &self.entry)
            .finish()
    }
}

impl EntryType {
    pub(crate) fn entry(&self) -> &Entry {
        &self.entry
    }

    pub(crate) fn mut_entry(&mut self) -> &mut Entry {
        &mut self.entry
    }

    pub(crate) fn ret(&self) -> Receiver<Result<()>> {
        self.fut_ch.rx()
    }

    pub(crate) async fn set_resp(&self, ret: Result<()>) {
        self.fut_ch.tx().send(ret).await.unwrap();
    }

    pub(crate) fn get_resp_channel(&self) -> Channel<Result<()>> {
        self.fut_ch.clone()
    }
}

impl From<Entry> for EntryType {
    fn from(value: Entry) -> Self {
        Self {
            entry: value,
            fut_ch: Channel::new(1),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    // Input values
    pub(crate) entries: Vec<EntryType>,
    // Output Values and wait group stuff below
    pub(crate) ptrs: Vec<Arc<Atomic<Option<ValuePointer>>>>,
}

impl Default for Request {
    fn default() -> Self {
        Request {
            entries: Default::default(),
            ptrs: Default::default(),
        }
    }
}

impl Request {
    pub(crate) async fn set_entries_resp(&self, ret: Result<()>) {
        for entry in self.entries.iter() {
            info!("set resp");
            entry.set_resp(ret.clone()).await;
        }
    }

    pub async fn get_first_err(&self) -> Result<()> {
        if let Some(ret) = self.entries.get(0) {
            ret.ret().recv().await.unwrap()
        } else {
            Ok(())
        }
    }

    pub async fn get_errs(&self) -> Vec<Result<()>> {
        let mut res = vec![];
        for entry in self.entries.iter() {
            let ch = entry.fut_ch.rx();
            let ret = ch.recv().await.unwrap();
            res.push(ret);
        }
        res
    }

    pub fn get_resp_channel(&self) -> Vec<Channel<Result<()>>> {
        self.entries
            .iter()
            .map(|ty| ty.fut_ch.clone())
            .collect::<Vec<_>>()
    }

    fn get_ptrs(&self) -> Vec<Arc<Atomic<Option<ValuePointer>>>> {
        self.ptrs.clone()
    }

    fn get_ptr(&self, i: usize) -> Option<&Arc<Atomic<Option<ValuePointer>>>> {
        self.ptrs.get(i)
    }
}

pub struct ValueLogCore {
    dir_path: Box<String>,
    pub(crate) max_fid: AtomicU32,
    // TODO
    // guards our view of which files exist, which to be deleted, how many active iterators
    pub(crate) files_log: TArcRW<()>,
    vlogs: TArcRW<HashMap<u32, TArcRW<LogFile>>>,
    // TODO It is not good idea that use raw lock for Arc<RwLock<LogFile>>, it maybe lock AsyncRuntime thread.
    dirty_vlogs: TArcRW<HashSet<u32>>,
    // TODO why?
    // A refcount of iterators -- when this hits zero, we can delete the files_to_be_deleted. Why?
    num_active_iterators: AtomicI32,
    writable_log_offset: AtomicU32,
    buf: TArcRW<Cursor<Vec<u8>>>,
    opt: Options,
    kv: BoxKV,
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
            buf: Arc::new(tokio::sync::RwLock::new(Cursor::new(Vec::with_capacity(
                1 << 12,
            )))),
            opt: Default::default(),
            kv: BoxKV::new(ptr::null_mut()),
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
        let mut _mmap = unsafe { Mmap::map(vlog_file.fd.as_ref().unwrap())? };
        vlog_file._mmap.replace(_mmap.into());
        Ok(vlog_file)
    }

    // TODO Use Arc<KV> to replace it
    pub(crate) async fn open(&mut self, kv: *const KV, opt: Options) -> Result<()> {
        self.dir_path = opt.value_dir.clone();
        self.opt = opt;
        self.kv = BoxKV::new(kv);
        self.open_create_files().await?;
        // todo add garbage and metrics
        self.garbage_ch = Channel::new(1);
        Ok(())
    }

    fn get_kv(&self) -> &KV {
        unsafe { &*self.kv.kv }
    }

    pub async fn close(&self) -> Result<()> {
        info!("Stopping garbage collection of values.");
        let mut vlogs = self.vlogs.write().await;
        for vlog in vlogs.iter() {
            let mut lf = vlog.1.write().await;
            if *vlog.0 == self.max_fid.load(Ordering::Acquire) {
                info!("close vlog: {}", vlog.0);
                let _mmap = lf._mmap.take().unwrap();
                _mmap.get_mut_mmap().flush()?;
                lf.fd
                    .as_mut()
                    .unwrap()
                    .set_len(self.writable_log_offset.load(Ordering::Acquire) as u64)?;
            }
        }
        vlogs.clear();
        Ok(())
    }

    async fn open_create_files(&self) -> Result<()> {
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
                .await
                .insert(fid as u32, TArcRW::new(tokio::sync::RwLock::new(log_file)));
            if fid > max_fid {
                max_fid = fid;
            }
        }
        self.max_fid.store(max_fid as u32, Ordering::Release);

        // Open all previous log files are read only. Open the last log file
        // as read write.
        let mut vlogs = self.vlogs.write().await;
        for (fid, fp) in vlogs.iter() {
            if *fid == max_fid as u32 {
                let fpath = self.fpath(*fid as u32);
                let _fp = open_existing_synced_file(&fpath, self.opt.sync_writes)?;
                fp.write().await.fd.replace(_fp);
            } else {
                fp.write().await.open_read_only()?;
            }
        }
        // If no files are found, creating a new file.
        if vlogs.is_empty() {
            let log_file = self.create_vlog_file(0)?;
            info!("Create zero vlog {}!!", log_file._path.as_ref());
            let vlog = TArcRW::new(tokio::sync::RwLock::new(log_file));
            vlogs.insert(0, vlog);
        }
        Ok(())
    }

    // Read the value log at given location.
    pub async fn read(
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
            if (h.meta & MetaBit::BIT_DELETE.bits()) != 0 {
                // Tombstone key
                return consumer(&vec![]);
            }
            let n = Header::encoded_size() + h.k_len as usize;
            consumer(&buffer[n..n + h.v_len as usize])
        })
        .await
    }

    pub async fn async_read(
        &self,
        vp: &ValuePointer,
        consumer: impl FnMut(&[u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
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
        self.async_read_bytes(vp, consumer).await?;
        Ok(())
    }

    /// Replays the value log. The kv provide is only valid for the lifetime of function call.
    pub async fn replay(
        &self,
        vp: &ValuePointer,
        mut f: impl for<'a> FnMut(
            &'a Entry,
            &'a ValuePointer,
        ) -> Pin<Box<dyn Future<Output = Result<bool>> + 'a>>,
    ) -> Result<()> {
        let vlogs = self.pick_log_guard().await;
        info!("Seeking at value pointer: {:?}", vp);
        let offset = vp.offset + vp.len;
        // Find the max file to recover
        for id in vlogs.fids {
            if id < vp.fid {
                continue;
            }
            let mut of = offset;
            if id > vp.fid {
                // It is very import that maybe the lasted memory table are not persistent at disk.
                of = 0;
            }
            let mut log_file = vlogs.vlogs.get(&id).unwrap().write().await;
            log_file.iterate(of, &mut f).await?;
        }
        // Seek to the end to start writing.
        let last_file = vlogs
            .vlogs
            .get(&self.max_fid.load(Ordering::Acquire))
            .unwrap();
        let last_offset = last_file
            .write()
            .await
            .fd
            .as_mut()
            .unwrap()
            .seek(SeekFrom::End(0))?;
        self.writable_log_offset
            .store(last_offset as u32, Ordering::Release);
        info!(
            "After recover, max_id:{}, last_offset:{}",
            self.max_fid.load(Ordering::Relaxed),
            last_offset
        );
        Ok(())
    }

    pub(crate) fn incr_iterator_count(&self) {
        self.num_active_iterators.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) async fn decr_iterator_count(&self) -> Result<()> {
        // TODO add share lock.
        let old = self.num_active_iterators.fetch_sub(1, Ordering::Relaxed);
        if old != 1 {
            // the lasted reference
            return Ok(());
        }
        let mut lfs = vec![];
        for dirty_vlog in self.dirty_vlogs.read().await.iter() {
            // TODO
            let vlog = self.vlogs.write().await.remove(dirty_vlog).unwrap();
            lfs.push(vlog);
        }
        self.dirty_vlogs.write().await.clear();
        for lf in lfs {
            self.delete_log_file_by_fid(lf).await?;
        }
        Ok(())
    }

    // Delete log file after no refernece of LogFile
    async fn delete_log_file_by_fid(&self, log_file: TArcRW<LogFile>) -> Result<()> {
        let mut vlog_file_wl = log_file.write().await;
        if let Some(mp) = vlog_file_wl._mmap.take() {
            mp.get_mut_mmap().flush()?;
        }
        if let Some(fp) = vlog_file_wl.fd.take() {
            fp.sync_all()?;
        }
        remove_file(self.fpath(vlog_file_wl.fid))?;
        Ok(())
    }

    // sync is thread-unsafe and should not be called concurrently with write.
    pub(crate) async fn sync(&self) -> Result<()> {
        if self.opt.sync_writes {
            return Ok(());
        }
        let cur_wt_vlog = self
            .pick_log_guard()
            .await
            .vlogs
            .get(&self.max_fid.load(Ordering::Acquire))
            .unwrap();
        // todo add sync directory meta
        // sync_dir_async()
        Ok(())
    }

    async fn read_value_bytes(
        &self,
        vp: &ValuePointer,
        mut consumer: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let log_file = self.pick_log_by_vlog_id(&vp.fid).await;
        let lf = log_file.read().await;
        let buffer = lf.read(vp)?;
        consumer(buffer)
    }

    async fn async_read_bytes(
        &self,
        vp: &ValuePointer,
        mut consumer: impl FnMut(&[u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    ) -> Result<()> {
        let mut vlog = self.pick_log_by_vlog_id(&vp.fid).await;
        let mut buffer = vlog.read().await;
        let buffer = buffer.read(&vp)?;
        let mut h = Header::default();
        h.dec(&mut Cursor::new(&buffer[0..Header::encoded_size()]))?;
        if (h.meta & MetaBit::BIT_DELETE.bits) != 0 {
            // Tombstone key
            consumer(&EMPTY_SLICE).await
        } else {
            let mut n = Header::encoded_size() + h.k_len as usize;
            consumer(&buffer[n..n + h.v_len as usize]).await
        }
    }

    // write is thread-unsafe by design and should not be called concurrently.
    pub(crate) async fn write(&self, reqs: Vec<Request>) -> Result<()> {
        defer! {info!("Finished write value log");}
        let cur_vlog_file = self
            .pick_log_by_vlog_id(&self.max_fid.load(Ordering::Acquire))
            .await;
        let mut cur_vlog_wl = cur_vlog_file.write().await;
        let cur_fid = cur_vlog_wl.fid;
        let reqs_count = reqs.len();
        for mut req in reqs.into_iter() {
            for (idx, mut entry) in req.entries.into_iter().enumerate() {
                if !self.opt.sync_writes && entry.entry().value.len() < self.opt.value_threshold {
                    // No need to write to value log.
                    // WARN: if mt not flush into disk but process abort, that will discard data(the data not write into vlog that WAL file)
                    req.ptrs[idx] = Arc::new(Atomic::new(None));
                    continue;
                }

                info!(
                    "Write a # {:?} into vlog file, mmap position: {}",
                    String::from_utf8(entry.entry().key.clone()).unwrap(),
                    self.buf.read().await.position(),
                );
                let mut ptr = ValuePointer::default();
                ptr.fid = cur_fid;
                // Use the offset including buffer length so far.
                ptr.offset = self.writable_log_offset.load(Ordering::Acquire)
                    + self.buf.read().await.position() as u32;
                let mut buf = self.buf.write().await;
                let mut entry = entry.mut_entry();
                entry.enc(&mut buf.get_mut()).unwrap();
                if buf.get_ref().is_empty() {
                    info!("It should be not happen!!!!!!!");
                }
                ptr.len = buf.get_ref().len() as u32 - ptr.offset;
                req.ptrs[idx].store(Some(ptr), Ordering::Release);
            }
        }
        {
            if self.buf.read().await.get_ref().is_empty() {
                info!("Nothing to write, buffer is empty!");
                return Ok(());
            }
            let mut buffer = self.buf.write().await;
            let fp_wt = cur_vlog_wl.mut_mmap();
            // write value ponter into vlog file. (Just only write to mmap)
            let n = fp_wt.as_mut().write(buffer.get_ref())?;
            assert_eq!(n, buffer.get_ref().len());
            // todo add metrics
            // update log
            self.writable_log_offset
                .fetch_add(n as u32, Ordering::Release);

            info!(
                "Flushing {} requests of total size: {}",
                reqs_count,
                self.writable_log_offset.load(Ordering::Acquire),
            );
            // clear buffer
            buffer.get_mut().clear();
            if self.writable_log_offset.load(Ordering::Acquire)
                > self.opt.value_log_file_size as u32
            {
                cur_vlog_wl.done_writing(self.writable_log_offset.load(Ordering::Acquire))?;
                let new_id = self.max_fid.fetch_add(1, Ordering::Release);
                assert!(new_id < 1 << 16, "newid will overflow u16: {}", new_id);
                *cur_vlog_wl =
                    self.create_mmap_vlog_file(new_id, 2 * self.opt.value_log_file_size)?;
            }
            Ok(())
        }
    }

    // rewrite the log_file
    async fn rewrite(&self, lf: TArcRW<LogFile>, x: &KV) -> Result<()> {
        let max_fid = self.max_fid.load(Ordering::Relaxed);
        assert!(
            lf.read().await.fid < max_fid,
            "fid to move: {}. Current max fid: {}",
            lf.read().await.fid,
            max_fid
        );
        // TODO add metrics

        let mut offset = 0;
        let mut count = 0;
        let kv = self.get_kv();
        let mut write_batch = Vec::with_capacity(1000);
        loop {
            let (mut entries, next) = lf.read().await.read_entries(offset, 1).await?;
            if entries.is_empty() {
                info!("not anything need to rewrite");
                break;
            }
            offset += next;
            count += 1;
            if count % 1000 == 0 {
                info!("Processing entry {}", count);
            }
            // TODO don't need decode vptr
            let entry = &mut entries[0].0;
            let vs = kv._get(&entry.key);
            if let Err(ref err) = vs {
                if err.is_not_found() {
                    info!(
                        "REWRITE=> not found the value, {}",
                        String::from_utf8_lossy(&entry.key)
                    );
                    continue;
                }
                return Err(err.clone());
            }
            let vs = vs.unwrap();
            // It should be not happen, if the value is deleted
            if vs.meta & MetaBit::BIT_DELETE.bits() > 0 {
                info!(
                    "REWRITE=> {} has been deleted",
                    String::from_utf8_lossy(&entry.key)
                );
                continue;
            }
            if vs.meta & MetaBit::BIT_VALUE_POINTER.bits() < 0 {
                info!(
                    "REWRITE=> {} has been skipped, meta: {}",
                    String::from_utf8_lossy(&entry.key),
                    entry.meta,
                );
                continue;
            }
            if vs.value.is_empty() {
                info!(
                    "REWRITE=> {} is empty value",
                    String::from_utf8_lossy(&entry.key)
                );
                return Err(format!("Empty value: {:?}", vs).into());
            }
            // the lasted vptr
            let mut vptr = ValuePointer::default();
            vptr.dec(&mut Cursor::new(&vs.value)).unwrap();
            if vptr.fid > lf.read().await.fid {
                continue;
            }
            if vptr.offset > entry.offset {
                continue;
            }
            assert_eq!(vptr.fid, lf.read().await.fid);
            assert_eq!(vptr.offset, entry.offset);
            {
                // This new entry only contains the key, and a pointer to the value.
                let mut ne = Entry::default();
                // TODO why?
                if entry.meta == MetaBit::BIT_SET_IF_ABSENT.bits() {
                    // If we rewrite this entry without removing BitSetIfAbsent, LSM would see that
                    // the key is already present, which would be this same entry and won't update
                    // the vptr to point to the new file.
                    entry.meta = 0;
                }
                assert_eq!(entry.meta, 0, "Got meta: 0");
                ne.meta = entry.meta;
                ne.user_meta = entry.user_meta;
                ne.key = entry.key.clone(); // TODO avoid copy
                ne.value = entry.value.clone();
                // CAS counter check. Do not rewrite if key has a newer value.
                ne.cas_counter_check = vs.cas_counter;
                write_batch.push(ne);
            }
        }
        if write_batch.is_empty() {
            info!("REWRITE: nothing to rewrite.");
            return Ok(());
        }
        info!(
            "REWRITE: request has {} entries, size {}",
            write_batch.len(),
            count
        );
        info!("REWRITE: Removing fid: {}", lf.read().await.fid);
        kv.batch_set(write_batch).await;
        info!("REWRITE: Processed {} entries in total", count);
        info!("REWRITE: Removing fid: {}", lf.read().await.fid);
        let mut deleted_file_now = false;
        // Entries written to LSM. Remove the older file now.
        {
            // Just a sanity-check.
            let mut vlogs = self.vlogs.write().await;
            if !vlogs.contains_key(&lf.read().await.fid) {
                return Err(format!("Unable to find fid: {}", lf.read().await.fid).into());
            }
            // TODO Why?
            if self.num_active_iterators.load(Ordering::Relaxed) == 0 {
                vlogs.remove(&lf.read().await.fid);
                deleted_file_now = true;
            } else {
                self.dirty_vlogs
                    .write()
                    .await
                    .insert(lf.read().await.fid.clone());
            }
        }
        if deleted_file_now {}
        Ok(())
    }

    async fn pick_log(&self) -> Option<TArcRW<LogFile>> {
        let vlogs_guard = self.pick_log_guard().await;
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

    pub(crate) async fn pick_log_by_vlog_id(&self, id: &u32) -> TArcRW<LogFile> {
        let pick_vlogs = self.pick_log_guard().await;
        let vlogs = pick_vlogs.vlogs.get(id);
        let vlog = vlogs.unwrap();
        vlog.clone()
    }

    // Note: it not including dirty file
    async fn pick_log_guard(&self) -> PickVlogsGuardsReadLock {
        let vlogs = self.vlogs.read().await;
        let vlogs_fids = vlogs.keys().map(|fid| *fid).collect::<HashSet<_>>();
        let dirty_vlogs = self.dirty_vlogs.read().await;
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

    pub(crate) async fn wait_on_gc(&self, lc: Closer) {
        defer! {lc.done()}
        lc.wait().await; // wait for lc to be closed.
                         // Block any GC in progress to finish, and don't allow any more writes to runGC by filling up
                         // the channel of size 1.
        self.garbage_ch.send(()).await.unwrap();
    }

    // only one gc worker
    pub async fn trigger_gc(&self, gc_threshold: f64) -> Result<()> {
        return match self.garbage_ch.try_send(()) {
            Ok(()) => {
                let ok = self.do_run_gc(gc_threshold).await;
                self.garbage_ch.recv().await.unwrap();
                ok
            }
            Err(err) => Err(Error::ValueRejected),
        };
    }

    pub async fn do_run_gc(&self, gc_threshold: f64) -> Result<()> {
        #[derive(Debug, Default)]
        struct Reason {
            total: f64,
            keep: f64,
            discard: f64,
        }
        let mut reason = ArcMx::new(parking_lot::Mutex::new(Reason::default()));
        let mut window = 100.0; //  limit 100M for gc every time
        let mut count = 0;
        // Pick a random start point for the log.
        let mut skip_first_m =
            thread_rng_n((self.opt.value_log_file_size / M) as u32) as f64 - window;
        let mut skipped = 0.0;
        let mut start = SystemTime::now();
        // Random pick a vlog file for gc
        let lf = self.pick_log().await.ok_or(Error::ValueNoRewrite)?;
        let fid = lf.read().await.fid;
        // Ennnnnnn
        let vlog = unsafe { &*(self as *const ValueLogCore as *mut ValueLogCore) };
        lf.clone()
            .read()
            .await
            .iterate_by_offset(0, &mut |entry, vptr| {
                let kv = vlog.get_kv();
                let reason = reason.clone();
                Box::pin(async move {
                    let mut reason = reason.lock();
                    let esz = vptr.len as f64 / (1 << 20) as f64; // in MBs, +4 for the CAS stuff.
                    skipped += esz;
                    if skipped < skip_first_m {
                        // Skip
                        return Ok(true);
                    }
                    count += 1;
                    // TODO confiure
                    if count % 100 == 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    reason.total += esz;
                    if reason.total > window {
                        // return Err(Error::StopGC);
                        return Ok(false);
                    }
                    if start.elapsed().unwrap().as_secs() > 10 {
                        // return Err(Error::StopGC);
                        return Ok(false);
                    }
                    // Get the late value
                    let vs = kv._get(&entry.key)?;
                    if (vs.meta & MetaBit::BIT_DELETE.bits()) > 0 {
                        // Key has been deleted. Discard.
                        reason.discard += esz;
                        return Ok(true); // Continue
                    }
                    if (vs.meta & MetaBit::BIT_VALUE_POINTER.bits()) == 0 {
                        // Value is stored alongside key. Discard.
                        reason.discard += esz;
                        return Ok(true);
                    }
                    // Value is still present in value log.
                    assert!(!vs.value.is_empty());
                    let mut vptr = vptr.clone(); // TODO avoid copy
                    vptr.dec(&mut io::Cursor::new(vs.value))?;
                    if vptr.fid > fid {
                        // Value is present in a later log. Discard.
                        reason.discard += esz;
                        return Ok(true);
                    }

                    if vptr.offset > entry.offset {
                        // Value is present in a later offset, but in the same log.
                        reason.discard += esz;
                        return Ok(true);
                    }

                    if vptr.fid == fid && vptr.offset == entry.offset {
                        // This is still the active entry, This would need to be rewritten.
                        reason.keep += esz;
                    } else {
                        // TODO Maybe abort gc process, it should be happen
                        info!("Reason={:?}", reason);
                        let err = vlog
                            .read_value_bytes(&vptr, |buf| {
                                let mut unexpect_entry = Entry::default();
                                unexpect_entry.dec(&mut io::Cursor::new(buf))?;
                                unexpect_entry.offset = vptr.offset;
                                if unexpect_entry.get_cas_counter() == entry.get_cas_counter() {
                                    info!("Latest Entry Header in LSM: {}", unexpect_entry);
                                    info!("Latest Entry in Log: {}", entry);
                                }
                                Ok(())
                            })
                            .await;
                        if err.is_err() {
                            return Err("Stop iteration".into());
                        }
                    }
                    Ok(true)
                })
            })
            .await?;
        let reason = reason.lock();
        info!("Fid: {} Data status={:?}", fid, reason);
        if reason.total < 10.0 || reason.discard < gc_threshold * reason.total {
            info!("Skipping GC on fid: {}", lf.read().await.fid);
            return Err(Error::ValueNoRewrite);
        }

        info!("REWRITING VLOG {}", lf.read().await.fid);
        self.rewrite(lf, self.get_kv()).await?;
        Ok(())
    }
}

struct PickVlogsGuardsReadLock<'a> {
    vlogs: tokio::sync::RwLockReadGuard<'a, HashMap<u32, TArcRW<LogFile>>>,
    fids: Vec<u32>,
}

#[test]
fn it() {
    // let mut buffer = Cursor::new(Vec::with_capacity(1<<10));
    // buffer.write(b"hello").unwrap();
    // println!("{:?}", buffer.get_ref());
    // buffer.get_mut().clear();
    // println!("{:?}", buffer.get_ref());
}
