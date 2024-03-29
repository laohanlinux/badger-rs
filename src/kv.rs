use crate::iterator::{IteratorExt, IteratorOptions, KVItem, KVItemInner};
use crate::levels::LevelsController;
use crate::manifest::{open_or_create_manifest_file, ManifestFile};
use crate::options::Options;
use crate::table::builder::Builder;
use crate::table::iterator::IteratorItem;
use crate::table::table::{new_file_name, Table, TableCore};
use crate::types::{ArcMx, Channel, Closer, TArcRW, XArc, XWeak};
use crate::value_log::{
    Entry, EntryType, MetaBit, Request, ValueLogCore, ValuePointer, MAX_KEY_SIZE,
};
use crate::y::{async_sync_directory, create_synced_file, Encode, Result, ValueStruct};
use crate::Error::{NotFound, Unexpected};
use crate::{
    event, hex_str, Decode, Error, MergeIterOverBuilder, Node, SkipList, SkipListManager,
    UniIterator, Xiterator,
};

use atomic::Atomic;
use crossbeam_epoch::{Owned, Shared};
use drop_cell::defer;
use fs2::FileExt;

use log::{debug, error, info, warn};

use parking_lot::Mutex;

use std::fs::File;
use std::fs::OpenOptions;
use std::future::Future;
use std::io::{Cursor, Write};

use std::path::Path;
use std::pin::Pin;

use crate::pb::backup::KVPair;
use libc::difftime;
use rand::random;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{fmt, string, vec};
use tokio::fs::create_dir_all;
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, RwLockWriteGuard};

///
pub const _BADGER_PREFIX: &[u8; 8] = b"!badger!";
/// Prefix for internal keys used by badger.
pub const _HEAD: &[u8; 12] = b"!badger!head"; // For Storing value offset for replay.

pub const KV_WRITE_CH_CAPACITY: usize = 1000;

#[derive(Clone)]
pub struct Closers {
    pub update_size: Closer,
    pub compactors: Closer,
    pub mem_table: Closer,
    // Wait flush job exit
    pub writes: Closer,
    pub value_gc: Closer,
}

#[derive(Debug, Clone)]
struct FlushTask {
    mt: Option<SkipList>,
    vptr: ValuePointer,
}

impl FlushTask {
    fn must_mt(&self) -> &SkipList {
        self.mt.as_ref().unwrap()
    }
}

/// A builder for KV building.
pub struct KVBuilder {
    opt: Options,
    kv: BoxKV,
}

/// Manage key/value
#[doc(hidden)]
#[derive(Clone)]
pub struct KVCore {
    pub opt: Options,
    pub vlog: Option<Arc<ValueLogCore>>,
    pub vptr: crossbeam_epoch::Atomic<ValuePointer>,
    pub manifest: Arc<RwLock<ManifestFile>>,
    lc: Option<LevelsController>,
    flush_chan: Channel<FlushTask>,
    pub notify_try_compact_chan: Channel<()>,
    // Notify zero has compact, so we should try add table into zero tables
    pub zero_level_compact_chan: Channel<()>,
    notify_write_request_chan: Channel<()>,
    // write_chan: Channel<Request>,
    dir_lock_guard: Arc<File>,
    value_dir_guard: Arc<File>,
    pub closers: Closers,
    // Our latest (actively written) in-memory table.
    pub mem_st_manger: Arc<SkipListManager>,
    // Add here only AFTER pushing to flush_ch
    pub write_ch: Channel<Request>,
    // Incremented in the non-concurrently accessed write loop.  But also accessed outside. So
    // we use an atomic op.
    pub(crate) last_used_cas_counter: Arc<AtomicU64>,
    share_lock: TArcRW<()>,
}

impl Drop for KVCore {
    fn drop(&mut self) {
        warn!("Drop kv");
    }
}

impl std::fmt::Debug for KVCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KVCore").finish()
    }
}

pub(crate) struct BoxKV {
    pub kv: *const KVCore,
}

unsafe impl Send for BoxKV {}

unsafe impl Sync for BoxKV {}

impl BoxKV {
    pub(crate) fn new(kv: *const KVCore) -> BoxKV {
        BoxKV { kv }
    }
}

impl KVCore {
    /// Walk the directory, return sst and vlog total size
    async fn walk_dir(dir: &str) -> Result<(u64, u64)> {
        let mut lsm_size = 0;
        let mut vlog_size = 0;
        let mut entries = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let meta = entry.metadata().await?;
            if meta.is_dir() {
                continue;
            }
            if entry.file_name().to_str().unwrap().ends_with(".sst") {
                lsm_size += meta.len();
            } else if entry.file_name().to_str().unwrap().ends_with(".vlog") {
                vlog_size += meta.len();
            }
        }
        Ok((lsm_size, vlog_size))
    }

    // get returns the value in `mem_table` or disk for given key.
    // Note that value will include meta byte.
    #[inline]
    pub(crate) fn _get(&self, key: &[u8]) -> Result<ValueStruct> {
        event::get_metrics().num_gets.inc();
        let p = crossbeam_epoch::pin();
        let tables = self.get_mem_tables(&p);
        // Must dereference skip list
        let decref_tables = || {
            tables
                .iter()
                .for_each(|tb| unsafe { tb.as_ref().unwrap().decr_ref() })
        };
        defer! {decref_tables()};

        crate::event::get_metrics().num_gets.inc(); // TODO add metrics

        for tb in tables.iter() {
            event::get_metrics().num_mem_tables_gets.inc();
            let st = unsafe { tb.as_ref().unwrap() };
            let vs = st.get(key);
            crate::event::get_metrics().num_mem_tables_gets.inc();
            if vs.is_none() {
                continue;
            }
            let vs = vs.unwrap();
            if (vs.meta & MetaBit::BIT_DELETE.bits()) > 0 {
                return Err(Error::NotFound);
            }
            if !vs.value.is_empty() {
                return Ok(vs);
            }
        }
        //#[cfg(test)]
        //info!(
        //  "found from disk table, key #{}, {:?}",
        // crate::y::hex_str(key),
        // self.must_lc()
        //);
        let vs = self.must_lc().get(key).ok_or(NotFound)?;
        if (vs.meta & MetaBit::BIT_DELETE.bits()) > 0 {
            return Err(Error::NotFound);
        }
        Ok(vs)
    }

    // Sets the provided value for a given key. If key is not present, it is created.  If it is
    // present, the existing value is overwritten with the one provided.
    // Along with key and value, Set can also take an optional userMeta byte. This byte is stored
    // alongside the key, and can be used as an aid to interpret the value or store other contextual
    // bits corresponding to the key-value pair.
    pub(crate) async fn set(&self, key: Vec<u8>, value: Vec<u8>, user_meta: u8) -> Result<()> {
        let res = self
            .batch_set(vec![Entry::default()
                .key(key)
                .value(value)
                .user_meta(user_meta)])
            .await;
        assert_eq!(res.len(), 1);
        res[0].to_owned()
    }

    // Called serially by only on goroutine
    async fn write_requests(&self, reqs: Vec<Request>) -> Result<()> {
        if reqs.is_empty() {
            return Ok(());
        }
        let cost = SystemTime::now();
        defer! {
            let mills = SystemTime::now().duration_since(cost).unwrap().as_millis();
            // crate::event::METRIC_WRITE_REQUEST.with_label_values(&["cost", &mills.to_string()]);
        }
        info!(
            "write_requests called. Writing to value log, req_count: {}, entry_total: {}",
            reqs.len(),
            reqs.iter().fold(0, |acc, req| acc + req.entries.len()),
        );

        // CAS counter for all operations has to go onto value log. Otherwise, if it is just in
        // memtable for a long time, and following CAS operations use that as a check, when
        // replaying, we will think that these CAS operations should fail, when they are actually
        // valid.

        // There is code (in flush_mem_table) whose correctness depends on us generating CAS Counter
        // values _before_ we modify s.vptr here.
        for req in reqs.iter() {
            let entries = &req.entries;
            let counter_base = self.new_cas_counter(entries.len() as u64);
            for (idx, entry) in entries.iter().enumerate() {
                entry
                    .entry()
                    .cas_counter
                    .store(counter_base + idx as u64, Ordering::Release);
            }
        }

        if let Err(err) = self.vlog.as_ref().unwrap().write(reqs.clone()).await {
            for req in reqs.iter() {
                req.set_entries_resp(Err(err.clone())).await;
            }
            return Err(err);
        }

        info!("Writing to memory table");
        let mut count = 0;
        let notify_write_request_chan = self.notify_write_request_chan.rx();
        for mut req in reqs.into_iter() {
            if req.entries.is_empty() {
                continue;
            }
            count += req.entries.len();
            while let Err(err) = self.ensure_room_for_write().await {
                debug!("failed to ensure room for write!, err:{}", err);
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                    _ = notify_write_request_chan.recv() => {
                        info!("receive a continue event, {}, {}", self.flush_chan.tx().is_full(), self.flush_chan.tx().is_empty());
                    },
                }
            }
            // warn!("Waiting for write lsm, count {}", count);
            self.update_offset(&mut req.ptrs).await;
            // It should not fail
            self.write_to_lsm(req).await.unwrap();
        }
        info!(
            "cost time at write request: {}ms",
            SystemTime::now().duration_since(cost).unwrap().as_millis()
        );
        info!("{} entries written", count);
        Ok(())
    }

    // async to flush memory table into zero level
    async fn flush_mem_table(&self, lc: Closer) -> Result<()> {
        defer! {lc.done()}
        defer! {info!("exit flush mem table")}
        while let Ok(task) = self.flush_chan.recv().await {
            info!("Receive a flush task, offset: {} !!!", task.vptr.offset);
            // after kv send empty mt, it will close flush_chan, so we should return the job.
            if task.mt.is_none() {
                warn!("receive a exit task!");
                break;
            }
            // TODO if is zero?
            if !task.vptr.is_zero() {
                let mut cur =
                    std::io::Cursor::new(vec![0u8; ValuePointer::value_pointer_encoded_size()]);
                let sz = task.vptr.enc(&mut cur).unwrap();
                let offset = cur.into_inner();
                assert_eq!(sz, offset.len());
                // CAS counter is needed and is desirable -- it's the first value log entry
                // we replay, so to speak, perhaps the only, and we use it to re-initialize
                // the CAS counter.
                //
                // The write loop generates CAS counter values _before_ it sets vptr.  It
                // is crucial that we read the cas counter here _after_ reading vptr.  That
                // way, our value here is guaranteed to be >= the CASCounter values written
                // before vptr (because they don't get replayed).
                warn!(
                    "Storing new vptr, fid:{}, len:{}, offset:{}",
                    task.vptr.fid, task.vptr.len, task.vptr.offset
                );
                let value = ValueStruct {
                    meta: 0,
                    user_meta: 0,
                    cas_counter: self.get_last_used_cas_counter(),
                    value: offset,
                };
                task.must_mt().put(_HEAD, value);
            }
            let fid = self.must_lc().reserve_file_id();
            let f_name = new_file_name(fid, &self.opt.dir);
            let fp = create_synced_file(&f_name, true)?;
            // Don't block just to sync the directory entry.
            // let task1 = async_sync_directory(self.opt.dir.clone().to_string());
            // let mut fp = tokio::fs::File::from_std(fp);
            // let task2 = write_level0_table(&task.mt.as_ref().unwrap(), &mut fp);
            // let (task1_res, task2_res) = tokio::join!(task1, task2);
            // task1_res?;
            // task2_res?;

            async_sync_directory(self.opt.dir.clone().to_string()).await?;
            let mut fp = tokio::fs::File::from_std(fp);
            write_level0_table(&task.mt.as_ref().unwrap(), &f_name, &mut fp).await?;

            debug!("Ready to advance im");
            let fp = fp.into_std().await;
            let tc = TableCore::open_table(fp, &f_name, self.opt.table_loading_mode)?;
            let tb = Table::from(tc);
            // We own a ref on tbl.
            self.must_lc().add_level0_table(tb.clone()).await?;
            debug!("Ready to advance im");
            let _ = self.share_lock.write().await;
            // This will incr_ref (if we don't error, sure)
            tb.decr_ref(); // releases our ref.
            self.mem_st_manger.advance_imm(task.must_mt()); // Update s.imm, need a lock.
            task.must_mt().decr_ref(); // Return memory
        }
        Ok(())
    }

    // Applies a list of `badger.entries`. If a request level error occurs it will be returned. Errors are also set on each
    // `Entry` and must be checked individually.
    // Check(kv.batch_set(entries))
    // for e in entries {
    //      Check(e.Error);
    // }
    pub(crate) async fn batch_set(&self, entries: Vec<Entry>) -> Vec<Result<()>> {
        let mut count = 0;
        let mut sz = 0u64;
        let mut res = vec![Ok(()); entries.len()];
        let mut req = Request::default();
        let mut req_index = vec![];
        let mut bad_count = 0;
        // packet entries into internal request message and filter invalid entry.
        for (i, entry) in entries.into_iter().enumerate() {
            if entry.key.len() > MAX_KEY_SIZE {
                res[i] = Err("Key too big".into());
                bad_count += 1;
                continue;
            }
            if entry.value.len() as u64 > self.opt.value_log_file_size {
                res[i] = Err("Value to big".into());
                bad_count += 1;
                continue;
            }
            {
                count += 1;
                sz += self.opt.estimate_size(&entry) as u64;
                req.entries.push(EntryType::from(entry));
                req.ptrs.push(Arc::new(Atomic::new(None)));
                req_index.push(i);
            }

            if count >= self.opt.max_batch_count || sz >= self.opt.max_batch_size {
                assert!(!self.write_ch.is_close());
                warn!("send tasks to write, entries: {}, count:{}, max_batch_count:{}, size:{}, max_batch_count:{}, free_count:{}", req.entries.len(), count, self.opt.max_batch_count, sz, self.opt.max_batch_size, self.must_mt().free_size());
                let cost = SystemTime::now();
                let resp_ch = req.get_resp_channel();
                // batch process requests
                self.write_ch.send(req).await.unwrap();
                event::get_metrics().num_puts.inc_by(count);
                {
                    count = 0;
                    sz = 0;
                    for (index, ch) in resp_ch.into_iter().enumerate() {
                        let entry_index = req_index[index];
                        res[entry_index] = ch.recv().await.unwrap();
                    }
                    req = Request::default();
                    req_index.clear();
                }
                info!(
                    "get response: {}ms",
                    SystemTime::now().duration_since(cost).unwrap().as_millis()
                );
            }
        }

        // process remaining requests
        if !req.entries.is_empty() {
            let resp_ch = req.get_resp_channel();
            self.write_ch.send(req).await.unwrap();
            event::get_metrics().num_puts.inc_by(count);
            {
                for (index, ch) in resp_ch.into_iter().enumerate() {
                    let entry_index = req_index[index];
                    res[entry_index] = ch.recv().await.unwrap();
                }
                req_index.clear();
            }
        }
        event::get_metrics().num_blocked_puts.inc_by(bad_count);
        res
    }

    async fn write_to_lsm(&self, req: Request) -> Result<()> {
        assert_eq!(req.entries.len(), req.ptrs.len());
        // defer! {info!("exit write to lsm")}

        #[cfg(test)]
        let tid = random::<u32>();

        for (i, pair) in req.entries.into_iter().enumerate() {
            let (entry, resp_ch) = pair.to_owned();

            #[cfg(test)]
            let debug_entry = entry.clone();

            let mut _old_cas = 0;

            if entry.cas_counter_check != 0 {
                // TODO FIXME if not found the key，maybe push something to resp_ch
                let old_value = self._get(&entry.key);
                if old_value.is_err() {
                    resp_ch.send(Err(old_value.unwrap_err())).await.unwrap();
                    continue;
                }

                let old_value = old_value.unwrap();

                _old_cas = old_value.cas_counter;

                // No need to decode existing value. Just need old CAS counter.
                if old_value.cas_counter != entry.cas_counter_check {
                    resp_ch.send(Err(Error::ValueCasMisMatch)).await.unwrap();

                    #[cfg(test)]
                    warn!(
                        "tid:{}, abort cas check, #{}, old_cas:{}, check_cas: {}, old_value:{}, new_val: {}",
                        tid,
                        crate::y::hex_str(&entry.key),
                        _old_cas, entry.cas_counter_check,
                        crate::y::hex_str(&old_value.value),
                        crate::y::hex_str(&entry.value));

                    continue;
                }
            }

            if entry.meta == MetaBit::BIT_SET_IF_ABSENT.bits() {
                // Someone else might have written a value, so lets check again if key exists.
                let exits = self._exists(&entry.key)?;
                // Value already exists. don't write.
                if exits {
                    resp_ch.send(Err(Error::ValueKeyExists)).await.unwrap();
                    continue;
                }
            }

            let key;
            let value;
            if self.should_write_value_to_lsm(&entry) {
                let cas = entry.get_cas_counter();
                let (_key, _value) = (entry.key, entry.value);
                key = _key;
                value = ValueStruct::new(_value, entry.meta, entry.user_meta, cas);
                // Will include deletion/tombstone case.
                debug!("Lsm ok, the value not at vlog file");
            } else {
                let ptr = req.ptrs.get(i).unwrap().load(Ordering::Relaxed);
                let ptr = ptr.unwrap();
                let mut wt = Cursor::new(vec![0u8; ValuePointer::value_pointer_encoded_size()]);
                ptr.enc(&mut wt).unwrap();
                let cas = entry.get_cas_counter();
                key = entry.key;
                value = ValueStruct::new(
                    wt.into_inner(),
                    entry.meta | MetaBit::BIT_VALUE_POINTER.bits(),
                    entry.user_meta,
                    cas,
                );
            }
            self.must_mt().put(&key, value);

            #[cfg(test)]
            debug!(
                "tid:{}, st:{}, key #{:?}, old_cas:{}, new_cas:{}, check_cas:{}, value #{:?} has inserted into SkipList!!!",
                tid,
                self.must_mt().id(),
                String::from_utf8_lossy(&key),
                _old_cas,
                debug_entry.get_cas_counter(),
                debug_entry.cas_counter_check,
                String::from_utf8_lossy(&debug_entry.value),
            );

            resp_ch.send(Ok(())).await.unwrap();
        }

        Ok(())
    }

    async fn ensure_room_for_write(&self) -> Result<()> {
        // defer! {info!("exit ensure room for write!")}
        // TODO a special global lock for this function
        let _ = self.share_lock.write().await;
        if self.must_mt().mem_size() < self.opt.max_table_size as u32 {
            return Ok(());
        }
        #[cfg(test)]
        warn!(
            "Will create a new SkipList, id: {}, cap: {}, free_count: {}, {} >= {}",
            self.must_mt().id(),
            self.must_mt().cap(),
            self.must_mt().free_size(),
            self.must_mt().mem_size(),
            self.opt.max_table_size
        );
        // A nil mt indicates that KV is being closed.
        assert!(!self.must_mt().empty());
        let flush_task = FlushTask {
            mt: Some(self.must_mt().clone()),
            vptr: self.must_vptr(),
        };
        let ret = self.flush_chan.try_send(flush_task);
        if ret.is_err() {
            //info!("No room for write, {:?}", ret.unwrap_err());
            return Err(Unexpected("No room for write".into()));
        }

        info!("Flushing value log to disk if async mode.");
        // Ensure value log is synced to disk so this memtable's contents wouldn't be lost.
        self.must_vlog().sync().await?;
        info!(
            "Flushing memtable, mt.size={} size of flushChan: {}",
            self.must_mt().mem_size(),
            self.flush_chan.tx().len()
        );
        // We manage to push this task. Let's modify imm.
        self.mem_st_manger.swap_st(self.opt.clone());
        // New memtable is empty. We certainly have room.
        Ok(())
    }

    async fn update_offset(&self, ptrs: &mut Vec<Arc<Atomic<Option<ValuePointer>>>>) {
        // #[cfg(test)]
        // warn!("Ready to update offset");
        let mut ptr = ValuePointer::default();
        for tmp_ptr in ptrs.iter().rev() {
            let tmp_ptr = tmp_ptr.load(Ordering::Acquire);
            if tmp_ptr.is_none() || tmp_ptr.unwrap().is_zero() {
                continue;
            }
            ptr = tmp_ptr.unwrap();
            warn!("Update offset, value pointer: {:?}", ptr);
            break;
        }

        if ptr.is_zero() {
            // #[cfg(test)]
            // warn!("ptr is null");
            return;
        }

        let _ = self.share_lock.write().await;
        self.vptr.store(Owned::new(ptr), Ordering::Release);
    }

    // Returns the current `mem_tables` and get references(here will incr mem table reference).
    fn get_mem_tables<'a>(&'a self, p: &'a crossbeam_epoch::Guard) -> Vec<Shared<'a, SkipList>> {
        self.mem_st_manger.lock_exclusive();
        defer! {self.mem_st_manger.unlock_exclusive()}

        let mt = self.mem_st_manger.mt_ref(p);
        let mut tables = Vec::with_capacity(self.mem_st_manger.imm().len() + 1);
        // Get mutable `mem_tables`.
        tables.push(mt);
        // TODO
        unsafe { tables[0].as_ref().unwrap().incr_ref() };
        // Get immutable `mem_tables`.
        for tb in self.mem_st_manger.imm().iter().rev() {
            let tb = tb.load(Ordering::Relaxed, &p);
            unsafe { tb.as_ref().unwrap().incr_ref() };
            tables.push(tb);
        }
        tables
    }

    fn _exists(&self, key: &[u8]) -> Result<bool> {
        return match self._get(key) {
            Err(err) if err.is_not_found() => Ok(false),
            Err(err) => Err(err),
            Ok(value) => {
                if value.value.is_empty() && value.meta == 0 {
                    return Ok(false);
                }
                Ok((value.meta & MetaBit::BIT_DELETE.bits()) == 0)
            }
        };
    }

    fn new_cas_counter(&self, how_many: u64) -> u64 {
        self.last_used_cas_counter
            .fetch_add(how_many, Ordering::Relaxed)
            + 1
    }

    fn should_write_value_to_lsm(&self, entry: &Entry) -> bool {
        entry.value.len() < self.opt.value_threshold
    }
}

impl KVCore {
    pub(crate) fn must_lc(&self) -> &LevelsController {
        let lc = self.lc.as_ref().unwrap();
        lc
    }

    pub(crate) fn must_mt(&self) -> &SkipList {
        let p = crossbeam_epoch::pin();
        let st = self.mem_st_manger.mt_ref(&p).as_raw();
        assert!(!st.is_null());
        unsafe { &*st }
    }

    fn must_vlog(&self) -> Arc<ValueLogCore> {
        let vlog = self.vlog.clone().unwrap();
        vlog
    }

    fn must_vptr(&self) -> ValuePointer {
        let p = crossbeam_epoch::pin();
        let ptr = self.vptr.load(Ordering::Acquire, &p);
        if ptr.is_null() {
            return ValuePointer::default();
        }
        unsafe { ptr.as_ref().unwrap().clone() }
    }

    pub(crate) fn get_last_used_cas_counter(&self) -> u64 {
        self.last_used_cas_counter.load(Ordering::Acquire)
    }

    pub(crate) fn update_last_used_cas_counter(&self, cas: u64) {
        self.last_used_cas_counter.store(cas, Ordering::Release);
    }

    pub(crate) fn incr_last_userd_cas_counter(&self, incr: u64) {
        self.last_used_cas_counter
            .fetch_add(incr, Ordering::Release);
    }
}

pub type WeakKV = XWeak<KVCore>;

/// DB handle
/// ```
/// use badger_rs::{Options, KV};
/// use badger_rs::IteratorOptions;
///
/// #[tokio::main]
///
/// pub async fn main() {
///      let kv = KV::open(Options::default()).await.unwrap();
///      kv.set(b"foo".to_vec(), b"bar".to_vec(), 0x0).await.unwrap();
///      let value = kv.get(b"foo").await.unwrap();
///      assert_eq!(&value, b"bar");
///      kv.delete(b"foo").await.unwrap();
///      let mut itr = kv.new_iterator(IteratorOptions::default()).await;
///      itr.rewind().await;
///      while let Some(item) = itr.peek().await {
///         let key = item.key().await;
///         let value = item.value().await.unwrap();
///         itr.next().await;
///     }
///     itr.close().await.unwrap();
///     kv.close().await.unwrap();
///}
/// ```
#[derive(Clone)]
pub struct KV {
    inner: XArc<KVCore>,
}

impl Deref for KV {
    type Target = KVCore;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl fmt::Debug for KV {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("KV").finish()
    }
}

impl KV {
    /// Async open a KV db with Options
    pub async fn open(mut opt: Options) -> Result<KV> {
        opt.max_batch_size = (15 * opt.max_table_size) / 100;
        opt.max_batch_count = 2 * opt.max_batch_size / Node::align_size() as u64;
        create_dir_all(opt.dir.as_str()).await?;
        create_dir_all(opt.value_dir.as_str()).await?;
        if !(opt.value_log_file_size <= 2 << 30 && opt.value_log_file_size >= 1 << 20) {
            return Err(Error::ValueLogSize);
        }
        let dir_lock_guard = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(Path::new(opt.dir.as_str()).join("dir_lock_guard.lock"))?;
        dir_lock_guard.try_lock_exclusive().map_err(|_| {
            crate::Error::Unexpected(
                "Another program process is using the Badger databse".to_owned(),
            )
        })?;

        let value_dir_guard = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(Path::new(opt.value_dir.as_str()).join("value_dir_guard.lock"))?;
        value_dir_guard.try_lock_exclusive().map_err(|_| {
            crate::Error::Unexpected(
                "Anthoer program process is using the Bader databse".to_owned(),
            )
        })?;
        let manifest_file = open_or_create_manifest_file(opt.dir.as_str()).await?;

        let closers = Closers {
            update_size: Closer::new("update_size".to_owned()),
            compactors: Closer::new("compactors".to_owned()),
            mem_table: Closer::new("mem_table".to_owned()),
            writes: Closer::new("writes".to_owned()),
            value_gc: Closer::new("value_gc".to_owned()),
        };

        let mut out = KVCore {
            opt: opt.clone(),
            vlog: None,
            vptr: crossbeam_epoch::Atomic::null(),
            manifest: Arc::new(RwLock::new(manifest_file)),
            lc: None,
            flush_chan: Channel::new(opt.num_mem_tables),
            notify_try_compact_chan: Channel::new(1),
            zero_level_compact_chan: Channel::new(3),
            notify_write_request_chan: Channel::new(3),
            dir_lock_guard: Arc::new(dir_lock_guard),
            value_dir_guard: Arc::new(value_dir_guard),
            closers,
            write_ch: Channel::new(KV_WRITE_CH_CAPACITY),
            last_used_cas_counter: Arc::new(AtomicU64::new(1)),
            mem_st_manger: Arc::new(SkipListManager::new(opt.arena_size() as usize)),
            share_lock: TArcRW::new(tokio::sync::RwLock::new(())),
        };

        let manifest = out.manifest.clone();

        // handle levels_controller
        let lc = LevelsController::new(
            manifest.clone(),
            out.notify_try_compact_chan.clone(),
            out.zero_level_compact_chan.clone(),
            out.notify_write_request_chan.clone(),
            out.opt.clone(),
        )
        .await?;
        lc.start_compact(out.closers.compactors.clone());
        out.lc.replace(lc);
        let mut vlog = ValueLogCore::default();
        {
            let kv = &out as *const KVCore;
            vlog.open(kv, opt.clone()).await?;
        }
        out.vlog.replace(Arc::new(vlog));

        let xout = KV::new(XArc::new(out));

        // update size
        {
            let _out = xout.clone();
            tokio::spawn(async move {
                _out.spawn_update_size().await;
            });
        }

        // mem_table closer
        {
            let _out = xout.clone();
            tokio::spawn(async move {
                if let Err(err) = _out
                    .inner
                    .flush_mem_table(_out.inner.closers.mem_table.spawn())
                    .await
                {
                    error!("abort exit flush mem table {:?}", err);
                } else {
                    info!("abort exit flush mem table");
                }
            });
        }

        // Get the lasted ValueLog Recover Pointer
        let item = match xout.inner._get(_HEAD) {
            Err(NotFound) => ValueStruct::default(), // Give it a default value
            Err(_) => return Err("Retrieving head".into()),
            Ok(item) => item,
        };
        // assert!(item.value.is_empty() , "got value {:?}", item);
        // lastUsedCasCounter will either be the value stored in !badger!head, or some subsequently
        // written value log entry that we replay.  (Subsequent value log entries might be _less_
        // than lastUsedCasCounter, if there was value log gc so we have to max() values while
        // replaying.)
        xout.get_inner_kv()
            .update_last_used_cas_counter(item.cas_counter);
        warn!("the last cas counter: {}", item.cas_counter);

        let mut vptr = ValuePointer::default();
        if !item.value.is_empty() {
            vptr.dec(&mut Cursor::new(&item.value))?;
        }
        warn!("the last vptr: {:?}", vptr);
        let replay_closer = Closer::new("tmp_writer_closer".to_owned());
        {
            let _out = xout.clone();
            let replay_closer = replay_closer.spawn();
            tokio::spawn(async move {
                _out.do_writes(replay_closer, true).await;
            });
        }

        // replay data from vlog
        let mut first = true;
        let mut count = 0;
        xout.inner
            .vlog
            .as_ref()
            .unwrap()
            .replay(&vptr, |entry, vptr| {
                let xout = xout.get_inner_kv();
                Box::pin(async move {
                    if first {
                        warn!("First key={}", string::String::from_utf8_lossy(&entry.key));
                    }
                    first = false;
                    // TODO maybe use comparse set
                    if xout.get_last_used_cas_counter() < entry.get_cas_counter() {
                        xout.update_last_used_cas_counter(entry.get_cas_counter());
                    }

                    // TODO why?
                    if entry.cas_counter_check != 0 {
                        let old_value = xout._get(&entry.key)?;
                        if old_value.cas_counter != entry.cas_counter_check {
                            return Ok(true);
                        }
                    }
                    let mut nv = vec![];
                    let mut meta = entry.meta;
                    if xout.should_write_value_to_lsm(entry) {
                        nv = entry.value.clone();
                    } else {
                        nv = Vec::with_capacity(ValuePointer::value_pointer_encoded_size());
                        vptr.enc(&mut nv).unwrap();
                        meta = meta | MetaBit::BIT_VALUE_POINTER.bits();
                    }
                    let v = ValueStruct {
                        meta,
                        user_meta: entry.user_meta,
                        cas_counter: entry.get_cas_counter(),
                        value: nv,
                    };
                    while let Err(err) = xout.ensure_room_for_write().await {
                        if count % 1000 == 0 {
                            info!("No room for write, {}", err);
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    xout.must_mt().put(&entry.key, v);
                    Ok(true)
                })
            })
            .await?;
        // Wait for replay to be applied first.
        replay_closer.signal_and_wait().await;

        // Mmap writeable log
        let max_fid = xout.get_inner_kv().must_vlog().get_max_fid();
        let lf = xout
            .get_inner_kv()
            .must_vlog()
            .pick_log_by_vlog_id(&max_fid)
            .await;
        lf.write()
            .await
            .set_write(opt.clone().value_log_file_size * 2)?;
        // TODO

        {
            let closer = xout.get_inner_kv().closers.writes.spawn();
            let _out = xout.clone();
            tokio::spawn(async move {
                _out.do_writes(closer, false).await;
            });
        }

        {
            let closer = xout.get_inner_kv().closers.value_gc.spawn();
            let _out = xout.get_inner_kv();
            tokio::spawn(async move {
                _out.must_vlog().wait_on_gc(closer).await;
            });
        }
        Ok(xout)
    }

    /// data size stats
    /// TODO
    pub async fn spawn_update_size(&self) {
        let lc = self.closers.update_size.spawn();
        defer! {lc.done()}

        let mut tk = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));

        let opt = self.opt.clone();
        let (dir, vdir) = (opt.dir, opt.value_dir);
        loop {
            let c = lc.has_been_closed();
            tokio::select! {
                _ = tk.tick() => {
                    // If value directory is different from dir, we'd have to do another walk.
                    let (lsm_sz, mut vlog_sz) = KVCore::walk_dir(dir.as_str()).await.unwrap();
                    crate::event::get_metrics().lsm_size.with_label_values(&[dir.as_ref()]).set(lsm_sz as i64);
                    if dir != vdir {
                         vlog_sz = KVCore::walk_dir(dir.as_str()).await.unwrap().1;
                    }
                    crate::event::get_metrics().vlog_size.set(vlog_sz as i64);
                    let lsm_sz = crate::event::get_metrics().lsm_size.get_metric_with_label_values(&[dir.as_ref()]).unwrap().get();
                    info!("ready to update size, lsm_sz: {}, vlog_size: {}", lsm_sz, crate::event::get_metrics().vlog_size.get());
                },
                _ = c.recv() => {return;},
            }
        }
    }

    /// Return a value that will async load value, if want not return value, should be `exists`
    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let got = self._get(key)?;
        if got.meta == MetaBit::BIT_DELETE.bits() {
            return Err(Error::NotFound);
        }
        let inner = KVItemInner::new(key.to_vec(), got, self.clone());
        inner.get_value().await
    }

    /// Set sets the provided value for a given key. If key is not present, it is created. If it is
    /// present, the existing value is overwritten with the one provided.
    /// Along with key and value, Set can also take an optional userMeta byte. This byte is stored
    /// alongside the key, and can be used as an aid to interpret the value or store other contextual
    /// bits corresponding to the key-value pair.
    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>, user_meta: u8) -> Result<()> {
        self.to_ref().set(key, value, user_meta).await
    }

    /// Sets value of key if key is not present.
    /// If it is present, it returns the key_exists error.
    /// TODO it should be atomic operate
    pub async fn set_if_ab_sent(&self, key: Vec<u8>, value: Vec<u8>, user_meta: u8) -> Result<()> {
        let exists = self.exists(&key).await?;
        // found the key, return key_exists
        if exists {
            return Err(Error::ValueKeyExists);
        }
        let entry = Entry::default()
            .key(key)
            .value(value)
            .user_meta(user_meta)
            .meta(MetaBit::BIT_SET_IF_ABSENT.bits());
        let ret = self.batch_set(vec![entry]).await;
        ret[0].to_owned()
    }

    /// Return Ok(true) if key exists, Ok(false) if key not exists, Otherwise Err(err) if happen some error.
    pub async fn exists(&self, key: &[u8]) -> Result<bool> {
        return self._exists(key);
    }

    /// Batch set entries, returns result sets
    pub async fn batch_set(&self, entries: Vec<Entry>) -> Vec<Result<()>> {
        self.inner.batch_set(entries).await
    }

    /// Asynchronous version of CompareAndSet. It accepts a callback function
    /// which is called when the CompareAndSet completes. Any error encountered during execution is
    /// passed as an argument to the callback function.
    pub async fn compare_and_set(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        cas_counter: u64,
    ) -> Result<()> {
        let entry = Entry::default()
            .key(key)
            .value(value)
            .cas_counter_check(cas_counter);
        let ret = self.batch_set(vec![entry]).await;
        ret[0].to_owned()
    }

    /// Delete deletes a key.
    /// Exposing this so that user does not have to specify the Entry directly.
    /// For example, BitDelete seems internal to badger.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let entry = Entry::default()
            .key(key.to_vec())
            .meta(MetaBit::BIT_DELETE.bits());
        let ret = self.batch_set(vec![entry]).await;
        ret[0].to_owned()
    }

    /// CompareAndDelete deletes a key ensuring that it has not been changed since last read.
    /// If existing key has different casCounter, this would not delete the key and return an error.
    pub async fn compare_and_delete(&self, key: Vec<u8>, cas_counter: u64) -> Result<()> {
        let entry = Entry::default().key(key).cas_counter_check(cas_counter);
        let ret = self.batch_set(vec![entry]).await;
        ret[0].to_owned()
    }

    /// RunValueLogGC would trigger a value log garbage collection with no guarantees that a call would
    /// result in a space reclaim. Every run would in the best case rewrite only one log file. So,
    /// repeated calls may be necessary.
    ///
    /// The way it currently works is that it would randomly pick up a value log file, and sample it. If
    /// the sample shows that we can discard at least discardRatio space of that file, it would be
    /// rewritten. Else, an ErrNoRewrite error would be returned indicating that the GC didn't result in
    /// any file rewrite.
    ///
    /// We recommend setting discardRatio to 0.5, thus indicating that a file be rewritten if half the
    /// space can be discarded.  This results in a lifetime value log write amplification of 2 (1 from
    /// original write + 0.5 rewrite + 0.25 + 0.125 + ... = 2). Setting it to higher value would result
    /// in fewer space reclaims, while setting it to a lower value would result in more space reclaims at
    /// the cost of increased activity on the LSM tree. discardRatio must be in the range (0.0, 1.0),
    /// both endpoints excluded, otherwise an ErrInvalidRequest is returned.
    ///
    /// Only one GC is allowed at a time. If another value log GC is running, or KV has been closed, this
    /// would return an ErrRejected.
    ///
    /// Note: Every time GC is run, it would produce a spike of activity on the LSM tree.
    pub async fn run_value_log_gc(&self, discard_ratio: f64) -> Result<()> {
        if discard_ratio >= 1.0 || discard_ratio <= 0.0 {
            return Err(Error::ValueInvalidRequest);
        }
        self.must_vlog().trigger_gc(discard_ratio).await
    }

    async fn do_writes(&self, lc: Closer, without_close_write_ch: bool) {
        info!("start do writes task!");
        defer! {info!("exit writes task!")}
        defer! {lc.done()}
        // TODO add metrics
        let has_been_close = lc.has_been_closed();
        let write_ch = self.to_ref().write_ch.clone();
        let reqs = ArcMx::<Vec<Request>>::new(Mutex::new(vec![]));

        let mut wait_time = tokio::time::interval(Duration::from_millis(10));
        let to_reqs = || {
            let to_reqs = reqs
                .lock()
                .clone()
                .into_iter()
                .map(|req| req.clone())
                .collect::<Vec<_>>();
            reqs.lock().clear();
            to_reqs
        };
        loop {
            tokio::select! {
                _ret = has_been_close.recv() => {
                    break;
                },
                _ = wait_time.tick() => {
                    // info!("wait time trigger!, {}", reqs.lock().len());
                },
                req = write_ch.recv() => {
                    if req.is_err() {
                        assert!(write_ch.is_close());
                        info!("receive a invalid write task, err: {:?}", req.unwrap_err());
                        break;
                    }
                    reqs.lock().push(req.unwrap());
                }
            }
            wait_time.reset();
            let to_reqs = if reqs.lock().len() >= 3 * KV_WRITE_CH_CAPACITY {
                to_reqs()
            } else {
                // Try to get next one
                if let Ok(req) = write_ch.try_recv() {
                    reqs.lock().push(req);
                    vec![]
                } else {
                    to_reqs()
                }
            };

            // TODO maybe currently
            let reqs_len = to_reqs.len();
            if !to_reqs.is_empty() {
                self.to_ref()
                    .write_requests(to_reqs)
                    .await
                    .expect("TODO: panic message");
                event::get_metrics().pending_writes.set(reqs_len as i64);
            }
        }

        // clear future requests
        if !without_close_write_ch {
            assert!(!write_ch.is_close());
            write_ch.close();
        }
        loop {
            let req = write_ch.try_recv();
            if let Err(err) = &req {
                assert!(err.is_closed() || err.is_empty(), "{:?}", err);
                break;
            }
            reqs.lock().push(req.unwrap());
            let to_reqs = to_reqs();
            self.to_ref()
                .write_requests(to_reqs.clone())
                .await
                .expect("TODO: panic message");
            event::get_metrics()
                .pending_writes
                .set(to_reqs.len() as i64);
        }
    }

    /// NewIterator returns a new iterator. Depending upon the options, either only keys, or both
    /// key-value pairs would be fetched. The keys are returned in lexicographically sorted order.
    pub async fn new_iterator(&self, opt: IteratorOptions) -> IteratorExt {
        // Notice, the iterator is global iterator, so must incr reference for memtable(SikpList), sst(file), vlog(file).
        let p = crossbeam_epoch::pin();
        let tables = self.get_mem_tables(&p);
        // TODO it should decr at IteratorExt close.
        defer! {
            tables.iter().for_each(|table| unsafe {table.as_ref().unwrap().decr_ref()});
        }
        // add vlog reference.
        self.must_vlog().incr_iterator_count();

        // Create iterators across all the tables involved first.
        let mut itrs: Vec<Box<dyn Xiterator<Output = IteratorItem>>> = vec![];
        for tb in tables.clone() {
            let st = unsafe { tb.as_ref().unwrap().clone() };
            let iter = Box::new(UniIterator::new(st, opt.reverse));
            itrs.push(iter);
        }
        // Extend sst.table
        itrs.extend(self.must_lc().as_iterator(opt.reverse));
        let mitr = MergeIterOverBuilder::default().add_batch(itrs).build();
        IteratorExt::new(self.clone(), mitr, opt)
    }
    /// Closes a KV. It's crucial to call it to ensure all the pending updates
    /// make their way to disk.
    pub async fn close(&self) -> Result<()> {
        info!("Closing database");
        // Stop value GC first;
        self.closers.value_gc.signal_and_wait().await;
        // Stop writes next.
        self.closers.writes.signal_and_wait().await;

        // Now close the value log.
        self.must_vlog().close().await?;

        // Make sure that block writer is done pushing stuff into memtable!
        // Otherwise, you will have a race condition: we are trying to flush memtables
        // and remove them completely, while the block / memtable writer is still trying
        // to push stuff into the memtable. This will also resolve the value
        // offset problem: as we push into memtable, we update value offsets there.
        if !self.must_mt().empty() {
            info!("Flushing memtable!");
            let _ = self.share_lock.write().await;
            // TODO
            let vptr = self.must_vptr();
            assert!(!self.mem_st_manger.mt_ref(&crossbeam_epoch::pin()).is_null());
            self.flush_chan
                .send(FlushTask {
                    mt: Some(self.mem_st_manger.mt_clone()),
                    vptr,
                })
                .await
                .unwrap();
            self.mem_st_manger.swap_st(self.opt.clone());
            warn!("Pushed to flush chan");
        }

        // Tell flusher to quit.
        self.flush_chan
            .send(FlushTask {
                mt: None,
                vptr: ValuePointer::default(),
            })
            .await
            .unwrap();
        self.closers.mem_table.signal_and_wait().await;
        info!("Memtable flushed!");

        self.closers.compactors.signal_and_wait().await;
        info!("Compaction finished!");

        self.must_lc().close()?;

        info!("Waiting for closer");
        self.closers.update_size.signal_and_wait().await;

        self.dir_lock_guard.unlock()?;
        self.value_dir_guard.unlock()?;

        self.manifest.write().await.close();
        // Fsync directions to ensure that lock file, and any other removed files whose directory
        // we haven't specifically fsynced, are guaranteed to have their directory entry removal
        // persisted to disk.
        async_sync_directory(self.opt.dir.clone().to_string()).await?;
        async_sync_directory(self.opt.value_dir.clone().to_string()).await?;

        warn!("metrics: \n{}", event::get_metrics());
        Ok(())
    }

    pub async fn backup<W>(&self, mut wt: W) -> Result<()>
    where
        W: Write,
    {
        let itr = self.new_iterator(IteratorOptions::default()).await;
        itr.rewind().await;
        while let Some(item) = itr.peek().await {
            let value = item.value().await?;
            let mut entry = KVPair::new();
            entry.key = item.key().await;
            entry.value = value;
            entry.userMeta.push(item.user_meta().await);
            // Write entries to disk
            crate::backup::write_to(&entry, &mut wt)?;
            itr.next().await;
        }
        Ok(())
    }
}

impl KV {
    // pub(crate) async fn new_std_iterator(
    //     &self,
    //     opt: IteratorOptions,
    // ) -> impl futures_core::Stream<Item = KVItem> {
    //     let mut itr = self.new_iterator(opt).await;
    //     itr
    // }
    // asyn yield item value from ValueLog
    pub(crate) async fn yield_item_value(
        &self,
        item: KVItemInner,
        mut consumer: impl FnMut(&[u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    ) -> Result<()> {
        //info!("ready to yield item:{} value from vlog!", item);
        // no value
        if !item.has_value() {
            info!(
                "not found the key:{}, it has not value",
                hex_str(item.key())
            );
            return consumer(&[0u8; 0]).await;
        }

        if (item.meta() & MetaBit::BIT_VALUE_POINTER.bits()) == 0 {
            info!(
                "not found the key:{}, meta: {} ",
                hex_str(item.key()),
                item.meta()
            );
            return consumer(item.vptr()).await;
        }
        let mut vptr = ValuePointer::default();
        vptr.dec(&mut Cursor::new(item.vptr()))?;
        let vlog = self.inner.must_vlog();
        vlog.async_read(&vptr, consumer).await?;
        Ok(())
    }

    pub(crate) async fn get_with_ext(&self, key: &[u8]) -> Result<KVItem> {
        let got = self._get(key)?;
        let inner = KVItemInner::new(key.to_vec(), got, self.clone());
        let item = KVItem::from(inner);
        Ok(item)
    }

    pub(crate) fn get_inner_kv(&self) -> XArc<KVCore> {
        self.inner.clone()
    }

    pub(crate) fn new(inner: XArc<KVCore>) -> KV {
        KV { inner }
    }

    pub(crate) fn to_ref(&self) -> &KVCore {
        self.inner.to_ref()
    }

    pub(crate) async fn manifest_wl(&self) -> RwLockWriteGuard<'_, ManifestFile> {
        self.manifest.write().await
    }
}

// Write level zero table
pub(crate) async fn write_level0_table(
    st: &SkipList,
    f_name: &String,
    f: &mut tokio::fs::File,
) -> Result<()> {
    defer! {info!("Finish write level zero table")}
    let st_id = st.id();
    let cur = st.new_cursor();
    let mut builder = Builder::default();
    while let Some(_) = cur.next() {
        let key = cur.key();
        let value = cur.value();
        builder.add(key, &value)?;
        #[cfg(test)]
        {
            let s = format!(
                "{}, {}, {}, {}",
                f_name,
                st_id,
                hex_str(key),
                value.pretty()
            );
            crate::test_util::push_log(s.as_bytes(), false);
        }
    }
    f.write_all(&builder.finish()).await?;
    Ok(())
}
